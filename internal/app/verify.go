package dsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/internal/util"
	"github.com/adiom-data/dsync/logger"
	"github.com/adiom-data/dsync/pkg/verify/ds"
	"github.com/benbjohnson/clock"
	"github.com/cespare/xxhash"
	"github.com/urfave/cli/v2"
	"go.akshayshah.org/memhttp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"golang.org/x/sync/errgroup"
)

var verifyCommand *cli.Command = &cli.Command{
	Name:   "verify",
	Action: runVerify,
	Flags: []cli.Flag{
		&cli.DurationFlag{
			Name:  "cooldown",
			Usage: "time between comparison attempts",
			Value: time.Minute,
		},
		&cli.StringSliceFlag{
			Name: "namespaces",
		},
		&cli.IntFlag{
			Name:  "parallelism",
			Usage: "for initial sync reads",
			Value: 4,
		},
		&cli.IntFlag{
			Name:  "total-partitions",
			Usage: "total number of outstanding partitions",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "partition",
			Usage: "partition number (0 indexed)",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "limit",
			Usage: "max diffs to print",
			Value: 10,
		},
		&cli.BoolFlag{
			Name:  "simple",
			Usage: "Use a simple verifier that tails both sources",
		},
		&cli.DurationFlag{
			Name:  "latency",
			Usage: "How stale entries are before being eligible to compare",
			Value: time.Second * 5,
		},
		&cli.StringFlag{
			Name:  "projection",
			Usage: "JSON describing which fields to include in comparisons: {\"field\": {\"inner_field\": true}}",
		},
		&cli.StringFlag{
			Name:  "verbosity",
			Usage: "DEBUG|INFO|WARN|ERROR",
			Value: "INFO",
		},
	},
}

type Update struct {
	Namespace   string
	ID          []bson.RawValue
	Data        []byte
	InitialSync bool
}

type NamespacedID struct {
	Namespace string
	ID        bson.RawValue
}

func connClient(impl adiomv1connect.ConnectorServiceHandler) adiomv1connect.ConnectorServiceClient {
	_, handler := adiomv1connect.NewConnectorServiceHandler(impl, connect.WithCompression("gzip", nil, nil))
	srv, err := memhttp.New(handler)
	if err != nil {
		panic(err)
	}
	return adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
}

type source struct {
	c               adiomv1connect.ConnectorServiceClient
	namespaces      []string
	parallelism     int
	initialSyncDone chan struct{}
	partition       int
	totalPartitions int
	skipInitialSync bool
	skipStream      bool
}

func (s *source) readUpdates(ctx context.Context, partitions []*adiomv1.UpdatesPartition, ch chan<- Update) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, partition := range partitions {
		eg.Go(func() error {
			s, err := s.c.StreamUpdates(ctx, connect.NewRequest(&adiomv1.StreamUpdatesRequest{
				Namespaces: partition.GetNamespaces(),
				Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				Cursor:     partition.GetCursor(),
			}))
			if err != nil {
				return err
			}
			for s.Receive() {
				msg := s.Msg()
				for _, update := range msg.GetUpdates() {
					var idBson []bson.RawValue
					for _, id := range update.GetId() {
						idBson = append(idBson, bson.RawValue{Type: bsontype.Type(id.GetType()), Value: id.GetData()})
					}
					if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_DELETE {
						ch <- Update{
							Namespace: msg.GetNamespace(),
							ID:        idBson,
						}
					} else {
						ch <- Update{
							Namespace: msg.GetNamespace(),
							ID:        idBson,
							Data:      update.GetData(),
						}
					}
				}
			}
			if s.Err() != nil {
				return s.Err()
			}
			return nil
		})
	}
	return eg.Wait()
}

func (s *source) readPartitions(ctx context.Context, partitions []*adiomv1.Partition, ch chan<- Update) error {
	defer slog.Debug("Done all partitions")
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(s.parallelism)
	for _, partition := range partitions {
		eg.Go(func() error {
			slog.Debug("Processing partition", "partition", partition)
			defer slog.Debug("Done reading initial sync partition", "partition", partition)
			var cursor []byte
			for {
				res, err := s.c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
					Partition: partition,
					Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON,
					Cursor:    cursor,
				}))
				if err != nil {
					return err
				}
				for _, d := range res.Msg.GetData() {
					id := bson.Raw(d).Lookup("_id")
					ch <- Update{
						Namespace:   partition.GetNamespace(),
						ID:          []bson.RawValue{id},
						Data:        d,
						InitialSync: true,
					}
				}
				cursor = res.Msg.GetNextCursor()
				if len(cursor) == 0 {
					return nil
				}
			}
		})
	}
	return eg.Wait()
}

func (s *source) WaitForInitialSync() {
	<-s.initialSyncDone
}

func (s *source) ProcessSource(ctx context.Context, process func(context.Context, Update) error) error {
	// Check capabilities
	info, err := s.c.GetInfo(ctx, connect.NewRequest(&adiomv1.GetInfoRequest{}))
	if err != nil {
		return err
	}
	srcCapabilities := info.Msg.GetCapabilities().GetSource()
	if srcCapabilities == nil {
		return fmt.Errorf("not a valid source %v", info.Msg.GetDbType())
	}
	var supportedType bool
	for _, t := range srcCapabilities.GetSupportedDataTypes() {
		if t == adiomv1.DataType_DATA_TYPE_MONGO_BSON {
			supportedType = true
		}
	}
	if !supportedType {
		return fmt.Errorf("not a valid source")
	}
	if err := util.ValidateNamespaces(s.namespaces, info.Msg.GetCapabilities()); err != nil {
		return err
	}

	// Read Plan
	plan, err := s.c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces:  s.namespaces,
		InitialSync: true,
		Updates:     true,
	}))
	if err != nil {
		return err
	}

	partitions := plan.Msg.GetPartitions()
	updatePartitions := plan.Msg.GetUpdatesPartitions()

	// Read
	eg, egCtx := errgroup.WithContext(ctx)
	ch := make(chan Update)
	eg.Go(func() error {
		defer close(ch)
		if !s.skipInitialSync {
			if err := s.readPartitions(egCtx, partitions, ch); err != nil {
				close(s.initialSyncDone)
				return err
			}
		}
		close(s.initialSyncDone)
		if !s.skipStream {
			if err := s.readUpdates(egCtx, updatePartitions, ch); err != nil {
				return err
			}
		}
		return nil
	})

	// Consume
	eg.Go(func() error {
		hasher := xxhash.New()
		for update := range ch {
			if s.totalPartitions > 0 {
				hasher.Reset()
				for _, id := range update.ID {
					if _, err := hasher.Write(id.Value); err != nil {
						return err
					}
				}
				if hasher.Sum64()%uint64(s.totalPartitions) != uint64(s.partition) {
					continue
				}
			}
			if err := process(egCtx, update); err != nil {
				return err
			}
		}
		return nil
	})

	return eg.Wait()
}

func processMemVerify(verifier func(string) ds.Verifier, left bool, namespaceMap map[string]string, projection map[string]interface{}) func(context.Context, Update) error {
	hasher := xxhash.New()
	return func(ctx context.Context, update Update) error {
		id, err := common.IDToString(update.ID)
		if err != nil {
			return fmt.Errorf("err converting id to str: %w", err)
		}
		ns := util.MapNamespace(namespaceMap, update.Namespace, ".", ":")

		if update.Data == nil {
			verifier(ns).Put(id, left, 0, update.InitialSync)
			return nil
		}
		hasher.Reset()
		if err := common.HashBson(hasher, bson.Raw(update.Data), false, projection); err != nil {
			return err
		}
		h := hasher.Sum64()
		verifier(ns).Put(id, left, h, update.InitialSync)
		return nil
	}
}

type memVerify struct {
	leftSource         source
	rightSource        source
	leftNamespacesMap  map[string]string
	rightNamespacesMap map[string]string
	limit              int
	cooldown           time.Duration
	latency            time.Duration
	projection         map[string]interface{}
}

func (m *memVerify) Run(ctx context.Context) error {
	verifiers := map[string]ds.Verifier{}
	var mut sync.RWMutex
	getVerifier := func(ns string) ds.Verifier {
		mut.RLock()
		v, ok := verifiers[ns]
		if !ok {
			mut.RUnlock()
			mut.Lock()
			v, ok := verifiers[ns]
			if !ok {
				v = ds.NewVerifier(ctx, clock.New(), m.latency, time.Second, 0)
				verifiers[ns] = v
			}
			mut.Unlock()
			return v
		}
		mut.RUnlock()
		return v
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return m.leftSource.ProcessSource(egCtx, processMemVerify(getVerifier, true, m.leftNamespacesMap, m.projection))
	})
	eg.Go(func() error {
		return m.rightSource.ProcessSource(egCtx, processMemVerify(getVerifier, false, m.rightNamespacesMap, m.projection))
	})
	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		m.leftSource.WaitForInitialSync()
		m.rightSource.WaitForInitialSync()
		for {
			select {
			case <-ticker.C:
				found := false
				var totalMismatches, totalCount int64
				mut.RLock()
				for ns, verifier := range verifiers {
					mismatches, total := verifier.MismatchCountAndTotal()
					if mismatches > 0 {
						found = true
						slog.Info("Verifying", "namespace", ns, "mismatches", mismatches, "count", total, "ids", verifier.Find(m.limit))
					}
					totalMismatches += mismatches
					totalCount += total
				}
				mut.RUnlock()
				slog.Info("Verifying", "ok", !found, "mismatches", totalMismatches, "count", totalCount)
				ticker.Reset(m.cooldown)
			case <-egCtx.Done():
				return nil
			}
		}
	})

	return eg.Wait()
}

type tailVerify struct {
	leftSource         source
	rightSource        source
	leftNamespacesMap  map[string]string
	rightNamespacesMap map[string]string
	limit              int
	cooldown           time.Duration
	latency            time.Duration
	projection         map[string]interface{}
}

type SimpleTailDiffer struct {
	lock sync.Mutex
	m    map[uint64]tailDiff
}

func (s *SimpleTailDiffer) Add(idHash uint64, id NamespacedID, h uint64, t time.Time, left bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	diff, ok := s.m[idHash]
	if !ok {
		if left {
			s.m[idHash] = tailDiff{
				id:   id,
				t:    t,
				left: h,
			}
		} else {
			s.m[idHash] = tailDiff{
				id:    id,
				t:     t,
				right: h,
			}
		}
		return
	} else if id.Namespace != diff.id.Namespace || id.ID.String() != diff.id.ID.String() {
		if !t.After(diff.t) {
			return
		}
		if left {
			s.m[idHash] = tailDiff{
				id:   id,
				t:    t,
				left: h,
			}
		} else {
			s.m[idHash] = tailDiff{
				id:    id,
				t:     t,
				right: h,
			}
		}
		return
	}
	if t.After(diff.t) {
		diff.t = t
	}
	if left {
		diff.left = h
	} else {
		diff.right = h
	}
	s.m[idHash] = diff
}

// GetAndClean will return all items older than `olderThan` and remove them
// and for now it will lock everything
func (s *SimpleTailDiffer) GetAndClean(olderThan time.Time) []tailDiff {
	s.lock.Lock()
	defer s.lock.Unlock()
	var res []tailDiff
	for k, v := range s.m {
		if v.t.Before(olderThan) {
			res = append(res, v)
			delete(s.m, k)
		}
	}
	return res
}

func processTail(std *SimpleTailDiffer, namespaceMap map[string]string, projection map[string]interface{}, left bool) func(context.Context, Update) error {
	hasher := xxhash.New()
	return func(ctx context.Context, update Update) error {
		innerIDCopy := bson.RawValue{
			Type:  update.ID[0].Type,
			Value: bytes.Clone(update.ID[0].Value),
		}
		hasher.Reset()
		if _, err := hasher.Write(update.ID[0].Value); err != nil {
			return err
		}
		idHash := hasher.Sum64()
		namespace := util.MapNamespace(namespaceMap, update.Namespace, ".", ":")
		id := NamespacedID{namespace, innerIDCopy}
		if update.Data == nil {
			std.Add(idHash, id, 0, time.Now(), left)
			return nil
		}
		hasher.Reset()
		if err := common.HashBson(hasher, bson.Raw(update.Data), false, projection); err != nil {
			return err
		}
		h := hasher.Sum64()
		std.Add(idHash, id, h, time.Now(), left)
		return nil
	}
}

type tailDiff struct {
	id    NamespacedID
	t     time.Time
	left  uint64
	right uint64
}

func (v *tailVerify) Run(ctx context.Context) error {
	std := &SimpleTailDiffer{m: map[uint64]tailDiff{}}
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		v.leftSource.skipInitialSync = true
		return v.leftSource.ProcessSource(egCtx, processTail(std, v.leftNamespacesMap, v.projection, true))
	})
	eg.Go(func() error {
		v.rightSource.skipInitialSync = true
		return v.rightSource.ProcessSource(egCtx, processTail(std, v.rightNamespacesMap, v.projection, false))
	})
	eg.Go(func() error {
		cumulativeTotal := 0
		cumulativeDiff := 0
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				diffs := 0
				items := std.GetAndClean(time.Now().Add(-v.latency))
				for _, item := range items {
					if item.left != item.right {
						if diffs < v.limit {
							slog.Info("diff", "id", item.id, "left", item.left, "right", item.right)
						}
						diffs++
						cumulativeDiff++
					}
				}
				cumulativeTotal += len(items)
				slog.Info("Total diffs", "diffs", diffs, "total_checked", len(items), "cumulative_diffs", cumulativeDiff, "cumulative_total", cumulativeTotal)
				ticker.Reset(v.cooldown)
			case <-egCtx.Done():
				return nil
			}
		}
	})

	return eg.Wait()
}

func runVerify(c *cli.Context) error {
	if c.Bool("pprof") {
		go func() {
			host := fmt.Sprintf("localhost:%d", c.Int("pprof-port"))
			slog.Info("Starting pprof server on " + host)
			_ = http.ListenAndServe(host, nil) // #nosec G114
		}()
	}
	slog.Info("Starting Verifier", "namespaces", c.StringSlice("namespaces"))
	totalPartitions := c.Int("total-partitions")
	partition := c.Int("partition")
	if totalPartitions < 1 {
		return fmt.Errorf("invalid total-partitions")
	}
	if partition >= totalPartitions || partition < 0 {
		return fmt.Errorf("invalid partition")
	}
	logger.Setup(logger.Options{Verbosity: c.String("verbosity")})
	namespaces := c.StringSlice("namespaces")
	cooldown := c.Duration("cooldown")
	parallelism := c.Int("parallelism")
	limit := c.Int("limit")
	simple := c.Bool("simple")
	latency := c.Duration("latency")
	projection := c.String("projection")
	var projectionMap map[string]interface{}
	if projection != "" {
		if err := json.Unmarshal([]byte(projection), &projectionMap); err != nil {
			slog.Error("Specified projection should be valid json.")
			return err
		}
	}
	leftNamespaces, rightNamespaces := util.NamespaceSplit(namespaces, ":")
	leftNamespacesMap := util.Mapify(leftNamespaces, namespaces)
	rightNamespacesMap := util.Mapify(rightNamespaces, namespaces)

	ctx := c.Context
	left, right, _, err := options.ConfigureConnectors(c.Args().Slice(), options.AdditionalSettings{})
	if err != nil {
		return err
	}
	leftC := left.Remote
	rightC := right.Remote
	if left.Local != nil {
		leftC = connClient(left.Local)
	}
	if right.Local != nil {
		rightC = connClient(right.Local)
	}

	leftSource := source{
		c:               leftC,
		namespaces:      leftNamespaces,
		parallelism:     parallelism,
		initialSyncDone: make(chan struct{}),
		partition:       partition,
		totalPartitions: totalPartitions,
	}
	rightSource := source{
		c:               rightC,
		namespaces:      rightNamespaces,
		parallelism:     parallelism,
		initialSyncDone: make(chan struct{}),
		partition:       partition,
		totalPartitions: totalPartitions,
	}

	if simple {
		tv := tailVerify{
			leftSource:         leftSource,
			rightSource:        rightSource,
			leftNamespacesMap:  leftNamespacesMap,
			rightNamespacesMap: rightNamespacesMap,
			limit:              limit,
			cooldown:           cooldown,
			latency:            latency,
			projection:         projectionMap,
		}

		if err := tv.Run(ctx); err != nil {
			slog.Error("Failed", "err", err)
			return err
		}

		return nil
	}

	mv := memVerify{
		leftSource:         leftSource,
		rightSource:        rightSource,
		leftNamespacesMap:  leftNamespacesMap,
		rightNamespacesMap: rightNamespacesMap,
		limit:              limit,
		cooldown:           cooldown,
		latency:            latency,
		projection:         projectionMap,
	}

	if err := mv.Run(ctx); err != nil {
		slog.Error("Failed", "err", err)
		return err
	}
	return nil
}
