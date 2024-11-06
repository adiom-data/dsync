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
	"github.com/cespare/xxhash"
	"github.com/jrhy/mast"
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
			Name:  "simple-latency",
			Usage: "When using simple mode, the how stale entries are before being eligible to compare",
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
	Namespace string
	ID        []bson.RawValue
	Data      []byte
}

type NamespacedID struct {
	Namespace string
	ID        bson.RawValue
}

func connClient(impl adiomv1connect.ConnectorServiceHandler) adiomv1connect.ConnectorServiceClient {
	_, handler := adiomv1connect.NewConnectorServiceHandler(impl)
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

func (s *source) readUpdates(ctx context.Context, partitions []*adiomv1.Partition, namespaces []string, ch chan<- Update) error {
	eg, ctx := errgroup.WithContext(ctx)
	for _, partition := range partitions {
		eg.Go(func() error {
			ns := namespaces
			if len(partition.GetNamespace()) > 0 {
				ns = []string{partition.GetNamespace()}
			}
			s, err := s.c.StreamUpdates(ctx, connect.NewRequest(&adiomv1.StreamUpdatesRequest{
				Namespaces: ns,
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
						Namespace: partition.GetNamespace(),
						ID:        []bson.RawValue{id},
						Data:      d,
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
	streamUpdatesNamespacesMap := map[string]struct{}{}
	var streamUpdatesNamespaces []string
	if len(s.namespaces) > 0 {
		for _, partition := range partitions {
			streamUpdatesNamespacesMap[partition.GetNamespace()] = struct{}{}
		}
		for k := range streamUpdatesNamespacesMap {
			streamUpdatesNamespaces = append(streamUpdatesNamespaces, k)
		}
	}

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
			if err := s.readUpdates(egCtx, updatePartitions, streamUpdatesNamespaces, ch); err != nil {
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
					hasher.Write(id.Value)
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

func processMast(m *mast.Mast, namespaceMap map[string]string, projection map[string]interface{}) func(context.Context, Update) error {
	hasher := xxhash.New()
	return func(ctx context.Context, update Update) error {
		innerIDCopy := bson.RawValue{
			Type:  update.ID[0].Type,
			Value: bytes.Clone(update.ID[0].Value),
		}
		namespace := util.MapNamespace(namespaceMap, update.Namespace, ".", ":")
		id := NamespacedID{namespace, innerIDCopy}
		if update.Data == nil {
			return m.Insert(ctx, id, uint64(0))
		}
		hasher.Reset()
		if err := common.HashBson(hasher, update.Data, false, projection); err != nil {
			return err
		}
		h := hasher.Sum64()
		return m.Insert(ctx, id, h)
	}
}

type mastVerify struct {
	leftSource         source
	rightSource        source
	leftNamespacesMap  map[string]string
	rightNamespacesMap map[string]string
	limit              int
	cooldown           time.Duration
	projection         map[string]interface{}
}

func (m *mastVerify) Run(ctx context.Context) error {
	leftM := mast.NewInMemory()
	rightM := mast.NewInMemory()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return m.leftSource.ProcessSource(egCtx, processMast(&leftM, m.leftNamespacesMap, m.projection))
	})
	eg.Go(func() error {
		return m.rightSource.ProcessSource(egCtx, processMast(&rightM, m.rightNamespacesMap, m.projection))
	})
	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		m.leftSource.WaitForInitialSync()
		m.rightSource.WaitForInitialSync()
		for {
			select {
			case <-ticker.C:
				diffs := 0
				slog.Info("Verifying", "left_total", leftM.Size(), "right_total", rightM.Size())
				leftM.DiffIter(egCtx, &rightM, func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error) {
					if added && removed {
					} else if added {
						if h, ok := addedValue.(uint64); h == 0 && ok {
							return true, nil
						}
					} else if removed {
						if h, ok := removedValue.(uint64); h == 0 && ok {
							return true, nil
						}
					}
					if diffs < m.limit {
						slog.Info("diff", "id", key, "left", added, "right", removed, "left_value", addedValue, "right_value", removedValue)
					}
					diffs++
					return true, nil
				})
				slog.Info("Total diffs", "diffs", diffs)
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
		hasher.Write(update.ID[0].Value)
		idHash := hasher.Sum64()
		namespace := util.MapNamespace(namespaceMap, update.Namespace, ".", ":")
		id := NamespacedID{namespace, innerIDCopy}
		if update.Data == nil {
			std.Add(idHash, id, 0, time.Now(), left)
			return nil
		}
		hasher.Reset()
		if err := common.HashBson(hasher, update.Data, false, projection); err != nil {
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
			http.ListenAndServe(host, nil)
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
	simpleLatency := c.Duration("simple-latency")
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
	left, right, err := options.ConfigureConnectors(c.Args().Slice(), options.AdditionalSettings{})
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
			latency:            simpleLatency,
			projection:         projectionMap,
		}

		if err := tv.Run(ctx); err != nil {
			slog.Error("Failed", "err", err)
			return err
		}

		return nil
	}

	mv := mastVerify{
		leftSource:         leftSource,
		rightSource:        rightSource,
		leftNamespacesMap:  leftNamespacesMap,
		rightNamespacesMap: rightNamespacesMap,
		limit:              limit,
		cooldown:           cooldown,
		projection:         projectionMap,
	}

	if err := mv.Run(ctx); err != nil {
		slog.Error("Failed", "err", err)
		return err
	}
	return nil
}
