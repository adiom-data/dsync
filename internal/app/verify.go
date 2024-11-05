package dsync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
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

func (s *source) processSource(ctx context.Context, process func(context.Context, Update) error) error {
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
		if err := s.readPartitions(egCtx, partitions, ch); err != nil {
			close(s.initialSyncDone)
			return err
		}
		close(s.initialSyncDone)
		if err := s.readUpdates(egCtx, updatePartitions, streamUpdatesNamespaces, ch); err != nil {
			return err
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

func processMast(m *mast.Mast, namespaceMap map[string]string) func(context.Context, Update) error {
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
		if err := common.HashBson(hasher, update.Data, false); err != nil {
			return err
		}
		h := hasher.Sum64()
		return m.Insert(ctx, id, h)
	}
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

	leftM := mast.NewInMemory()
	rightM := mast.NewInMemory()

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

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return leftSource.processSource(egCtx, processMast(&leftM, leftNamespacesMap))
	})
	eg.Go(func() error {
		return rightSource.processSource(egCtx, processMast(&rightM, rightNamespacesMap))
	})
	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		<-leftSource.initialSyncDone
		<-rightSource.initialSyncDone
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
					if diffs < limit {
						slog.Info("diff", "id", key, "left", added, "right", removed, "left_value", addedValue, "right_value", removedValue)
					}
					diffs++
					return true, nil
				})
				slog.Info("Total diffs", "diffs", diffs)
				ticker.Reset(cooldown)
			case <-egCtx.Done():
				return nil
			}
		}
	})

	if err := eg.Wait(); err != nil {
		slog.Error("Failed", "err", err)
		return err
	}
	return nil
}
