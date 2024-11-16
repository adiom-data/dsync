package airbyte

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"golang.org/x/sync/errgroup"
)

type source struct {
	dockerImage string
	config      string

	saveCatalog         string
	syncMode            string
	destinationSyncMode string
	syncID              int
	generationID        int
	minimumGenerationID int
	cursorField         []string
	primaryKey          [][]string
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (s *source) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	executable := NewAirbyteExecutable(s.dockerImage)
	ok, err := executable.Check(ctx, s.config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("check failed"))
	}
	res, err := executable.Discover(ctx, s.config)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	catalog, err := NewAirbyteCatalog(res)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) == 0 {
		namespaces = catalog.CatalogNames()
	}
	cat, err := catalog.ConfigureCatalog(namespaces, s.syncMode, s.destinationSyncMode, s.cursorField, s.primaryKey, s.syncID, s.minimumGenerationID, s.generationID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if s.saveCatalog != "" {
		f, err := os.Create(s.saveCatalog)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if _, err := f.Write(cat); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	var cursor bytes.Buffer
	enc := gob.NewEncoder(&cursor)
	if err := enc.Encode(cat); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := enc.Encode(false); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		UpdatesPartitions: []*adiomv1.Partition{{
			Cursor: cursor.Bytes(),
		}},
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (s *source) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: fmt.Sprintf("airbyte-source-%s", s.dockerImage),
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_UNKNOWN}, // TODO
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (s *source) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (s *source) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	return nil, nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (s *source) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (s *source) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], stream *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	dec := gob.NewDecoder(bytes.NewReader(r.Msg.GetCursor()))
	var catalog []byte
	var hasState bool
	var state []byte
	var stateName string
	if err := dec.Decode(&catalog); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if err := dec.Decode(&hasState); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if hasState {
		if err := dec.Decode(&state); err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
	}
	catalogFile, err := os.CreateTemp("", "catalog.json")
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer catalogFile.Close()
	defer os.Remove(catalogFile.Name())
	if _, err := catalogFile.Write(catalog); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if err := catalogFile.Sync(); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if hasState {
		stateFile, err := os.CreateTemp("", "state.json")
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		defer stateFile.Close()
		defer os.Remove(stateFile.Name())
		if _, err := stateFile.Write(state); err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		if err := stateFile.Sync(); err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		stateName = stateFile.Name()
	}

	configuredCatalog, err := NewConfiguredAirbyteCatalog(catalog)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	executable := NewAirbyteExecutable(s.dockerImage)
	ch := make(chan []byte)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for d := range ch {
			var entry ReadStreamMessage
			if err := json.Unmarshal(d, &entry); err != nil {
				return err
			}
			switch entry.Type {
			case "RECORD":
				streamName := entry.Record.Stream
				data := entry.Record.Data
				id, err := configuredCatalog.ExtractID(streamName, data)
				if err != nil {
					return err
				}
				if err := stream.Send(&adiomv1.StreamUpdatesResponse{
					Updates: []*adiomv1.Update{{
						Id:   id,
						Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
						Data: d,
					}},
					Namespace: streamName,
				}); err != nil {
					return err
				}
			case "STATE":
				if entry.State.Type == "" {
					entry.State.Type = "LEGACY"
				}
				switch entry.State.Type {
				case "GLOBAL":
					if err := stream.Send(&adiomv1.StreamUpdatesResponse{
						NextCursor: []byte(entry.State.Global),
					}); err != nil {
						return err
					}
				case "LEGACY":
					if err := stream.Send(&adiomv1.StreamUpdatesResponse{
						NextCursor: []byte(entry.State.Data),
					}); err != nil {
						return err
					}
				case "STREAM":
					// TODO: make this work
				}
			}
		}
		return nil
	})

	eg.Go(func() error {
		return executable.Read(egCtx, s.config, catalogFile.Name(), stateName, ch)
	})

	if err := eg.Wait(); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (s *source) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	panic("unimplemented")
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (s *source) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	panic("unimplemented")
}

func NewSource(dockerImage string, config string, saveCatalog string, syncMode string, destinationSyncMode string, syncID int, generationID int, minimumGenerationID int, cursorField []string, primaryKey [][]string) adiomv1connect.ConnectorServiceHandler {
	return &source{
		dockerImage:         dockerImage,
		config:              config,
		saveCatalog:         saveCatalog,
		syncMode:            syncMode,
		destinationSyncMode: destinationSyncMode,
		syncID:              syncID,
		generationID:        generationID,
		minimumGenerationID: minimumGenerationID,
		cursorField:         cursorField,
		primaryKey:          primaryKey,
	}
}
