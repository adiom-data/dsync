package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/IBM/sarama"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"google.golang.org/protobuf/proto"
)

func DsyncMessageToNamespace(m *sarama.ConsumerMessage, _ map[string]struct{}) (string, error) {
	for _, h := range m.Headers {
		if string(h.Key) == "ns" {
			return string(h.Value), nil
		}
	}
	return "", fmt.Errorf("missing namespace header")
}

func DsyncMessageToUpdate(m *sarama.ConsumerMessage, _ map[string]struct{}) (*adiomv1.Update, string, error) {
	update := &adiomv1.Update{}
	if err := proto.Unmarshal(m.Value, update); err != nil {
		return nil, "", fmt.Errorf("err unmarshalling proto: %w", err)
	}
	for _, h := range m.Headers {
		if string(h.Key) == "ns" {
			return update, string(h.Value), nil
		}
	}

	return nil, "", fmt.Errorf("missing namespace header")
}

type destConn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	producer         sarama.SyncProducer
	namespaceToTopic map[string]string
	defaultTopic     string
	dataType         adiomv1.DataType
}

func NewDestKafka(brokers []string, defaultTopic string, namespacesToTopic map[string]string, user string, password string, dataType adiomv1.DataType) (*destConn, error) {
	cfg := sarama.NewConfig()
	if user != "" {
		cfg.Net.TLS.Enable = true
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
		cfg.Net.SASL.User = user
		cfg.Net.SASL.Password = password
	}
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("err creating kafka producer: %w", err)
	}
	return &destConn{
		producer:         producer,
		namespaceToTopic: namespacesToTopic,
		defaultTopic:     defaultTopic,
		dataType:         dataType,
	}, nil
}

func (d *destConn) Teardown() {
	_ = d.producer.Close()
}

// GeneratePlan implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// GetInfo implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	supported := []adiomv1.DataType{d.dataType}
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     "kafka-dest",
		DbType: "kafka-dest",
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: supported,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// ListData implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// StreamLSN implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// StreamUpdates implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// WriteData implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	slog.Warn("kafka connector does not support initial sync, ignoring")
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func key(id []*adiomv1.BsonValue) []byte {
	var res []byte
	for _, part := range id {
		res = append(res, part.GetData()...)
	}
	return res
}

// WriteUpdates implements [adiomv1connect.ConnectorServiceHandler].
func (d *destConn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	topic := d.defaultTopic
	ns := r.Msg.GetNamespace()
	header := []sarama.RecordHeader{{Key: []byte("ns"), Value: []byte(ns)}}
	if namespaceTopic, ok := d.namespaceToTopic[ns]; ok {
		topic = namespaceTopic
	}
	var messages []*sarama.ProducerMessage
	for _, update := range r.Msg.GetUpdates() {
		b, err := proto.Marshal(update)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err marshalling proto: %w", err))
		}
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(key(update.GetId())),
			Value:     sarama.ByteEncoder(b),
			Headers:   header,
			Partition: -1,
		}
		messages = append(messages, msg)
	}
	if err := d.producer.SendMessages(messages); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err sending message: %w", err))
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

var _ adiomv1connect.ConnectorServiceHandler = &destConn{}
