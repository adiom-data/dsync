package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/IBM/sarama"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
)

type MessageToNamespace func(*sarama.ConsumerMessage, map[string]struct{}) (string, error)
type MessageToUpdate func(*sarama.ConsumerMessage, map[string]struct{}) (*adiomv1.Update, string, error)

var _ adiomv1connect.ConnectorServiceHandler = &kafkaConn{}

type kafkaConn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	brokers           []string
	topicToNamespaces map[string][]string
	user, password    string
	offset            int64
	dataType          adiomv1.DataType

	messageToUpdate    MessageToUpdate
	messageToNamespace MessageToNamespace
}

// GetInfo implements [adiomv1connect.ConnectorServiceHandler].
func (k *kafkaConn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	supported := []adiomv1.DataType{k.dataType}
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     "kafka-src",
		DbType: "kafka-src",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: supported,
				LsnStream:          true,
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements [adiomv1connect.ConnectorServiceHandler].
func (k *kafkaConn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{}), nil
}

// ListData implements [adiomv1connect.ConnectorServiceHandler].
func (k *kafkaConn) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	slog.Warn("kafka connector does not support initial sync, ignoring")
	return connect.NewResponse(&adiomv1.ListDataResponse{}), nil
}

// WriteData implements [adiomv1connect.ConnectorServiceHandler].
func (k *kafkaConn) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// WriteUpdates implements [adiomv1connect.ConnectorServiceHandler].
func (k *kafkaConn) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func NewKafkaConn(brokers []string, topicToNamespaces map[string][]string, messageToUpdate MessageToUpdate, messageToNamespace MessageToNamespace, user string, password string, offset int64, dataType adiomv1.DataType) *kafkaConn {
	return &kafkaConn{
		brokers:            brokers,
		topicToNamespaces:  topicToNamespaces,
		messageToUpdate:    messageToUpdate,
		messageToNamespace: messageToNamespace,
		user:               user,
		password:           password,
		offset:             offset,
		dataType:           dataType,
	}
}

type streamCursor struct {
	Topic     string
	Partition int32
	Offset    int64
}

func (c *streamCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeStreamCursor(in []byte) (*streamCursor, error) {
	if len(in) == 0 {
		return &streamCursor{}, nil
	}
	var c streamCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (k *kafkaConn) NewConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	if k.user != "" {
		cfg.Net.TLS.Enable = true
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
		cfg.Net.SASL.User = k.user
		cfg.Net.SASL.Password = k.password
	}
	return cfg
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (k *kafkaConn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	namespaceMap := map[string]struct{}{}
	for _, n := range r.Msg.GetNamespaces() {
		namespaceMap[n] = struct{}{}
	}

	if !r.Msg.GetUpdates() {
		return connect.NewResponse(&adiomv1.GeneratePlanResponse{}), nil
	}
	cfg := k.NewConfig()
	client, err := sarama.NewClient(k.brokers, cfg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer client.Close()
	var updatePartitions []*adiomv1.UpdatesPartition

	for topic, namespaces := range k.topicToNamespaces {
		var finalNamespaces []string
		if len(namespaces) == 0 {
			finalNamespaces = r.Msg.GetNamespaces()
		} else if len(r.Msg.GetNamespaces()) == 0 {
			finalNamespaces = namespaces
		} else {
			for _, ns := range namespaces {
				if _, ok := namespaceMap[ns]; ok {
					finalNamespaces = append(finalNamespaces, ns)
				}
			}
			if len(finalNamespaces) == 0 {
				continue
			}
		}

		partitions, err := client.Partitions(topic)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		for _, partition := range partitions {
			offset, err := client.GetOffset(topic, partition, k.offset)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			c := &streamCursor{
				Topic:     topic,
				Partition: partition,
				Offset:    offset,
			}
			cursor, err := c.Encode()
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			updatePartitions = append(updatePartitions, &adiomv1.UpdatesPartition{
				Namespaces: finalNamespaces,
				Cursor:     cursor,
			})
		}
	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{UpdatesPartitions: updatePartitions}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (k *kafkaConn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	namespaceMap := map[string]struct{}{}
	for _, n := range r.Msg.GetNamespaces() {
		namespaceMap[n] = struct{}{}
	}

	cursor, err := DecodeStreamCursor(r.Msg.GetCursor())
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	cfg := k.NewConfig()
	consumer, err := sarama.NewConsumer(k.brokers, cfg)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(cursor.Topic, cursor.Partition, cursor.Offset)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer pc.Close()

	var lsn uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-pc.Messages():
			if !ok {
				return nil
			}

			nextCursor := &streamCursor{
				Topic:     cursor.Topic,
				Partition: cursor.Partition,
				Offset:    message.Offset + 1,
			}
			encodedNextCursor, err := nextCursor.Encode()
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			ns, err := k.messageToNamespace(message, namespaceMap)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			if _, ok := namespaceMap[ns]; !ok && len(namespaceMap) > 0 {
				continue
			}
			if ns == "" {
				continue
			}

			lsn++
			// TODO: maybe batch it up
			if err := s.Send(&adiomv1.StreamLSNResponse{
				NextCursor: encodedNextCursor,
				Lsn:        lsn,
			}); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return connect.NewError(connect.CodeInternal, err)
			}
		}
	}
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (k *kafkaConn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	namespaceMap := map[string]struct{}{}
	for _, n := range r.Msg.GetNamespaces() {
		namespaceMap[n] = struct{}{}
	}

	cursor, err := DecodeStreamCursor(r.Msg.GetCursor())
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	cfg := k.NewConfig()
	consumer, err := sarama.NewConsumer(k.brokers, cfg)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(cursor.Topic, cursor.Partition, cursor.Offset)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer pc.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-pc.Messages():
			if !ok {
				return nil
			}

			nextCursor := &streamCursor{
				Topic:     cursor.Topic,
				Partition: cursor.Partition,
				Offset:    message.Offset + 1,
			}
			encodedNextCursor, err := nextCursor.Encode()
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}

			update, namespace, err := k.messageToUpdate(message, namespaceMap)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			if _, ok := namespaceMap[namespace]; !ok && len(namespaceMap) > 0 {
				continue
			}
			if update != nil {
				// TODO: maybe batch it up
				if err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates:    []*adiomv1.Update{update},
					Namespace:  namespace,
					NextCursor: encodedNextCursor,
				}); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return connect.NewError(connect.CodeInternal, err)
				}
			}
		}
	}
}
