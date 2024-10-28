package main

import (
	"context"
	"net"
	"net/http"

	"github.com/adiom-data/dsync/connectors/null"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

func main() {
	// Runs 2 servers - one using standard go grpc, and the other using connect
	// This is for testing purposes only.
	go func() {
		l, err := net.Listen("tcp", "localhost:8086")
		if err != nil {
			panic(err)
		}
		s := grpc.NewServer()
		adiomv1.RegisterConnectorServiceServer(s, newConnector())
		s.Serve(l)
	}()

	nullConn := null.NewConn()
	mux := http.NewServeMux()
	path, handler := adiomv1connect.NewConnectorServiceHandler(nullConn)
	mux.Handle(path, handler)
	http.ListenAndServe(
		"localhost:8085",
		h2c.NewHandler(mux, &http2.Server{}),
	)
}

func (*nullConnector) GetInfo(context.Context, *adiomv1.GetInfoRequest) (*adiomv1.GetInfoResponse, error) {
	return &adiomv1.GetInfoResponse{
		DbType:       "/dev/null",
		Capabilities: &adiomv1.Capabilities{Sink: &adiomv1.Capabilities_Sink{}},
	}, nil
}

func (*nullConnector) WriteData(context.Context, *adiomv1.WriteDataRequest) (*adiomv1.WriteDataResponse, error) {
	return &adiomv1.WriteDataResponse{}, nil
}

func (*nullConnector) WriteUpdates(context.Context, *adiomv1.WriteUpdatesRequest) (*adiomv1.WriteUpdatesResponse, error) {
	return &adiomv1.WriteUpdatesResponse{}, nil
}

type nullConnector struct {
	adiomv1.UnimplementedConnectorServiceServer
}

func newConnector() adiomv1.ConnectorServiceServer {
	return &nullConnector{}
}
