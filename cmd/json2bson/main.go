package main

import (
	"flag"
	"net/http"

	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/transform"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	hostPort := flag.String("hostport", "localhost:8085", "host:port")
	flag.Parse()
	mux := http.NewServeMux()
	tpath, thandler := adiomv1connect.NewTransformServiceHandler(transform.NewJson2BsonTransform())
	mux.Handle(tpath, thandler)
	http.ListenAndServe(
		*hostPort,
		h2c.NewHandler(mux, &http2.Server{}),
	)
}
