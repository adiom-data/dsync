FROM golang:1.24.6 AS build

WORKDIR /go/src/dsync
COPY . .

RUN go mod download

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /go/bin/dsync .

FROM gcr.io/distroless/static-debian12

COPY --from=build /go/bin/dsync /

ENTRYPOINT [ "/dsync" ]
