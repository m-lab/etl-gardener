FROM golang:1.13 as builder

ENV CGO_ENABLED 0

WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# Get the requirements and put the produced binaries in /go/bin
RUN go get -v ./...
WORKDIR /go/src/github.com/m-lab/etl-gardener
RUN go install \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h) -X main.Version=$(git describe --tags) -X main.GitCommit=$(git log -1 --format=%H)" \
      ./cmd/gardener

FROM alpine:3.12
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

COPY --from=builder /go/bin/gardener /bin/gardener

EXPOSE 9090 8080 8081

WORKDIR /
ENTRYPOINT [ "/bin/gardener" ]
