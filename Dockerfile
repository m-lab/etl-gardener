FROM golang:1.20 as builder

ARG VERSION

# Copy sources into correct working directory.
WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# Build fully static binary.
ENV CGO_ENABLED 0

# Get the requirements and put the produced binaries in /go/bin
RUN go get -v ./...
WORKDIR /go/src/github.com/m-lab/etl-gardener

# NOTE: the version is either a branch, a specific tag, or a short git commit.
RUN go install -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h) \
                -X main.Version=$VERSION \
                -X main.GitCommit=$(git log -1 --format=%H)" \
      ./cmd/gardener

FROM alpine:3.14
RUN apk update && \
    apk add ca-certificates && \
    rm -rf /var/cache/apk/*

COPY --from=builder /go/bin/gardener /bin/gardener

EXPOSE 9090 8080 8081

WORKDIR /
ENTRYPOINT [ "/bin/gardener" ]
