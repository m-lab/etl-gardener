FROM golang:1.12 as builder

ENV CGO_ENABLED 0

WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# Get the requirements and put the produced binaries in /go/bin
RUN go get -v -t ./...
RUN go install \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)" \
      ./...

FROM alpine
COPY --from=builder /go/bin/gardener /bin/gardener

EXPOSE 9090 8080

WORKDIR /
ENTRYPOINT [ "/bin/gardener" ]
