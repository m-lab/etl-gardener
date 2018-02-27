FROM golang:alpine

RUN apk update && apk add bash git
# RUN go get -u -v bytes cloud.google.com/go/datastore cloud.google.com/go/storage context encoding/json errors fmt github.com/prometheus/client_golang/prometheus/promhttp google.golang.org/api/iterator google.golang.org/api/option google.golang.org/appengine/taskqueue io io/ioutil log math/rand net/http net/http/pprof os runtime strconv strings sync time

WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

RUN go get -u -v $(go list -f '{{join .Imports "\n"}}{{"\n"}}{{join .TestImports "\n"}}' ./... | sort | uniq | grep -v etl-gardener)
RUN go install -v ./...

EXPOSE 9090 8080

CMD gardener