FROM golang:alpine

RUN apk update && apk add bash git

WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# List all of the go imports, excluding any in this repo, and run go get to import them.
RUN go get -u -v $(go list -f '{{join .Imports "\n"}}{{"\n"}}{{join .TestImports "\n"}}' ./... | sort | uniq | grep -v etl-gardener)

# Install all go executables.
RUN go install -v ./...

EXPOSE 9090 8080

CMD gardener