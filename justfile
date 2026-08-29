default: check

build:
    go build -o tend .

check:
    go test ./...
    go test -race ./...
    go vet ./...

install:
    go install .
