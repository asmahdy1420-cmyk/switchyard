BINARY := bin/switchyard
PKG    := ./cmd/switchyard

.PHONY: build test lint fmt run docker clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) $(PKG)

test:
	go test -race ./...

lint:
	go vet ./...
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

fmt:
	gofmt -w .

run: build
	$(BINARY) -config examples/switchyard.yaml

docker:
	docker build -t switchyard:dev .

clean:
	rm -rf bin dist
