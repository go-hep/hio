## simple makefile to log workflow
.PHONY: all test clean build bench

#GOFLAGS := $(GOFLAGS:-race -v)

all: build test
	@# done

build: clean
	@go get $(GOFLAGS) ./...

test: build
	@go test $(GOFLAGS) -v ./...

clean:
	@go clean $(GOFLAGS) -i ./...

bench: build
	@go test $(GOFLAGS) -bench=. -benchmem

## EOF
