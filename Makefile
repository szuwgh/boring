BINARY=boring
SRC=$(shell find . -name '*.go' -type f)

all: build

build: $(SRC)
	go build -o $(BINARY) main.go

clean:
	rm -f $(BINARY)

.PHONY: all build clean
