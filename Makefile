GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)
NAME = $(shell basename $(CURDIR))

default: build

build:
	@echo build binary for $(GOOS)/$(GOARCH)
	@go build -o $(NAME)-$(GOOS)-$(GOARCH)
