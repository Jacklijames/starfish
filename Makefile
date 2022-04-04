# Go parameters
GO           = go
TIMEOUT_UNIT = 5m

.PHONY: all
all: build-mac

.PHONY: build
build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -o starfish -ldflags "-X github.com/transaction-mesh/starfish/common/version.Version=1.0.0" -v ./cmd/...
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o starfish -ldflags "-X github.com/transaction-mesh/starfish/common/version.Version=1.0.0" -v ./cmd/...
build-mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -o starfish -ldflags "-X github.com/transaction-mesh/starfish/common/version.Version=1.0.0" -v ./cmd/...

.PHONY: test
test:
	$(GO) test -timeout $(TIMEOUT_UNIT) -v ./test/...

.PHONY: clean
clean:
	$(GO) clean
	@rm -rf test/tests.* test/coverage.*
	@rm -rf starfish