help:
	@echo "make build: build exporter"

build:
	GOOS="linux" GOARCH="amd64" go build -ldflags "-X main.Version=$$(git describe --tags)" -o bin/singlestore_exporter main.go
