.PHONY: default test lint

test:
	go test -race -cover ./...

lint:
	golangci-lint run
