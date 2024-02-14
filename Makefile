.PHONY: default test ling

test:
	go test -race -cover ./...

lint:
	golangci-lint run
