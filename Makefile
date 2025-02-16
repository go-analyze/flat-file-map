export GO111MODULE = on

.PHONY: default test test-cover lint

test:
	go test -race -cover ./...

test-cover:
	go test -race -coverprofile=test.out ./... && go tool cover --html=test.out

lint:
	golangci-lint run --timeout=600s --enable=asasalint,asciicheck,bidichk,containedctx,contextcheck,decorder,durationcheck,errorlint,exptostd,fatcontext,forbidigo,gocheckcompilerdirectives,gochecksumtype,goconst,gofmt,goimports,gosmopolitan,grouper,iface,importas,mirror,misspell,nilerr,nilnil,perfsprint,prealloc,reassign,recvcheck,sloglint,testifylint,unconvert,wastedassign,whitespace

