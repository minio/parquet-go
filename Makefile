GOPATH := $(shell go env GOPATH)

all: check

getdeps:
	@if [ ! -f ${GOPATH}/bin/golint ]; then echo "Installing golint" && go get -u golang.org/x/lint/golint; fi
	@if [ ! -f ${GOPATH}/bin/staticcheck ]; then echo "Installing staticcheck" && go get -u honnef.co/go/tools/...; fi
	@if [ ! -f ${GOPATH}/bin/misspell ]; then echo "Installing misspell" && go get -u github.com/client9/misspell/cmd/misspell; fi

vet:
	@echo "Running $@"
	@go vet ./...

fmt:
	@echo "Running $@"
	@gofmt -d *.go

lint:
	@echo "Running $@"
	@${GOPATH}/bin/golint -set_exit_status

staticcheck:
	@echo "Running $@"
	@${GOPATH}/bin/staticcheck *.go

spelling:
	@${GOPATH}/bin/misspell -locale US -error *.go README.md


check: getdeps vet fmt lint staticcheck spelling
	@echo "Running unit tests"
	@go test -tags kqueue .
