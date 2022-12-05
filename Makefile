test:
	go test -v ./...

fmt:
	go run golang.org/x/tools/cmd/goimports@v0.3.0 -w .

update-licenses:
	go run github.com/elastic/go-licenser@v0.4.1 .

MODULE_DEPS:=$(sort $(shell go list -deps -f "{{with .Module}}{{if not .Main}}{{.Path}}{{end}}{{end}}"))
NOTICE.txt: go.mod
	go list -m -json $(MODULE_DEPS) | \
	  go run go.elastic.co/go-licence-detector@v0.5.0 -includeIndirect -noticeTemplate docs/NOTICE.txt.tmpl -noticeOut $@
