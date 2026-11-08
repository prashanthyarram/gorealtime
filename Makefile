.PHONY: fmt test race vet

fmt:
	gofmt -w .

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

bench:
	go test -bench=. -benchmem -count=3 -run='^$$' .
