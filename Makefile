build:
	GO111MODULE=on CGO_ENABLED=0 go build -o bin/gogine ./cmd/gogine

demo:
	GO111MODULE=on CGO_ENABLED=0 go build -o bin/spreadsheetdemo ./cmd/demo/spreadsheet
	GO111MODULE=on CGO_ENABLED=0 go build -o bin/drivedemo ./cmd/demo/drive

.PHONY: build demo
