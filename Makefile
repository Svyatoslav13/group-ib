.PHONY: build

build:
	go build -v -o ./bin/broker main.go

.DEFAULT_GOAL:=build