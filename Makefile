.PHONY: proto build run docker-up docker-down

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pb/*.proto

build:
	go build -o bin/app cmd/app/main.go

run:
	go run cmd/app/main.go

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
