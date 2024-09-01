DB_URL = "postgresql://root:1qaz@localhost:5432/b1?sslmode=disable"


postgres:
	docker run --name postgres14 -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=1qaz -d postgres:latest

createdb:
	docker exec -it postgres14 createdb --username=root --owner=root broker

dropdb:
	docker exec -it postgres14 dropdb messenger

migrateup:
	migrate -path ./db/postgres/migration -database $(DB_URL) -verbose up

migrateup1:
	migrate -path db/migration -database $(DB_URL) -verbose up 1

migratedown:
	migrate -path db/migration -database $(DB_URL) -verbose down

migratedown1:
	migrate -path db/migration -database $(DB_URL) -verbose down 1

sqlc:
	sqlc generate

test:
	go test -count=1 -v -cover ./...

server:
	go run main.go

mock:
	mockgen -destination db/mock/store.go -package mockdb github.com/techschool/simplebank/db/sqlc Store

proto:
	 protoc --proto_path=proto --go_out=pb --go_opt=paths=source_relative --go-grpc_out=pb --go-grpc_opt=paths=source_relative proto/*.proto

.PHONY: postgres createdb dropdb migrateup migratedown sqlc test server mock migrateup1 migratedown1 proto