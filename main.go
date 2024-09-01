package main

import (
	"BaleBroker/broker"
	db "BaleBroker/db"
	memory "BaleBroker/db/memory"
	postgres "BaleBroker/db/postgres"
	"BaleBroker/gapi/pb"
	"BaleBroker/gapi/server"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"net/http"
	"time"
)

func main() {
	config, err := utils.LoadConfig(".")
	if err != nil {
		log.Fatalf("can not load the config: %v", err)
	}
	storage, err := getStorage(&config)
	if err != nil {
		log.Fatal(err)
	}
	idGen := pkg.NewSequentialIdentifier()
	baleBroker := broker.NewBaleBroker(storage, idGen)
	rpcServer := server.NewServer(baleBroker)
	prometheus := server.NewPrometheusMetrics()
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(prometheus.PrometheusUnaryCollector, server.UnaryLoggerInterceptor),
	)

	pb.RegisterBrokerServer(grpcServer, rpcServer)
	reflection.Register(grpcServer)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(config.PrometheusMetricAddress, nil))
	}()

	listner, err := net.Listen("tcp", config.GRPCServerAddress)
	if err != nil {
		log.Fatalf("can not create listner: %v", err)
	}

	log.Printf("grpc server started on %v", config.GRPCServerAddress)
	err = grpcServer.Serve(listner)
	if err != nil {
		log.Fatalf("can not start grpc server: %v", err)
	}
}
func createPool(dbUrl string) *pgxpool.Config {
	dbConfig, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		log.Fatal("Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = int32(2)
	dbConfig.MinConns = int32(0)
	dbConfig.MaxConnLifetime = time.Hour
	dbConfig.MaxConnIdleTime = time.Minute * 30
	dbConfig.HealthCheckPeriod = time.Minute
	dbConfig.ConnConfig.ConnectTimeout = time.Second * 5

	return dbConfig
}
func getStorage(config *utils.Config) (db.DB, error) {
	log.Printf("set %v as storage service \n", config.Storage)
	switch config.Storage {
	case "POSTGRES":
		connPool, err := pgxpool.NewWithConfig(context.Background(), createPool(config.DBSource))
		if err != nil {
			return nil, errors.New(fmt.Sprintf("can not connect to : %v", config.DBSource))
		}
		if config.WriteOrder == "BATCH" {
			return postgres.NewBatchPostgresDb(connPool), nil
		} else {
			return postgres.NewParallelPostgresDb(connPool), nil
		}
	case "MEMORY":
		return memory.NewMemoryDB(), nil
	default:
		return nil, errors.New(fmt.Sprintf("invalid storage: %v", config.Storage))
	}
}
