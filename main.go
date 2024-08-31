package main

import (
	"BaleBroker/broker"
	db "BaleBroker/db/postgres"
	"BaleBroker/gapi/pb"
	"BaleBroker/gapi/server"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"net/http"
	"time"
)

func pool(dbUrl string) *pgxpool.Config {
	dbConfig, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		log.Fatal("Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = int32(30)
	dbConfig.MinConns = int32(0)
	dbConfig.MaxConnLifetime = time.Hour
	dbConfig.MaxConnIdleTime = time.Minute * 30
	dbConfig.HealthCheckPeriod = time.Minute
	dbConfig.ConnConfig.ConnectTimeout = time.Second * 5

	return dbConfig
}

func main() {
	config, err := utils.LoadConfig(".")
	if err != nil {
		log.Fatalf("can not load the config: %v", err)
	}
	conn, err := pgx.Connect(context.Background(), config.DBSource)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(context.Background())
	idGen := pkg.NewSequentialIdentifier()
	postgresDb := db.NewPostgresDb(conn)
	baleBroker := broker.NewBaleBroker(postgresDb, idGen)
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
