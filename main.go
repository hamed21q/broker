package main

import (
	"BaleBroker/broker"
	db "BaleBroker/db"
	memory "BaleBroker/db/memory"
	postgres "BaleBroker/db/postgres"
	crud "BaleBroker/db/postgres/crud"
	"BaleBroker/gapi/pb"
	"BaleBroker/gapi/server"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var StorageFactoryMapper = map[string]func() db.DB{
	"BATCH_PG": func() db.DB {
		return postgres.NewBatchPostgresDb(pool, crud.New())
	},
	"PARALLEL_PG": func() db.DB {
		return postgres.NewParallelPostgresDb(pool, crud.New())
	},
	"MEMORY": func() db.DB {
		return memory.NewMemoryDB()
	},
}

var (
	pool  *pgxpool.Pool
	idGen *pkg.SequentialIdentifier
)

func main() {
	config, err := utils.LoadConfig(".")
	if err != nil {
		log.Fatalf("can not load the config: %v", err)
	}

	_, err = startTracing(config.TracerSource)
	if err != nil {
		log.Fatalf("can not start tracing: %v", err)
	}

	pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig(config.DBSource))
	if err != nil {
		log.Fatalf("can not connect to : %v", config.DBSource)
	}

	idGen = pkg.NewSequentialIdentifier()
	queries := crud.New()
	idSync := pkg.NewPGSync(queries, pool, idGen)
	err = idSync.Sync(context.Background())
	if err != nil {
		log.Printf("can not sync with db : %v", err.Error())
	}

	storageFactory, ok := StorageFactoryMapper[config.Storage]
	if !ok {
		log.Fatalf("can not find storage driver")
	}
	var storage = storageFactory()
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

	listener, err := net.Listen("tcp", config.GRPCServerAddress)
	if err != nil {
		log.Fatalf("can not create listener: %v", err)
	}

	log.Printf("grpc server started on %v", config.GRPCServerAddress)
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("can not start grpc server: %v", err)
	}
}

func poolConfig(dbUrl string) *pgxpool.Config {
	dbConfig, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		log.Fatal("Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = int32(15)
	dbConfig.MinConns = int32(0)
	dbConfig.MaxConnLifetime = time.Hour
	dbConfig.MaxConnIdleTime = time.Minute * 30
	dbConfig.HealthCheckPeriod = time.Minute
	dbConfig.ConnConfig.ConnectTimeout = time.Second * 5

	return dbConfig
}

func startTracing(tracerEndpoint string) (*trace.TracerProvider, error) {
	headers := map[string]string{
		"content-type": "application/json",
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint(tracerEndpoint),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating new exporter: %w", err)
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(
			exporter,
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
			trace.WithBatchTimeout(trace.DefaultScheduleDelay*time.Millisecond),
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
		),
		trace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("broker-app"),
			),
		),
	)

	otel.SetTracerProvider(tracerProvider)

	return tracerProvider, nil
}
