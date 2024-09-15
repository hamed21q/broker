package main

import (
	"BaleBroker/broker"
	db "BaleBroker/db"
	memory "BaleBroker/db/memory"
	sql "BaleBroker/db/postgres"
	postgres "BaleBroker/db/postgres/crud"
	cql "BaleBroker/db/scylla/cql"
	"BaleBroker/gapi/pb"
	"BaleBroker/gapi/server"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var WriterFactory = map[string]func(db db.DB) db.Writer{
	"BATCH": func(d db.DB) db.Writer {
		return db.NewBatchWriter(d)
	},
	"PARALLEL": func(d db.DB) db.Writer {
		return db.NewConcurrentWriter(d)
	},
}

var DBFactory = map[string]func(interface{}) db.DB{
	"POSTGRES": func(conn interface{}) db.DB {
		return sql.NewPostgresDb(conn.(*pgxpool.Pool), postgres.New())
	},
	"CASSANDRA": func(conn interface{}) db.DB {
		return cql.NewCqlDb(conn.(*gocql.Session))
	},
	"MEMORY": func(conn interface{}) db.DB {
		return memory.NewMemoryDB()
	},
}

var ConnectionFactory = map[string]func(config utils.Config) (interface{}, func()){
	"POSTGRES": func(config utils.Config) (interface{}, func()) {
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig(config))
		if err != nil {
			log.Fatalf("can not connect to : %v", config.PGSource)
		}
		return pool, func() { pool.Config() }
	},
	"CASSANDRA": func(config utils.Config) (interface{}, func()) {
		cluster := gocql.NewCluster(config.CassandraSource)
		cluster.Keyspace = config.CassandraKeySpace
		cluster.Consistency = gocql.Quorum
		cluster.Port = config.CassandraPort
		session, err := cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
		return session, func() { session.Close() }
	},
	"MEMORY": func(config utils.Config) (interface{}, func()) {
		return nil, func() {}
	},
}

var SyncFactory = map[string]func(interface{}, *pkg.SequentialIdentifier) pkg.Sync{
	"POSTGRES": func(conn interface{}, idGen *pkg.SequentialIdentifier) pkg.Sync {
		queries := postgres.New()
		return pkg.NewPGSync(queries, conn.(*pgxpool.Pool), idGen)
	},
	"CASSANDRA": func(conn interface{}, idGen *pkg.SequentialIdentifier) pkg.Sync {
		return nil
	},
	"MEMORY": func(conn interface{}, idGen *pkg.SequentialIdentifier) pkg.Sync {
		return nil
	},
}

func main() {
	config, err := utils.LoadConfig(".")
	if err != nil {
		log.Fatalf("can not load the config: %v", err)
	}

	_, err = startTracing(config.TracerSource)
	if err != nil {
		log.Fatalf("can not start tracing: %v", err)
	}

	go func() {
		log.Println("Starting pprof server on port :6060")
		if err := http.ListenAndServe(config.ProfilingServerAddress, nil); err != nil {
			log.Fatalf("Failed to start pprof server: %v", err)
		}
	}()

	idGen := pkg.NewSequentialIdentifier()

	connFactory, ok := ConnectionFactory[config.Storage]
	if !ok {
		log.Fatalf("can not connect to storage: %v", config.Storage)
	}
	conn, closer := connFactory(config)
	defer closer()

	dbFactory := DBFactory[config.Storage]
	d := dbFactory(conn)

	writerFactory, ok := WriterFactory[config.WriteMode]
	if !ok {
		log.Fatalf("undefiened writer: %v", config.WriteMode)
	}
	writer := writerFactory(d)

	store := db.NewStore(writer, d)
	baleBroker := broker.NewBaleBroker(store, idGen)

	syncFactory := SyncFactory[config.Storage]
	sync := syncFactory(conn, idGen)
	if sync != nil {
		err = sync.Sync(context.Background())
		if err != nil {
			log.Printf("can not sync with db : %v", err.Error())
		}
	}

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

func poolConfig(config utils.Config) *pgxpool.Config {
	dbConfig, err := pgxpool.ParseConfig(config.PGSource)
	if err != nil {
		log.Fatal("Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = int32(config.PGConnectionPoolSize)
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
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(0.1))),
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
