package server

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type PrometheusMetrics struct {
	RpcDurations *prometheus.HistogramVec
	RpcCounts    *prometheus.CounterVec
}

func NewPrometheusMetrics() *PrometheusMetrics {
	pm := &PrometheusMetrics{
		RpcDurations: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "grpc_server_duration_seconds",
				Help:    "Histogram of RPC duration by method",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "status_code"},
		),
		RpcCounts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpc_server_requests_total",
				Help: "Total number of RPC requests by method and status",
			},
			[]string{"method", "status_code"},
		),
	}
	prometheus.MustRegister(pm.RpcDurations)
	prometheus.MustRegister(pm.RpcCounts)
	return pm
}

func (pm *PrometheusMetrics) PrometheusUnaryCollector(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	duration := time.Since(start).Seconds()
	st, _ := status.FromError(err)

	go func() {
		pm.RpcDurations.WithLabelValues(info.FullMethod, st.Code().String()).Observe(duration)
		if err != nil {
			pm.RpcCounts.WithLabelValues(info.FullMethod, st.Code().String()).Inc()
		} else {
			pm.RpcCounts.WithLabelValues(info.FullMethod, codes.OK.String()).Inc()
		}
	}()
	return resp, err
}
