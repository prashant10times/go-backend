package middleware

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests received",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	httpRequestSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_size_bytes",
			Help:    "Size of HTTP requests in bytes",
			Buckets: prometheus.ExponentialBuckets(200, 2, 8), // 200B to ~25KB
		},
		[]string{"method", "path"},
	)

	httpResponseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "Size of HTTP responses in bytes",
			Buckets: prometheus.ExponentialBuckets(200, 2, 8),
		},
		[]string{"method", "path", "status"},
	)

	uptime = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "app_uptime_seconds_total",
			Help: "Total uptime of the API server",
		},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(httpRequestSize)
	prometheus.MustRegister(httpResponseSize)
	prometheus.MustRegister(uptime)
}

func PrometheusMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()

		err := c.Next()

		duration := time.Since(start).Seconds()
		statusCode := c.Response().StatusCode()

		fmt.Printf("[METRICS] %s %s -> %d in %.3fs\n", c.Method(), c.Path(), statusCode, duration)

		httpRequestsTotal.WithLabelValues(c.Method(), c.Path(), fmt.Sprintf("%d", statusCode)).Inc()
		httpRequestDuration.WithLabelValues(c.Method(), c.Path(), fmt.Sprintf("%d", statusCode)).Observe(duration)

		return err
	}
}
