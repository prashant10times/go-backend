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

		finalStatusCode := statusCode
		if err != nil {
			if customErr, ok := err.(*CustomError); ok {
				finalStatusCode = customErr.StatusCode
			} else if fiberErr, ok := err.(*fiber.Error); ok {
				finalStatusCode = fiberErr.Code
			} else {
				finalStatusCode = 500
			}
		}

		route := c.Route().Path
		if route == "" {
			route = "unknown"
		}

		if err != nil {
			fmt.Printf("[METRICS] %s %s -> %d in %.3fs (ERROR: %v)\n", c.Method(), route, finalStatusCode, duration, err)
		} else {
			fmt.Printf("[METRICS] %s %s -> %d in %.3fs\n", c.Method(), route, finalStatusCode, duration)
		}

		httpRequestsTotal.WithLabelValues(c.Method(), route, fmt.Sprintf("%d", finalStatusCode)).Inc()
		httpRequestDuration.WithLabelValues(c.Method(), route, fmt.Sprintf("%d", finalStatusCode)).Observe(duration)

		return err
	}
}
