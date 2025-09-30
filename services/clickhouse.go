package services

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"search-event-go/config"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseService struct {
	conn driver.Conn
}

func NewClickHouseService(cfg *config.Config) (*ClickHouseService, error) {
	if cfg.ClickhouseURL == "" {
		return nil, fmt.Errorf("CLICKHOUSE_URL not configured")
	}

	parsedURL, err := url.Parse(cfg.ClickhouseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ClickHouse URL: %w", err)
	}

	username := parsedURL.User.Username()
	password, _ := parsedURL.User.Password()
	host := parsedURL.Hostname()
	port := parsedURL.Port()

	options := &clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsedURL.Path, "/"), // Remove leading slash
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		MaxOpenConns:    50,
		MaxIdleConns:    20,
		ConnMaxLifetime: 2 * time.Hour,
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 1 * time.Minute,
			}
			return dialer.DialContext(ctx, "tcp", addr)
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	log.Println("ClickHouse connection established successfully")

	return &ClickHouseService{conn: conn}, nil
}

func (c *ClickHouseService) GetConnection() driver.Conn {
	return c.conn
}

func (c *ClickHouseService) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *ClickHouseService) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}
