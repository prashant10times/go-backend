package health

import (
	"context"
	"log"
	"search-event-go/redis"
	"search-event-go/services"
	"time"
)

func DatabaseHealthMonitor(dbService *services.DatabaseService, clickhouseService *services.ClickHouseService) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := dbService.CheckHealth(); err != nil {
			log.Printf("ERROR: PostgreSQL connection health check failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := clickhouseService.CheckHealth(ctx); err != nil {
			log.Printf("ERROR: ClickHouse connection health check failed: %v", err)
		}
		cancel()

		if err := redis.CheckHealth(); err != nil {
			log.Printf("ERROR: Redis connection health check failed: %v", err)
		}
	}
}
