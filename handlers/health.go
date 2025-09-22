package handlers

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
)

type HealthHandler struct {
	workerPool chan int
	nextWorker int
}

func NewHealthHandler(workerCount int) *HealthHandler {
	// Initialize worker pool with worker IDs
	workerPool := make(chan int, workerCount)
	for i := 0; i < workerCount; i++ {
		workerPool <- i
	}

	return &HealthHandler{
		workerPool: workerPool,
		nextWorker: workerCount,
	}
}

func (h *HealthHandler) HealthCheck(c *fiber.Ctx) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultChan := make(chan fiber.Map, 1)

	go h.processHealthCheck(ctx, resultChan)

	select {
	case result := <-resultChan:
		return c.JSON(result)
	case <-ctx.Done():
		return c.Status(500).JSON(fiber.Map{
			"status":  "error",
			"message": "Health check timeout",
		})
	}
}

func (h *HealthHandler) processHealthCheck(ctx context.Context, resultChan chan<- fiber.Map) {
	result := fiber.Map{
		"status": "ok",
	}

	select {
	case resultChan <- result:
	case <-ctx.Done():
		return
	}
}
