package handlers

import (
	"context"
	"search-event-go/middleware"
	"time"

	"github.com/gofiber/fiber/v2"
)

type HealthHandler struct {
	workerPool chan int
	nextWorker int
}

func NewHealthHandler(workerCount int) *HealthHandler {
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
		return middleware.NewInternalServerError("Health check timeout", "Health check operation timed out after 5 seconds")
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
