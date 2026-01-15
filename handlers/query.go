package handlers

import (
	"context"
	"log"
	"search-event-go/middleware"
	"search-event-go/services"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryHandler struct {
	clickhouseService *services.ClickHouseService
}

func NewQueryHandler(clickhouseService *services.ClickHouseService) *QueryHandler {
	return &QueryHandler{
		clickhouseService: clickhouseService,
	}
}

func (h *QueryHandler) ExecuteQuery(c *fiber.Ctx) error {
	var request QueryRequest

	if err := c.BodyParser(&request); err != nil {
		return middleware.NewValidationError("Invalid request body", err.Error())
	}

	if request.Query == "" {
		return middleware.NewValidationError("Query is required", "The 'query' field cannot be empty")
	}

	overallStartTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var executionTimes []time.Duration
	var errors []error

	numGoroutines := 1
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			startTime := time.Now()

			rows, err := h.clickhouseService.ExecuteQuery(ctx, request.Query)
			if err != nil {
				log.Printf("Query execution error in goroutine %d: %v", routineID, err)
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
				return
			}
			defer rows.Close()

			responseTime := time.Since(startTime)
			log.Printf("Goroutine %d: Query executed successfully. Response time: %v", routineID, responseTime)

			mu.Lock()
			executionTimes = append(executionTimes, responseTime)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	overallResponseTime := time.Since(overallStartTime)

	if len(errors) > 0 {
		log.Printf("Query execution failed in %d goroutine(s)", len(errors))
		return middleware.NewInternalServerError("Query execution failed", errors[0].Error())
	}

	log.Printf("All queries executed successfully. Overall response time: %v", overallResponseTime)

	// Build execution times arrays dynamically for all goroutines
	executionTimesMs := make([]int64, len(executionTimes))
	executionTimesStr := make([]string, len(executionTimes))
	for i, et := range executionTimes {
		executionTimesMs[i] = et.Milliseconds()
		executionTimesStr[i] = et.String()
	}

	response := fiber.Map{
		"overallResponseTime":   overallResponseTime.String(),
		"overallResponseTimeMs": overallResponseTime.Milliseconds(),
		"executionTimes":        executionTimesMs,
		"executionTimesString":  executionTimesStr,
		"goroutineCount":        len(executionTimes),
	}

	return c.JSON(response)
}
