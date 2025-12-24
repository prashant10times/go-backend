package handlers

import (
	"log"
	"search-event-go/middleware"
	"search-event-go/services"
	"strings"

	"github.com/gofiber/fiber/v2"
)

type RankingHandler struct {
	workerPool     chan int
	nextWorker     int
	rankingService *services.RankingService
}

func NewRankingHandler(rankingService *services.RankingService, workerCount int) *RankingHandler {
	workerPool := make(chan int, workerCount)
	for i := 0; i < workerCount; i++ {
		workerPool <- i
	}
	return &RankingHandler{
		workerPool:     workerPool,
		nextWorker:     workerCount,
		rankingService: rankingService,
	}
}

func (h *RankingHandler) Ranking(c *fiber.Ctx) error {
	workerID := <-h.workerPool
	defer func() {
		h.workerPool <- workerID
	}()

	log.Printf("Raw query string: %s", c.Request().URI().QueryString())
	eventId := c.Params("eventId")

	// Validate eventId
	if eventId == "" {
		return middleware.NewValidationError("Event ID is required", "eventId parameter cannot be empty")
	}

	if strings.Contains(strings.ToLower(eventId), "union") ||
		strings.Contains(strings.ToLower(eventId), "select") ||
		strings.Contains(strings.ToLower(eventId), "drop") ||
		strings.Contains(strings.ToLower(eventId), "delete") ||
		strings.Contains(strings.ToLower(eventId), "insert") ||
		strings.Contains(strings.ToLower(eventId), "update") {
		return middleware.NewValidationError("Invalid event ID format", "eventId contains invalid characters")
	}

	result, err := h.rankingService.GetEventRankings(eventId)
	if err != nil {
		if customErr, ok := err.(*middleware.CustomError); ok {
			return customErr
		}

		return middleware.NewInternalServerError("Error getting event rankings", err.Error())
	}

	return c.JSON(result)
}
