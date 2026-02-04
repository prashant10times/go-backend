package handlers

import (
	"log"
	"net/http"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

type PastEditionsHandler struct {
	workerPool           chan int
	pastEditionsService  *services.PastEditionsService
	transformDataService *services.TransformDataService
}

func NewPastEditionsHandler(pastEditionsService *services.PastEditionsService, transformDataService *services.TransformDataService, workerCount int) *PastEditionsHandler {
	workerPool := make(chan int, workerCount)
	for i := 0; i < workerCount; i++ {
		workerPool <- i
	}
	return &PastEditionsHandler{
		workerPool:           workerPool,
		pastEditionsService:  pastEditionsService,
		transformDataService: transformDataService,
	}
}

func (h *PastEditionsHandler) GetPastEditions(c *fiber.Ctx) error {
	startTime := time.Now()
	workerID := <-h.workerPool
	defer func() {
		h.workerPool <- workerID
	}()

	eventId := c.Params("eventId")
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

	var query models.PastEditionsQuery
	if err := c.QueryParser(&query); err != nil {
		return middleware.NewValidationError("Invalid query parameters", err.Error())
	}
	if err := query.Validate(); err != nil {
		return middleware.NewValidationError("Validation failed", err.Error())
	}

	log.Printf("Past editions: eventId=%s limit=%d offset=%d editionType=%s", eventId, query.Limit, query.Offset, query.EditionType)

	result, err := h.pastEditionsService.GetPastEditions(eventId, query.Limit, query.Offset, query.ParsedEditionType)
	if err != nil {
		if customErr, ok := err.(*middleware.CustomError); ok {
			return customErr
		}
		return middleware.NewInternalServerError("Error getting past editions", err.Error())
	}

	dataAsMaps := make([]map[string]interface{}, 0, len(result.Data))
	for _, item := range result.Data {
		dataAsMaps = append(dataAsMaps, map[string]interface{}{
			"id":            item.Id,
			"eventLocation": item.EventLocation,
			"status":        item.Status,
			"format":        item.Format,
			"start":         item.Start,
			"end":           item.End,
		})
	}
	pagination := models.PaginationDto{Limit: query.Limit, Offset: query.Offset}
	listResponse, err := h.transformDataService.BuildClickhouseListViewResponse(dataAsMaps, pagination, result.Count, c)
	if err != nil {
		return middleware.NewInternalServerError("Error building past editions response", err.Error())
	}

	responseTime := time.Since(startTime).Seconds()
	successResponse := fiber.Map{
		"status":     "success",
		"statusCode": http.StatusOK,
		"meta": fiber.Map{
			"responseTime": responseTime,
			"pagination": fiber.Map{
				"page": (query.Offset / query.Limit) + 1,
			},
		},
		"data": listResponse,
	}
	return c.JSON(successResponse)
}
