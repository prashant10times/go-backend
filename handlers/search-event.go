package handlers

import (
	"log"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

type Api struct {
	Endpoint string `json:"endpoint"`
	ID       string `json:"id"`
}

var Apis = map[string]Api{
	"SEARCH_EVENTS": {
		Endpoint: "search-events",
		ID:       "3e9d0e4e-b9c9-4e16-8468-9910075c9b88",
	},
}

type SearchEventsHandler struct {
	workerPool         chan int
	nextWorker         int
	searchEventService *services.SearchEventService
}

func NewSearchEventsHandler(workerCount int, searchEventService *services.SearchEventService) *SearchEventsHandler {
	workerPool := make(chan int, workerCount)
	for i := 0; i < workerCount; i++ {
		workerPool <- i
	}

	return &SearchEventsHandler{
		workerPool:         workerPool,
		searchEventService: searchEventService,
		nextWorker:         workerCount,
	}
}

func (h *SearchEventsHandler) SearchEvents(c *fiber.Ctx) error {
	var request models.SearchEventsRequest

	startTime := time.Now()

	log.Printf("Raw query string: %s", c.Request().URI().QueryString())

	if err := c.QueryParser(&request); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"statusCode": 400,
			"message":    "VALIDATION_ERROR",
			"data": fiber.Map{
				"message": "Invalid query parameters",
				"error":   err.Error(),
			},
			"meta": fiber.Map{
				"responseTime": time.Since(startTime).Seconds(),
			},
		})
	}

	request.StartGte = c.Query("start.gte")
	request.EndLte = c.Query("end.lte")
	request.StartLte = c.Query("start.lte")
	request.EndGte = c.Query("end.gte")
	request.StartGt = c.Query("start.gt")
	request.EndGt = c.Query("end.gt")
	request.StartLt = c.Query("start.lt")
	request.EndLt = c.Query("end.lt")
	request.ActiveGte = c.Query("active.gte")
	request.ActiveLte = c.Query("active.lte")
	request.ActiveGt = c.Query("active.gt")
	request.ActiveLt = c.Query("active.lt")
	request.SpeakerGte = c.Query("speaker.gte")
	request.SpeakerLte = c.Query("speaker.lte")
	request.SpeakerGt = c.Query("speaker.gt")
	request.SpeakerLt = c.Query("speaker.lt")
	request.ExhibitorsGte = c.Query("exhibitors.gte")
	request.ExhibitorsLte = c.Query("exhibitors.lte")
	request.ExhibitorsGt = c.Query("exhibitors.gt")
	request.ExhibitorsLt = c.Query("exhibitors.lt")
	request.FollowingGte = c.Query("following.gte")
	request.FollowingLte = c.Query("following.lte")
	request.FollowingGt = c.Query("following.gt")
	request.FollowingLt = c.Query("following.lt")

	UserID := "4677ea16-8b96-4964-8edf-ff7c344402d8"
	request.APIID = Apis["SEARCH_EVENTS"].ID

	if err := (&request).Validate(); err != nil {
		log.Printf("Validation error: %v", err)
		return c.Status(400).JSON(fiber.Map{
			"statusCode": 400,
			"message":    "VALIDATION_ERROR",
			"data": fiber.Map{
				"message": "Request validation failed",
				"error":   err.Error(),
			},
			"meta": fiber.Map{
				"responseTime": time.Since(startTime).Seconds(),
			},
		})
	}

	result, err := h.searchEventService.GetEventDataV2(UserID, request.APIID, request.FilterDataDto, request.PaginationDto, request.ResponseDataDto, c)
	if err != nil {
		statusCode := 500
		message := "INTERNAL_SERVER_ERROR"

		if strings.Contains(err.Error(), "unauthorized filters") {
			statusCode = 403
			message = "FORBIDDEN"
		} else if strings.Contains(err.Error(), "unauthorized advanced parameters") {
			statusCode = 403
			message = "FORBIDDEN"
		} else if strings.Contains(err.Error(), "Daily limit reached") {
			statusCode = 429
			message = "TOO_MANY_REQUESTS"
		}

		return c.Status(statusCode).JSON(fiber.Map{
			"statusCode": statusCode,
			"message":    message,
			"data": fiber.Map{
				"message": "Error getting event data",
				"error":   err.Error(),
			},
			"meta": fiber.Map{
				"responseTime": time.Since(startTime).Seconds(),
			},
		})
	}

	// Placeholder response - replace with actual ElasticSearch service call
	// result := fiber.Map{
	// 	"status": "success",
	// 	"data": fiber.Map{
	// 		"user_id":       UserID,
	// 		"api_id":        request.APIID,
	// 		"filter_fields": request.FilterFields,
	// 		"pagination":    request.Pagination,
	// 	},
	// 	"message": "Search events endpoint called with validated data",
	// }

	return c.JSON(result)
}
