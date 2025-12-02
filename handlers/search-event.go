package handlers

import (
	"encoding/json"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"

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

func (h *SearchEventsHandler) extractDotNotationFields(c *fiber.Ctx, request *models.SearchEventsRequest, bodyMap map[string]interface{}) {
	getValue := func(key string) string {
		// for GET requests
		if val := c.Query(key); val != "" {
			return val
		}
		// for POST requests
		if bodyMap != nil {
			if val, ok := bodyMap[key].(string); ok {
				return val
			}
		}
		return ""
	}

	request.StartGte = getValue("start.gte")
	request.EndLte = getValue("end.lte")
	request.StartLte = getValue("start.lte")
	request.EndGte = getValue("end.gte")
	request.StartGt = getValue("start.gt")
	request.EndGt = getValue("end.gt")
	request.StartLt = getValue("start.lt")
	request.EndLt = getValue("end.lt")
	request.ActiveGte = getValue("active.gte")
	request.ActiveLte = getValue("active.lte")
	request.ActiveGt = getValue("active.gt")
	request.ActiveLt = getValue("active.lt")
	request.SpeakerGte = getValue("speaker.gte")
	request.SpeakerLte = getValue("speaker.lte")
	request.SpeakerGt = getValue("speaker.gt")
	request.SpeakerLt = getValue("speaker.lt")
	request.ExhibitorsGte = getValue("exhibitors.gte")
	request.ExhibitorsLte = getValue("exhibitors.lte")
	request.ExhibitorsGt = getValue("exhibitors.gt")
	request.ExhibitorsLt = getValue("exhibitors.lt")
	request.EditionsGte = getValue("editions.gte")
	request.EditionsLte = getValue("editions.lte")
	request.EditionsGt = getValue("editions.gt")
	request.EditionsLt = getValue("editions.lt")
	request.FollowingGte = getValue("following.gte")
	request.FollowingLte = getValue("following.lte")
	request.FollowingGt = getValue("following.gt")
	request.FollowingLt = getValue("following.lt")
	request.InboundScoreGte = getValue("inboundScore.gte")
	request.InboundScoreLte = getValue("inboundScore.lte")
	request.InternationalScoreGte = getValue("internationalScore.gte")
	request.InternationalScoreLte = getValue("internationalScore.lte")
	request.TrustScoreGte = getValue("trustScore.gte")
	request.TrustScoreLte = getValue("trustScore.lte")
	request.ImpactScoreGte = getValue("impactScore.gte")
	request.ImpactScoreLte = getValue("impactScore.lte")
	request.EconomicImpactGte = getValue("economicImpact.gte")
	request.EconomicImpactLte = getValue("economicImpact.lte")
	request.CalendarType = getValue("calendar_type")
}

func (h *SearchEventsHandler) processSearchRequest(c *fiber.Ctx, request *models.SearchEventsRequest, bodyMap map[string]interface{}) error {
	h.extractDotNotationFields(c, request, bodyMap)

	userID, ok := c.Locals("userId").(string)
	if !ok || userID == "" {
		return middleware.NewAuthorizationError("User ID not found in request context")
	}

	request.APIID = Apis["SEARCH_EVENTS"].ID

	if err := request.Validate(); err != nil {
		log.Printf("Validation error: %v", err)
		return middleware.NewValidationError("Request validation failed", err.Error())
	}

	result, err := h.searchEventService.GetEventDataV2(userID, request.APIID, request.FilterDataDto, request.PaginationDto, request.ResponseDataDto, request.ShowValues, c)
	if err != nil {
		// If error is already a CustomError, preserve it to maintain original message and details
		if customErr, ok := err.(*middleware.CustomError); ok {
			return customErr
		}

		if strings.Contains(err.Error(), "unauthorized filters") {
			return middleware.NewForbiddenError("Unauthorized filters", err.Error())
		} else if strings.Contains(err.Error(), "unauthorized advanced parameters") {
			return middleware.NewForbiddenError("Unauthorized advanced parameters", err.Error())
		} else if strings.Contains(err.Error(), "Daily limit reached") {
			return middleware.NewTooManyRequestsError("Daily limit reached", err.Error())
		}

		return middleware.NewInternalServerError("Error getting event data", err.Error())
	}

	return c.JSON(result)
}

func (h *SearchEventsHandler) SearchEvents(c *fiber.Ctx) error {
	workerID := <-h.workerPool
	defer func() {
		h.workerPool <- workerID
	}()

	var request models.SearchEventsRequest

	log.Printf("Raw query string: %s", c.Request().URI().QueryString())

	if err := c.QueryParser(&request); err != nil {
		return middleware.NewValidationError("Invalid query parameters", err.Error())
	}

	return h.processSearchRequest(c, &request, nil)
}

func (h *SearchEventsHandler) SearchEventsPost(c *fiber.Ctx) error {
	workerID := <-h.workerPool
	defer func() {
		h.workerPool <- workerID
	}()

	var request models.SearchEventsRequest
	var bodyMap map[string]interface{}

	bodyBytes := c.Body()
	log.Printf("Raw request body: %s", string(bodyBytes))

	if err := json.Unmarshal(bodyBytes, &bodyMap); err != nil {
		return middleware.NewValidationError("Invalid request body", err.Error())
	}

	if err := json.Unmarshal(bodyBytes, &request); err != nil {
		return middleware.NewValidationError("Invalid request body", err.Error())
	}

	return h.processSearchRequest(c, &request, bodyMap)
}
