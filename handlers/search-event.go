package handlers

import (
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
	request.EditionsGte = c.Query("editions.gte")
	request.EditionsLte = c.Query("editions.lte")
	request.EditionsGt = c.Query("editions.gt")
	request.EditionsLt = c.Query("editions.lt")
	request.FollowingGte = c.Query("following.gte")
	request.FollowingLte = c.Query("following.lte")
	request.FollowingGt = c.Query("following.gt")
	request.FollowingLt = c.Query("following.lt")
	request.InboundScoreGte = c.Query("inboundScore.gte")
	request.InboundScoreLte = c.Query("inboundScore.lte")
	request.InternationalScoreGte = c.Query("internationalScore.gte")
	request.InternationalScoreLte = c.Query("internationalScore.lte")
	request.TrustScoreGte = c.Query("trustScore.gte")
	request.TrustScoreLte = c.Query("trustScore.lte")
	request.ImpactScoreGte = c.Query("impactScore.gte")
	request.ImpactScoreLte = c.Query("impactScore.lte")
	request.EconomicImpactGte = c.Query("economicImpact.gte")
	request.EconomicImpactLte = c.Query("economicImpact.lte")
	request.CalendarType = c.Query("calendar_type")

	userID, ok := c.Locals("userId").(string)
	if !ok || userID == "" {
		return middleware.NewAuthorizationError("User ID not found in request context")
	}

	request.APIID = Apis["SEARCH_EVENTS"].ID

	if err := (&request).Validate(); err != nil {
		log.Printf("Validation error: %v", err)
		return middleware.NewValidationError("Request validation failed", err.Error())
	}

	result, err := h.searchEventService.GetEventDataV2(userID, request.APIID, request.FilterDataDto, request.PaginationDto, request.ResponseDataDto, request.ShowValues, c)
	if err != nil {
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
