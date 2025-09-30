package routes

import (
	"search-event-go/handlers"
	"search-event-go/services"

	"github.com/gofiber/fiber/v2"
)

func SetupRoutes(app *fiber.App, dbService *services.DatabaseService, clickhouseService *services.ClickHouseService) {
	healthHandler := handlers.NewHealthHandler(10)
	sharedFunctionService := services.NewSharedFunctionService(dbService.DB)
	searchEventService := services.NewSearchEventService(dbService.DB, sharedFunctionService, clickhouseService)
	searchEventsHandler := handlers.NewSearchEventsHandler(100, searchEventService)

	api := app.Group("/v1")
	api.Get("/health", healthHandler.HealthCheck)
	api.Get("/search-events", searchEventsHandler.SearchEvents)
}
