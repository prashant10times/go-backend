package routes

import (
	"search-event-go/handlers"
	"github.com/gofiber/fiber/v2"
)

func SetupRoutes(app *fiber.App) {
	healthHandler := handlers.NewHealthHandler(10)

	// API v1 routes
	api := app.Group("/v1")
	api.Get("/health", healthHandler.HealthCheck)
}
