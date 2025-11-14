package routes

import (
	"search-event-go/config"
	"search-event-go/handlers"
	"search-event-go/middleware"
	"search-event-go/services"
	"strconv"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupRoutes(app *fiber.App, dbService *services.DatabaseService, clickhouseService *services.ClickHouseService, cfg *config.Config) {

	limit, _ := strconv.Atoi(cfg.ThrottleLimit)
	ttl, _ := strconv.Atoi(cfg.ThrottleTTL)
	blockDuration, _ := strconv.Atoi(cfg.ThrottleBlockDuration)

	healthHandler := handlers.NewHealthHandler(10)
	authHandler := handlers.NewAuthHandler(dbService.DB)
	sharedFunctionService := services.NewSharedFunctionService(dbService.DB, clickhouseService)
	loginHandler := handlers.NewLoginHandler(dbService.DB)
	searchEventService := services.NewSearchEventService(dbService.DB, sharedFunctionService, clickhouseService, cfg)
	searchEventsHandler := handlers.NewSearchEventsHandler(100, searchEventService)

	api := app.Group("/v1")
	api.Get("/health", healthHandler.HealthCheck)
	api.Post("/register", authHandler.Register)
	api.Post("/login", loginHandler.Login)

	//protected route
	api.Use(middleware.JwtAuthMiddleware(dbService.DB))
	api.Use(middleware.SearchRateLimit(limit, time.Duration(ttl)*time.Millisecond, time.Duration(blockDuration)*time.Millisecond))
	api.Get("/search-events", searchEventsHandler.SearchEvents)

	app.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})))
}
