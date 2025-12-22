package routes

import (
	"search-event-go/config"
	"search-event-go/handlers"
	"search-event-go/helpers/category"
	"search-event-go/helpers/convert"
	"search-event-go/helpers/designation"
	"search-event-go/helpers/eventtype"
	"search-event-go/helpers/location"
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
	sharedFunctionService := services.NewSharedFunctionService(dbService.DB, clickhouseService, cfg)
	transformDataService := services.NewTransformDataService()
	loginHandler := handlers.NewLoginHandler(dbService.DB)
	searchEventService := services.NewSearchEventService(dbService.DB, sharedFunctionService, transformDataService, clickhouseService, cfg)
	searchEventsHandler := handlers.NewSearchEventsHandler(100, searchEventService)

	// helper module
	categoryService := category.NewCategoryService(clickhouseService)
	categoryController := category.NewCategoryController(categoryService)
	designationService := designation.NewDesignationService(clickhouseService)
	designationController := designation.NewDesignationController(designationService)
	locationService := location.NewLocationService(clickhouseService)
	locationController := location.NewLocationController(locationService)
	eventTypeService := eventtype.NewEventTypeService(clickhouseService)
	eventTypeController := eventtype.NewEventTypeController(eventTypeService)

	convertService := convert.NewConvertService(clickhouseService)
	convertController := convert.NewConvertController(convertService)

	api := app.Group("/v1")
	api.Get("/health", healthHandler.HealthCheck)
	api.Post("/register", authHandler.Register)
	api.Post("/login", loginHandler.Login)

	// helper module routes
	app.Get("/categories", categoryController.GetCategories)
	app.Get("/designations", designationController.GetDesignations)
	app.Get("/locations", locationController.GetLocations)
	app.Get("/convert/ids", convertController.ConvertIds)
	app.Get("/event-types", eventTypeController.GetAll)
	//protected route
	api.Use(middleware.JwtAuthMiddleware(dbService.DB))
	api.Use(middleware.SearchRateLimit(limit, time.Duration(ttl)*time.Millisecond, time.Duration(blockDuration)*time.Millisecond))
	api.Get("/search-events", searchEventsHandler.SearchEvents)
	api.Post("/search-events", searchEventsHandler.SearchEventsPost)

	app.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})))
}
