package main

import (
	"log"
	"search-event-go/config"
	"search-event-go/health"
	"search-event-go/middleware"
	"search-event-go/redis"
	"search-event-go/routes"
	"search-event-go/services"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func main() {
	cfg := config.LoadConfig()

	dbService, err := services.NewDatabaseService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer dbService.Close()

	clickhouseService, err := services.NewClickHouseService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize ClickHouse: %v", err)
	}
	defer clickhouseService.Close()

	if err := redis.InitRedis(); err != nil {
		log.Fatalf("Failed to initialize Redis: %v", err)
	}
	defer redis.Close()

	app := fiber.New(fiber.Config{
		AppName:      "search-event-api",
		ErrorHandler: middleware.GlobalErrorHandler,
	})

	app.Use(middleware.RecoverMiddleware())
	app.Use(middleware.RequestLoggerMiddleware())
	app.Use(logger.New())

	allowOrigins := cfg.Origin
	allowMethods := strings.Trim(cfg.AllowMethods, `[]"`)
	app.Use(cors.New(
		cors.Config{
			AllowCredentials: true,
			AllowOrigins:     allowOrigins,
			AllowMethods:     allowMethods,
		},
	))
	app.Use(middleware.PrometheusMiddleware()) //prometheus middleware

	routes.SetupRoutes(app, dbService, clickhouseService, cfg)

	go health.DatabaseHealthMonitor(dbService, clickhouseService)

	log.Printf("Server starting on port %s", cfg.Port)
	log.Fatal(app.Listen(":" + cfg.Port))
}
