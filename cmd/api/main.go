package main

import (
	"log"
	"search-event-go/config"
	"search-event-go/middleware"
	"search-event-go/redis"
	"search-event-go/routes"
	"search-event-go/services"

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

	redis.InitRedis()
	defer redis.Close()

	app := fiber.New(fiber.Config{
		AppName:      "search-event-api",
		ErrorHandler: middleware.GlobalErrorHandler,
	})

	app.Use(middleware.RecoverMiddleware())
	app.Use(logger.New())
	app.Use(cors.New())

	routes.SetupRoutes(app, dbService, clickhouseService, cfg)

	log.Printf("Server starting on port %s", cfg.Port)
	log.Fatal(app.Listen(":" + cfg.Port))
}
