package main

import (
	"log"
	"search-event-go/config"
	"search-event-go/routes"
	"search-event-go/services"
	"time"

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

	app := fiber.New(fiber.Config{
		AppName: "search-event-api",
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{
				"statusCode": code,
				"message":    "ERROR",
				"data": fiber.Map{
					"message": "An error occurred",
					"error":   err.Error(),
				},
				"meta": fiber.Map{
					"timestamp": time.Now().Unix(),
				},
			})
		},
	})

	app.Use(logger.New())
	app.Use(cors.New())

	routes.SetupRoutes(app, dbService, clickhouseService)

	log.Printf("Server starting on port %s", cfg.Port)
	log.Fatal(app.Listen(":" + cfg.Port))
}
