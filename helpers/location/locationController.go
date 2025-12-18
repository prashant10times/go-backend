package location

import (
	"search-event-go/middleware"
	"search-event-go/models"

	"github.com/gofiber/fiber/v2"
)

type LocationController struct {
	locationService *LocationService
}

func NewLocationController(locationService *LocationService) *LocationController {
	return &LocationController{locationService: locationService}
}

func (c *LocationController) GetLocations(ctx *fiber.Ctx) error {
	var query models.LocationQueryDto

	// parsing '_' and '.' Notation query params
	query.ID10x = ctx.Query("id_10x")

	if err := ctx.QueryParser(&query); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]interface{}{
			"error": err.Error(),
		})
	}

	if err := query.Validate(); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]interface{}{
			"error": err.Error(),
		})
	}

	data, err := c.locationService.SearchLocations(query)
	if err != nil {
		if customErr, ok := err.(*middleware.CustomError); ok {
			return customErr
		}
		return middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	return ctx.JSON(map[string]interface{}{
		"data": data,
	})
}
