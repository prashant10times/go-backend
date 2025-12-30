package locationpolygon

import (
	"search-event-go/middleware"

	"github.com/gofiber/fiber/v2"
)

type LocationPolygonController struct {
	locationPolygonService *LocationPolygonService
}

func NewLocationPolygonController(locationPolygonService *LocationPolygonService) *LocationPolygonController {
	return &LocationPolygonController{locationPolygonService: locationPolygonService}
}

func (c *LocationPolygonController) GetLocationPolygons(ctx *fiber.Ctx) error {
	eventId := ctx.Params("eventId")
	if eventId == "" {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]interface{}{
			"error": "eventId parameter is required",
		})
	}

	data, err := c.locationPolygonService.GetLocationPolygons(eventId)
	if err != nil {
		if customErr, ok := err.(*middleware.CustomError); ok {
			return customErr
		}
		return middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	return ctx.JSON(data)
}
