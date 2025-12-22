package eventtype

import (
	"search-event-go/middleware"
	"search-event-go/models"

	"github.com/gofiber/fiber/v2"
)

type EventTypeController struct {
	EventTypeService *EventTypeService
}

func NewEventTypeController(eventTypeService *EventTypeService) *EventTypeController {
	return &EventTypeController{EventTypeService: eventTypeService}
}

func (c *EventTypeController) GetAll(ctx *fiber.Ctx) error {
	var query models.SearchEventTypeDto

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

	data, err := c.EventTypeService.GetAll(query)
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
