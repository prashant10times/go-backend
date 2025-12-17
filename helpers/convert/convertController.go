package convert

import (
	"search-event-go/models"

	"github.com/gofiber/fiber/v2"
)

type ConvertController struct {
	convertService *ConvertService
}

func NewConvertController(convertService *ConvertService) *ConvertController {
	return &ConvertController{convertService: convertService}
}

func (c *ConvertController) ConvertIds(ctx *fiber.Ctx) error {
	var query models.ConvertSchemaDto

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

	data, err := c.convertService.ConvertIds(query)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.JSON(map[string]interface{}{
		"data": data,
	})
}
