package designation

import (
	"search-event-go/middleware"
	"search-event-go/models"

	"github.com/gofiber/fiber/v2"
)

type DesignationController struct {
	designationService *DesignationService
}

func NewDesignationController(designationService *DesignationService) *DesignationController {
	return &DesignationController{designationService: designationService}
}

func (c *DesignationController) GetDesignations(ctx *fiber.Ctx) error {
	var query models.SearchDesignationDto

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

	data, err := c.designationService.GetDesignation(query)
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
