package models

import (
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var SortFieldMap = map[string]string{
	"id":                  "event_id",
	"end":                 "end_date",
	"start":               "start_date",
	"created":             "event_created",
	"following":           "event_followers",
	"exhibitors":          "event_exhibitor",
	"speakers":            "event_speaker",
	"avgRating":           "event_avgRating",
	"sponsors":            "event_sponsor",
	"estimatedExhibitors": "exhibitors_mean",
	"impactScore":         "impact_score",
}

type PaginationDto struct {
	Limit  int    `json:"limit" form:"limit"`
	Offset int    `json:"offset" form:"offset"`
	Sort   string `json:"sort" form:"sort"`
}

func (p *PaginationDto) SetDefaultValues() {
	if p.Limit == 0 {
		p.Limit = 20
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
}

func (p *PaginationDto) Validate() error {
	p.SetDefaultValues()

	return validation.ValidateStruct(p,
		validation.Field(&p.Limit, validation.Max(20).Error("Limit must be less than or equal to 20")),
		validation.Field(&p.Offset, validation.Min(0).Error("Offset must be non-negative")),
		validation.Field(&p.Sort, validation.By(func(value interface{}) error {
			sortStr := value.(string)
			if sortStr == "" {
				return nil
			}

			sortFields := strings.Split(sortStr, ",")
			for _, field := range sortFields {
				cleanField := strings.TrimSpace(field)
				cleanField = strings.TrimPrefix(cleanField, "-")
				if _, exists := SortFieldMap[cleanField]; !exists {
					validFields := make([]string, 0, len(SortFieldMap))
					for key := range SortFieldMap {
						validFields = append(validFields, key)
					}
					return validation.NewError("invalid_sort_field", "Invalid sort field. Valid fields are: "+strings.Join(validFields, ", "))
				}
			}
			return nil
		})),
	)
}
