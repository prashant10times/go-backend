package models

import (
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var SortFieldMap = map[string]string{
	"id":                    "event_id",
	"end":                   "end_date",
	"start":                 "start_date",
	"created":               "event_created",
	"following":             "event_followers",
	"visitor":               "event_followers",
	"visitors":              "estimatedVisitorsMean",
	"exhibitors":            "event_exhibitor",
	"speakers":              "event_speaker",
	"avgRating":             "event_avgRating",
	"sponsors":              "event_sponsor",
	"estimatedExhibitors":   "exhibitors_mean",
	"impactScore":           "impactScore",
	"score":                 "event_score",
	"inboundScore":          "inboundScore",
	"internationalScore":    "internationalScore",
	"inboundEstimate":       "inboundAttendance",
	"internationalEstimate": "internationalAttendance",
	"title":                 "event_name",
	"status":                "status",
	"duration":              "duration",
	"updated":               "event_updated",
}

type PaginationDto struct {
	Limit  int    `json:"limit" form:"limit"`
	Offset int    `json:"offset" form:"offset"`
	Sort   string `json:"sort" form:"sort"`
}

func (p *PaginationDto) SetDefaultValues() {
	if p.Limit == 0 {
		// p.Limit = 20
		p.Limit = 500
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
}

func (p *PaginationDto) Validate() error {
	p.SetDefaultValues()

	return validation.ValidateStruct(p,
		validation.Field(&p.Limit, validation.Max(100).Error("Limit must be less than or equal to 500")),
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
				if cleanField == "distance" {
					continue
				}
				if _, exists := SortFieldMap[cleanField]; !exists {
					validFields := make([]string, 0, len(SortFieldMap)+1)
					for key := range SortFieldMap {
						validFields = append(validFields, key)
					}
					validFields = append(validFields, "distance")
					return validation.NewError("invalid_sort_field", "Invalid sort field. Valid fields are: "+strings.Join(validFields, ", "))
				}
			}
			return nil
		})),
	)
}
