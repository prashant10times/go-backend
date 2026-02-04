package models

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const PastEditionsLimitMax = 100
const PastEditionsLimitDefault = 20

type PastEditionsBasic struct {
	Id string `json:"id"`
	EventLocation map[string]interface{} `json:"eventLocation,omitempty"`
	Status string `json:"status"`
	Format *string `json:"format,omitempty"`
}

var PastEditionsSelectSpec = map[string]string{
	"id":           "edition_uuid",
	"event_id":     "event_id",
	"end_date":     "end_date",
	"status":       "status",
	"event_format": "event_format",
}

type PastEditionsQuery struct {
	Limit             int      `json:"limit" form:"limit"`
	Offset            int      `json:"offset" form:"offset"`
	EditionType       string   `json:"editionType,omitempty" form:"editionType"`
	ParsedEditionType []string `json:"-"`
}

func (q *PastEditionsQuery) SetDefaultValues() {
	if q.Limit == 0 {
		q.Limit = PastEditionsLimitDefault
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	if q.EditionType == "" {
		q.EditionType = "past"
	}
}

func (q *PastEditionsQuery) Validate() error {
	q.SetDefaultValues()
	return validation.ValidateStruct(q,
		validation.Field(&q.Limit, validation.Min(1), validation.Max(PastEditionsLimitMax).Error("Limit must be at most 100")),
		validation.Field(&q.Offset, validation.Min(0).Error("Offset must be non-negative")),
		validation.Field(&q.EditionType, validation.By(func(value interface{}) error {
			parsed, err := ParseEditionType(q.EditionType, "past")
			if err != nil {
				return err
			}
			q.ParsedEditionType = parsed
			return nil
		})),
	)
}
