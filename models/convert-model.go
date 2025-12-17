package models

import (
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type ConvertSchemaDto struct {
	CityIds     string `json:"cityIds,omitempty" form:"cityIds"`
	CountryIds  string `json:"countryIds,omitempty" form:"countryIds"`
	EventIds    string `json:"eventIds,omitempty" form:"eventIds"`
	CategoryIds string `json:"categoryIds,omitempty" form:"categoryIds"`

	ParsedCityIds     []string `json:"-"`
	ParsedCountryIds  []string `json:"-"`
	ParsedEventIds    []string `json:"-"`
	ParsedCategoryIds []string `json:"-"`
}

func (c *ConvertSchemaDto) Validate() error {
	parseCSV := func(target *[]string) func(interface{}) error {
		return func(value interface{}) error {
			valStr := value.(string)
			parts := strings.Split(valStr, ",")
			*target = make([]string, 0, len(parts))
			for _, part := range parts {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					*target = append(*target, trimmed)
				}
			}
			return nil
		}
	}

	return validation.ValidateStruct(c,
		validation.Field(&c.CityIds, validation.When(c.CityIds != "", validation.By(parseCSV(&c.ParsedCityIds)))),
		validation.Field(&c.CountryIds, validation.When(c.CountryIds != "", validation.By(parseCSV(&c.ParsedCountryIds)))),
		validation.Field(&c.EventIds, validation.When(c.EventIds != "", validation.By(parseCSV(&c.ParsedEventIds)))),
		validation.Field(&c.CategoryIds, validation.When(c.CategoryIds != "", validation.By(parseCSV(&c.ParsedCategoryIds)))),
	)
}
