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
	// CityUUIDs   string `json:"cityUUIDs,omitempty" form:"cityUUIDs"`
	// CountryUUIDs string `json:"countryUUIDs,omitempty" form:"countryUUIDs"`
	LocationIDs    string `json:"locationIDs,omitempty" form:"locationIDs"`
	EventUUIDs     string `json:"eventUUIDs,omitempty" form:"eventUUIDs"`
	CategoryUUIDs  string `json:"categoryUUIDs,omitempty" form:"categoryUUIDs"`
	DesignationIds string `json:"designationIds,omitempty" form:"designationIds"`
	RoleIds        string `json:"roleIds,omitempty" form:"roleIds"`
	DepartmentIds  string `json:"departmentIds,omitempty" form:"departmentIds"`
	PageUrls       string `json:"pageUrls,omitempty" form:"pageUrls"`

	ParsedCityIds        []string `json:"-"`
	ParsedCountryIds     []string `json:"-"`
	ParsedEventIds       []string `json:"-"`
	ParsedCategoryIds    []string `json:"-"`
	ParsedLocationIDs    []string `json:"-"`
	ParsedEventUUIDs     []string `json:"-"`
	ParsedCategoryUUIDs  []string `json:"-"`
	ParsedDesignationIds []string `json:"-"`
	ParsedRoleIds        []string `json:"-"`
	ParsedDepartmentIds  []string `json:"-"`
	ParsedPageUrls       []string `json:"-"`
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
		validation.Field(&c.LocationIDs, validation.When(c.LocationIDs != "", validation.By(parseCSV(&c.ParsedLocationIDs)))),
		validation.Field(&c.EventUUIDs, validation.When(c.EventUUIDs != "", validation.By(parseCSV(&c.ParsedEventUUIDs)))),
		validation.Field(&c.CategoryUUIDs, validation.When(c.CategoryUUIDs != "", validation.By(parseCSV(&c.ParsedCategoryUUIDs)))),
		validation.Field(&c.DesignationIds, validation.When(c.DesignationIds != "", validation.By(parseCSV(&c.ParsedDesignationIds)))),
		validation.Field(&c.RoleIds, validation.When(c.RoleIds != "", validation.By(parseCSV(&c.ParsedRoleIds)))),
		validation.Field(&c.DepartmentIds, validation.When(c.DepartmentIds != "", validation.By(parseCSV(&c.ParsedDepartmentIds)))),
		validation.Field(&c.PageUrls, validation.When(c.PageUrls != "", validation.By(parseCSV(&c.ParsedPageUrls)))),
	)
}
