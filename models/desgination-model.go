package models

import (
	"strconv"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type SearchDesignationDto struct {
	IDs           string `json:"ids,omitempty" form:"ids"`
	ID            string `json:"id,omitempty" form:"id"`
	Slugs         string `json:"slugs,omitempty" form:"slugs"`
	Name          string `json:"name,omitempty" form:"name"`
	IsDesignation string `json:"isDesignation,omitempty" form:"isDesignation"`
	IsDepartment  string `json:"isDepartment,omitempty" form:"isDepartment"`
	IsRole        string `json:"isRole,omitempty" form:"isRole"`
	Take          string `json:"take,omitempty" form:"take"`
	Skip          string `json:"skip,omitempty" form:"skip"`

	ParsedIDs           []string `json:"-"`
	ParsedID            []string `json:"-"`
	ParsedSlugs         []string `json:"-"`
	ParsedIsDesignation *bool    `json:"-"`
	ParsedIsDepartment  *bool    `json:"-"`
	ParsedIsRole        *bool    `json:"-"`
	ParsedTake          int      `json:"-"`
	ParsedSkip          int      `json:"-"`
}

func (s *SearchDesignationDto) Validate() error {
	// Set defaults
	if s.Take == "" {
		s.Take = "15"
	}
	if s.Skip == "" {
		s.Skip = "0"
	}

	return validation.ValidateStruct(s,
		validation.Field(&s.IDs, validation.When(s.IDs != "", validation.By(func(value interface{}) error {
			idsStr := value.(string)
			ids := strings.Split(idsStr, ",")
			s.ParsedIDs = make([]string, 0, len(ids))
			for _, id := range ids {
				id = strings.TrimSpace(id)
				if id != "" {
					s.ParsedIDs = append(s.ParsedIDs, id)
				}
			}
			return nil
		}))),

		validation.Field(&s.ID, validation.When(s.ID != "", validation.By(func(value interface{}) error {
			idStr := value.(string)
			ids := strings.Split(idStr, ",")
			s.ParsedID = make([]string, 0, len(ids))
			for _, id := range ids {
				id = strings.TrimSpace(id)
				if id != "" {
					s.ParsedID = append(s.ParsedID, id)
				}
			}
			return nil
		}))),

		validation.Field(&s.Slugs, validation.When(s.Slugs != "", validation.By(func(value interface{}) error {
			slugsStr := value.(string)
			slugs := strings.Split(slugsStr, ",")
			s.ParsedSlugs = make([]string, 0, len(slugs))
			for _, slug := range slugs {
				slug = strings.TrimSpace(slug)
				if slug != "" {
					s.ParsedSlugs = append(s.ParsedSlugs, slug)
				}
			}
			return nil
		}))),

		validation.Field(&s.IsDesignation, validation.When(s.IsDesignation != "", validation.By(func(value interface{}) error {
			isDesignationStr := value.(string)
			if isDesignationStr != "true" && isDesignationStr != "false" {
				return validation.NewError("invalid_isDesignation", "Invalid isDesignation value: "+isDesignationStr+". Must be 'true' or 'false'")
			}
			parsedValue := isDesignationStr == "true"
			s.ParsedIsDesignation = &parsedValue
			return nil
		}))),

		validation.Field(&s.IsDepartment, validation.When(s.IsDepartment != "", validation.By(func(value interface{}) error {
			isDepartmentStr := value.(string)
			if isDepartmentStr != "true" && isDepartmentStr != "false" {
				return validation.NewError("invalid_isDepartment", "Invalid isDepartment value: "+isDepartmentStr+". Must be 'true' or 'false'")
			}
			parsedValue := isDepartmentStr == "true"
			s.ParsedIsDepartment = &parsedValue
			return nil
		}))),

		validation.Field(&s.IsRole, validation.When(s.IsRole != "", validation.By(func(value interface{}) error {
			isRoleStr := value.(string)
			if isRoleStr != "true" && isRoleStr != "false" {
				return validation.NewError("invalid_isRole", "Invalid isRole value: "+isRoleStr+". Must be 'true' or 'false'")
			}
			parsedValue := isRoleStr == "true"
			s.ParsedIsRole = &parsedValue
			return nil
		}))),

		validation.Field(&s.Take, validation.By(func(value interface{}) error {
			takeStr := value.(string)
			if takeStr == "" {
				takeStr = "15"
			}
			take, err := strconv.Atoi(takeStr)
			if err != nil {
				return validation.NewError("invalid_take", "Invalid take value: "+takeStr+". Must be a number")
			}
			if take < 0 {
				return validation.NewError("invalid_take", "Take must be a positive number")
			}
			s.ParsedTake = take
			return nil
		})),

		validation.Field(&s.Skip, validation.By(func(value interface{}) error {
			skipStr := value.(string)
			if skipStr == "" {
				skipStr = "0"
			}
			skip, err := strconv.Atoi(skipStr)
			if err != nil {
				return validation.NewError("invalid_skip", "Invalid skip value: "+skipStr+". Must be a number")
			}
			if skip < 0 {
				return validation.NewError("invalid_skip", "Skip must be a positive number")
			}
			s.ParsedSkip = skip
			return nil
		})),
	)
}
