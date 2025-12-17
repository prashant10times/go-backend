package models

import (
	"fmt"
	"strconv"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type SearchCategoryDto struct {
	ID            string `json:"id,omitempty" form:"id"`
	Slugs         string `json:"slugs,omitempty" form:"slugs"`
	Name          string `json:"name,omitempty" form:"name"`
	IsGroup       string `json:"isGroup,omitempty" form:"isGroup"`
	IsDesignation string `json:"isDesignation,omitempty" form:"isDesignation"`
	Take          string `json:"take,omitempty" form:"take"`
	Skip          string `json:"skip,omitempty" form:"skip"`
	ID10x         string `json:"id_10x,omitempty" form:"id_10x"`

	ParsedID            []string `json:"-"`
	ParsedSlugs         []string `json:"-"`
	ParsedIsGroup       *bool    `json:"-"`
	ParsedIsDesignation *bool    `json:"-"`
	ParsedTake          int      `json:"-"`
	ParsedSkip          int      `json:"-"`
	ParsedID10x         []int    `json:"-"`
}

func (s *SearchCategoryDto) Validate() error {
	// Set defaults
	if s.Take == "" {
		s.Take = "15"
	}
	if s.Skip == "" {
		s.Skip = "0"
	}

	return validation.ValidateStruct(s,
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

		validation.Field(&s.IsGroup, validation.When(s.IsGroup != "", validation.By(func(value interface{}) error {
			isGroupStr := value.(string)
			if isGroupStr != "true" && isGroupStr != "false" {
				return validation.NewError("invalid_isGroup", "Invalid isGroup value: "+isGroupStr+". Must be 'true' or 'false'")
			}
			parsedValue := isGroupStr == "true"
			s.ParsedIsGroup = &parsedValue
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

		validation.Field(&s.ID10x, validation.When(s.ID10x != "", validation.By(func(value interface{}) error {
			id10xStr := value.(string)
			ids := strings.Split(id10xStr, ",")
			s.ParsedID10x = make([]int, 0, len(ids))
			for _, idStr := range ids {
				idStr = strings.TrimSpace(idStr)
				if idStr != "" {
					id, err := strconv.Atoi(idStr)
					if err != nil {
						return validation.NewError("invalid_id_10x", fmt.Sprintf("Invalid id_10x value: %s. Must be a number", idStr))
					}
					s.ParsedID10x = append(s.ParsedID10x, id)
				}
			}
			return nil
		}))),
	)
}
