package models

import (
	"fmt"
	"strconv"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type SearchEventTypeDto struct {
	IDs            string `json:"ids,omitempty" form:"ids"`
	Slugs          string `json:"slugs,omitempty" form:"slugs"`
	Query          string `json:"query,omitempty" form:"query"`
	Take           string `json:"take,omitempty" form:"take"`
	Skip           string `json:"skip,omitempty" form:"skip"`
	GroupBy        string `json:"groupBy,omitempty" form:"groupBy"`
	EventTypeGroup string `json:"eventTypeGroup,omitempty" form:"eventTypeGroup"`

	ParsedIDs            []string `json:"-"`
	ParsedSlugs          []string `json:"-"`
	ParsedTake           int      `json:"-"`
	ParsedSkip           int      `json:"-"`
	ParsedGroupBy        *string  `json:"-"`
	ParsedEventTypeGroup *string  `json:"-"`
}

func (s *SearchEventTypeDto) Validate() error {
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

		validation.Field(&s.GroupBy, validation.When(s.GroupBy != "", validation.By(func(value interface{}) error {
			groupByStr := value.(string)
			if groupByStr != "typeGroup" {
				return validation.NewError("invalid_groupBy", "Invalid groupBy value: "+groupByStr+". Must be 'typeGroup'")
			}
			s.ParsedGroupBy = &groupByStr
			return nil
		}))),

		validation.Field(&s.EventTypeGroup, validation.When(s.EventTypeGroup != "", validation.By(func(value interface{}) error {
			eventTypeGroupStr := strings.ToLower(value.(string))
			validGroups := []string{"business", "social", "unattended"}
			isValid := false
			for _, group := range validGroups {
				if eventTypeGroupStr == group {
					isValid = true
					break
				}
			}
			if !isValid {
				return validation.NewError("invalid_eventTypeGroup", fmt.Sprintf("Invalid eventTypeGroup value: %s. Must be one of: %s", value.(string), strings.Join(validGroups, ", ")))
			}
			s.ParsedEventTypeGroup = &eventTypeGroupStr
			return nil
		}))),
	)
}
