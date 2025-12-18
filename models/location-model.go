package models

import (
	"fmt"
	"strconv"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type LocationType string

const (
	LocationTypeCity    LocationType = "CITY"
	LocationTypeState   LocationType = "STATE"
	LocationTypeCountry LocationType = "COUNTRY"
	LocationTypeVenue   LocationType = "VENUE"
)

type LocationQueryDto struct {
	Query        string `json:"query,omitempty" form:"query"`
	LocationIds  string `json:"locationIds,omitempty" form:"locationIds"`
	LocationType string `json:"locationType,omitempty" form:"locationType"`
	Take         string `json:"take,omitempty" form:"take"`
	Offset       string `json:"offset,omitempty" form:"offset"`
	ID10x        string `json:"id_10x,omitempty" form:"id_10x"`
	Slug         string `json:"slug,omitempty" form:"slug"`

	ParsedQuery        *string       `json:"-"`
	ParsedLocationIds  []string      `json:"-"`
	ParsedLocationType *LocationType `json:"-"`
	ParsedTake         int           `json:"-"`
	ParsedOffset       int           `json:"-"`
}

func escapeSqlLikePattern(s string) string {
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

func (l *LocationQueryDto) Validate() error {
	// Set defaults
	if l.Take == "" {
		l.Take = "10"
	}
	if l.Offset == "" {
		l.Offset = "0"
	}

	return validation.ValidateStruct(l,
		validation.Field(&l.Query, validation.When(l.Query != "", validation.By(func(value interface{}) error {
			queryStr := value.(string)
			escaped := escapeSqlLikePattern(queryStr)
			l.ParsedQuery = &escaped
			return nil
		}))),

		validation.Field(&l.LocationIds, validation.When(l.LocationIds != "", validation.By(func(value interface{}) error {
			locationIdsStr := value.(string)
			ids := strings.Split(locationIdsStr, ",")
			l.ParsedLocationIds = make([]string, 0, len(ids))
			for _, id := range ids {
				id = strings.TrimSpace(id)
				if id != "" {
					l.ParsedLocationIds = append(l.ParsedLocationIds, id)
				}
			}
			return nil
		}))),

		validation.Field(&l.LocationType, validation.When(l.LocationType != "", validation.By(func(value interface{}) error {
			locationTypeStr := value.(string)
			locationType := LocationType(locationTypeStr)
			switch locationType {
			case LocationTypeCity, LocationTypeState, LocationTypeCountry, LocationTypeVenue:
				l.ParsedLocationType = &locationType
				return nil
			default:
				return validation.NewError("invalid_locationType", fmt.Sprintf("Invalid locationType value: %s. Must be one of: CITY, STATE, COUNTRY, VENUE", locationTypeStr))
			}
		}))),

		validation.Field(&l.Take, validation.By(func(value interface{}) error {
			takeStr := value.(string)
			if takeStr == "" {
				takeStr = "10"
			}
			take, err := strconv.Atoi(takeStr)
			if err != nil {
				return validation.NewError("invalid_take", "Invalid take value: "+takeStr+". Must be a number")
			}
			if take < 0 {
				return validation.NewError("invalid_take", "Take must be a positive number")
			}
			l.ParsedTake = take
			return nil
		})),

		validation.Field(&l.Offset, validation.By(func(value interface{}) error {
			offsetStr := value.(string)
			if offsetStr == "" {
				offsetStr = "0"
			}
			offset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return validation.NewError("invalid_offset", "Invalid offset value: "+offsetStr+". Must be a number")
			}
			if offset < 0 {
				return validation.NewError("invalid_offset", "Offset must be a positive number")
			}
			l.ParsedOffset = offset
			return nil
		})),
	)
}
