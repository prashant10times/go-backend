package models

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type ResponseDataDto struct {
	EventCreated    *string `json:"event_created,omitempty"`
	EventExhibitors *string `json:"event_exhibitors,omitempty"`
	EventPublished  *string `json:"event_published,omitempty"`
	MyJoinField     *string `json:"my_join_field,omitempty"`
	EventVenueID    *string `json:"event_venueId,omitempty"`
	EventScore      *string `json:"event_score,omitempty"`
}

type Keywords struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
}

type DateRange [2]*string

type Groups string

const (
	GroupSocial     Groups = "social"
	GroupBusiness   Groups = "business"
	GroupUnattended Groups = "unattended"
)

type View string

const (
	ViewList     View = "list"
	ViewAgg      View = "agg"
	ViewMap      View = "map"
	ViewDetail   View = "detail"
	ViewCalendar View = "calendar"
	ViewTracker  View = "tracker"
	ViewPromote  View = "promote"
)

type BoundType string

const (
	BoundTypePoint BoundType = "point"
	BoundTypeBox   BoundType = "box"
)

type GeoCoordinates struct {
	Latitude  float64  `json:"latitude"`
	Longitude float64  `json:"longitude"`
	Radius    *float64 `json:"radius,omitempty"`
}

type ViewBound struct {
	BoundType   BoundType       `json:"boundType"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type AlertSearchGroupBy string

const (
	AlertSearchGroupByDay       AlertSearchGroupBy = "day"
	AlertSearchGroupByAlertType AlertSearchGroupBy = "alertType"
)

type AlertSearchSortBy string

const (
	AlertSearchSortBySeverity     AlertSearchSortBy = "severity"
	AlertSearchSortBySeverityDesc AlertSearchSortBy = "-severity"
	AlertSearchSortByStart        AlertSearchSortBy = "start"
	AlertSearchSortByStartDesc    AlertSearchSortBy = "-start"
)

type AlertSearchParams struct {
	SortBy      []AlertSearchSortBy  `json:"sortBy,omitempty"`
	LocationIds []string             `json:"locationIds,omitempty"`
	Coordinates *GeoCoordinates      `json:"coordinates,omitempty"`
	EventIds    []string             `json:"eventIds,omitempty"`
	Required    string               `json:"required"`
	Limit       *int                 `json:"limit,omitempty"`
	Offset      *int                 `json:"offset,omitempty"`
	StartDate   *string              `json:"startDate,omitempty"`
	EndDate     *string              `json:"endDate,omitempty"`
	GroupBy     []AlertSearchGroupBy `json:"groupBy,omitempty"`
}

type FilterDataDto struct {
	Q              string `json:"q,omitempty" form:"q"`
	EventIds       string `json:"eventIds,omitempty" form:"eventIds"`
	NotEventIds    string `json:"notEventIds,omitempty" form:"notEventIds"`
	SourceEventIds string `json:"sourceEventIds,omitempty" form:"sourceEventIds"`
	Keywords       string `json:"keywords,omitempty" form:"keywords"`
	Category       string `json:"category,omitempty" form:"category"`
	City           string `json:"city,omitempty" form:"city"`
	State          string `json:"state,omitempty" form:"state"`
	Country        string `json:"country,omitempty" form:"country"`
	Products       string `json:"products,omitempty" form:"products"`

	Price     string `json:"price,omitempty" form:"price"`
	AvgRating string `json:"avgRating,omitempty" form:"avgRating"`

	FollowingGte string `json:"following.gte,omitempty" form:"following.gte"`
	FollowingLte string `json:"following.lte,omitempty" form:"following.lte"`
	FollowingGt  string `json:"following.gt,omitempty" form:"following.gt"`
	FollowingLt  string `json:"following.lt,omitempty" form:"following.lt"`

	VisitorDesignation string `json:"visitorDesignation,omitempty" form:"visitorDesignation"`
	VisitorCountry     string `json:"visitorCountry,omitempty" form:"visitorCountry"`
	VisitorCompany     string `json:"visitorCompany,omitempty" form:"visitorCompany"`
	VisitorCity        string `json:"visitorCity,omitempty" form:"visitorCity"`
	VisitorState       string `json:"visitorState,omitempty" form:"visitorState"`
	VisitorName        string `json:"visitorName,omitempty" form:"visitorName"`

	JobComposite   string `json:"jobComposite,omitempty" form:"jobComposite"`
	DesignationIds string `json:"designationIds,omitempty" form:"designationIds"`
	SeniorityIds   string `json:"seniorityIds,omitempty" form:"seniorityIds"`

	SpeakerDesignation string `json:"speakerDesignation,omitempty" form:"speakerDesignation"`
	SpeakerCity        string `json:"speakerCity,omitempty" form:"speakerCity"`
	SpeakerState       string `json:"speakerState,omitempty" form:"speakerState"`
	SpeakerCountry     string `json:"speakerCountry,omitempty" form:"speakerCountry"`
	SpeakerCompany     string `json:"speakerCompany,omitempty" form:"speakerCompany"`
	SpeakerName        string `json:"speakerName,omitempty" form:"speakerName"`

	ExhibitorName     string `json:"exhibitorName,omitempty" form:"exhibitorName"`
	ExhibitorWebsite  string `json:"exhibitorWebsite,omitempty" form:"exhibitorWebsite"`
	ExhibitorDomain   string `json:"exhibitorDomain,omitempty" form:"exhibitorDomain"`
	ExhibitorState    string `json:"exhibitorState,omitempty" form:"exhibitorState"`
	ExhibitorCountry  string `json:"exhibitorCountry,omitempty" form:"exhibitorCountry"`
	ExhibitorCity     string `json:"exhibitorCity,omitempty" form:"exhibitorCity"`
	ExhibitorFacebook string `json:"exhibitorFacebook,omitempty" form:"exhibitorFacebook"`
	ExhibitorTwitter  string `json:"exhibitorTwitter,omitempty" form:"exhibitorTwitter"`
	ExhibitorLinkedin string `json:"exhibitorLinkedin,omitempty" form:"exhibitorLinkedin"`

	SponsorName     string `json:"sponsorName,omitempty" form:"sponsorName"`
	SponsorWebsite  string `json:"sponsorWebsite,omitempty" form:"sponsorWebsite"`
	SponsorDomain   string `json:"sponsorDomain,omitempty" form:"sponsorDomain"`
	SponsorState    string `json:"sponsorState,omitempty" form:"sponsorState"`
	SponsorCountry  string `json:"sponsorCountry,omitempty" form:"sponsorCountry"`
	SponsorCity     string `json:"sponsorCity,omitempty" form:"sponsorCity"`
	SponsorFacebook string `json:"sponsorFacebook,omitempty" form:"sponsorFacebook"`
	SponsorTwitter  string `json:"sponsorTwitter,omitempty" form:"sponsorTwitter"`
	SponsorLinkedin string `json:"sponsorLinkedin,omitempty" form:"sponsorLinkedin"`

	EndGte    string `json:"end.gte,omitempty" form:"end.gte"`
	EndLte    string `json:"end.lte,omitempty" form:"end.lte"`
	StartGte  string `json:"start.gte,omitempty" form:"start.gte"`
	StartLte  string `json:"start.lte,omitempty" form:"start.lte"`
	StartGt   string `json:"start.gt,omitempty" form:"start.gt"`
	EndGt     string `json:"end.gt,omitempty" form:"end.gt"`
	StartLt   string `json:"start.lt,omitempty" form:"start.lt"`
	EndLt     string `json:"end.lt,omitempty" form:"end.lt"`
	CreatedAt string `json:"createdAt,omitempty" form:"createdAt"`
	Dates     string `json:"dates,omitempty" form:"dates"`

	ActiveGte string `json:"active.gte,omitempty" form:"active.gte"`
	ActiveLte string `json:"active.lte,omitempty" form:"active.lte"`
	ActiveGt  string `json:"active.gt,omitempty" form:"active.gt"`
	ActiveLt  string `json:"active.lt,omitempty" form:"active.lt"`

	Type       string `json:"type,omitempty" form:"type"`
	Venue      string `json:"venue,omitempty" form:"venue"`
	SpeakerGte string `json:"speaker.gte,omitempty" form:"speaker.gte"`
	SpeakerLte string `json:"speaker.lte,omitempty" form:"speaker.lte"`
	SpeakerGt  string `json:"speaker.gt,omitempty" form:"speaker.gt"`
	SpeakerLt  string `json:"speaker.lt,omitempty" form:"speaker.lt"`

	ExhibitorsGte string `json:"exhibitors.gte,omitempty" form:"exhibitors.gte"`
	ExhibitorsLte string `json:"exhibitors.lte,omitempty" form:"exhibitors.lte"`
	ExhibitorsGt  string `json:"exhibitors.gt,omitempty" form:"exhibitors.gt"`
	ExhibitorsLt  string `json:"exhibitors.lt,omitempty" form:"exhibitors.lt"`

	EditionsGte string `json:"editions.gte,omitempty" form:"editions.gte"`
	EditionsLte string `json:"editions.lte,omitempty" form:"editions.lte"`
	EditionsGt  string `json:"editions.gt,omitempty" form:"editions.gt"`
	EditionsLt  string `json:"editions.lt,omitempty" form:"editions.lt"`

	Lat                string `json:"lat,omitempty" form:"lat"`
	Lon                string `json:"lon,omitempty" form:"lon"`
	Radius             string `json:"radius,omitempty" form:"radius"`
	Unit               string `json:"unit,omitempty" form:"unit"`
	EventDistanceOrder string `json:"eventDistanceOrder,omitempty" form:"eventDistanceOrder"`

	Company        string `json:"company,omitempty" form:"company"`
	CompanyCountry string `json:"companyCountry,omitempty" form:"companyCountry"`
	CompanyDomain  string `json:"companyDomain,omitempty" form:"companyDomain"`
	CompanyCity    string `json:"companyCity,omitempty" form:"companyCity"`
	CompanyState   string `json:"companyState,omitempty" form:"companyState"`

	View                string `json:"view,omitempty" form:"view"`
	Frequency           string `json:"frequency,omitempty" form:"frequency"`
	Visibility          string `json:"visibility,omitempty" form:"visibility"`
	Mode                string `json:"mode,omitempty" form:"mode"`
	EstimatedVisitors   string `json:"estimatedVisitors,omitempty" form:"estimatedVisitors"`
	EstimatedExhibitors string `json:"estimatedExhibitors,omitempty" form:"estimatedExhibitors"`
	IsBranded           string `json:"isBranded,omitempty" form:"isBranded"`
	Maturity            string `json:"maturity,omitempty" form:"maturity"`
	Status              string `json:"status,omitempty" form:"status"`
	Published           string `json:"published,omitempty" form:"published"`
	EventTypeGroup      string `json:"eventTypeGroup,omitempty" form:"eventTypeGroup"`
	ViewBound           string `json:"viewBound,omitempty" form:"viewBound"`

	VenueLatitude  string `json:"venueLatitude,omitempty" form:"venueLatitude"`
	VenueLongitude string `json:"venueLongitude,omitempty" form:"venueLongitude"`

	ToAggregate string `json:"toAggregate,omitempty" form:"toAggregate"`

	EventRanking   string `json:"eventRanking,omitempty" form:"eventRanking"`
	AudienceZone   string `json:"audienceZone,omitempty" form:"audienceZone"`
	EventEstimate  bool   `json:"eventEstimate,omitempty" form:"eventEstimate"`
	AudienceSpread string `json:"audienceSpread,omitempty" form:"audienceSpread"`

	InboundScoreGte       string `json:"inboundScore.gte,omitempty" form:"inboundScore.gte"`
	InboundScoreLte       string `json:"inboundScore.lte,omitempty" form:"inboundScore.lte"`
	InternationalScoreGte string `json:"internationalScore.gte,omitempty" form:"internationalScore.gte"`
	InternationalScoreLte string `json:"internationalScore.lte,omitempty" form:"internationalScore.lte"`
	TrustScoreLte         string `json:"trustScore.lte,omitempty" form:"trustScore.lte"`
	TrustScoreGte         string `json:"trustScore.gte,omitempty" form:"trustScore.gte"`
	ImpactScoreLte        string `json:"impactScore.lte,omitempty" form:"impactScore.lte"`
	ImpactScoreGte        string `json:"impactScore.gte,omitempty" form:"impactScore.gte"`
	EconomicImpactGte     string `json:"economicImpact.gte,omitempty" form:"economicImpact.gte"`
	EconomicImpactLte     string `json:"economicImpact.lte,omitempty" form:"economicImpact.lte"`

	ParsedCategory       []string    `json:"-"`
	ParsedCity           []string    `json:"-"`
	ParsedCountry        []string    `json:"-"`
	ParsedProducts       []string    `json:"-"`
	ParsedType           []string    `json:"-"`
	ParsedVenue          []string    `json:"-"`
	ParsedCompany        []string    `json:"-"`
	ParsedView           []string    `json:"-"`
	ParsedToAggregate    []string    `json:"-"`
	ParsedKeywords       *Keywords   `json:"-"`
	ParsedIsBranded      *bool       `json:"-"`
	ParsedMode           *string     `json:"-"`
	ParsedStatus         []string    `json:"-"`
	ParsedState          []string    `json:"-"`
	ParsedCompanyState   []string    `json:"-"`
	ParsedCompanyCity    []string    `json:"-"`
	ParsedCompanyDomain  []string    `json:"-"`
	ParsedCompanyCountry []string    `json:"-"`
	ParsedSpeakerState   []string    `json:"-"`
	ParsedExhibitorState []string    `json:"-"`
	ParsedSponsorState   []string    `json:"-"`
	ParsedVisitorState   []string    `json:"-"`
	ParsedJobComposite   []string    `json:"-"`
	ParsedEventRanking   []string    `json:"-"`
	ParsedAudienceZone   []string    `json:"-"`
	ParsedAudienceSpread []string    `json:"-"`
	ParsedPublished      []string    `json:"-"`
	ParsedEventTypeGroup *Groups     `json:"-"`
	ParsedDesignationId  []string    `json:"-"`
	ParsedSeniorityId    []string    `json:"-"`
	ParsedViewBound      *ViewBound  `json:"-"`
	ParsedEventIds       []string    `json:"-"`
	ParsedNotEventIds    []string    `json:"-"`
	ParsedSourceEventIds []string    `json:"-"`
	ParsedDates          []DateRange `json:"-"`
}

func (f *FilterDataDto) SetDefaultValues() {
	if f.Radius == "" {
		f.Radius = "5"
	}
	if f.Unit == "" {
		f.Unit = "km"
	}
	if f.EventDistanceOrder == "" {
		f.EventDistanceOrder = "closest"
	}
	if f.Published == "" {
		f.Published = "1"
	}
}

func validateAndNormalizeDate(dateStr *string, fieldName string) validation.Rule {
	return validation.When(*dateStr != "", validation.By(func(value interface{}) error {
		dateValue := value.(string)
		if dateValue == "" {
			return nil
		}

		var parsedDate time.Time
		var err error

		parsedDate, err = time.Parse("2006-01-02", dateValue)
		if err != nil {
			parsedDate, err = time.Parse(time.RFC3339, dateValue)
			if err != nil {
				parsedDate, err = time.Parse("2006-01-02T15:04:05", dateValue)
				if err != nil {
					return validation.NewError("invalid_date", "Invalid date format for "+fieldName+". Expected YYYY-MM-DD or ISO8601/RFC3339 format (e.g., 2025-11-14 or 2025-11-14T10:30:00Z)")
				}
			}
		}

		normalizedDate := parsedDate.Format("2006-01-02")
		*dateStr = normalizedDate
		return nil
	}))
}

func (f *FilterDataDto) Validate() error {
	f.SetDefaultValues()

	return validation.ValidateStruct(f,
		validation.Field(&f.Price, validation.When(f.Price != "", validation.In("free", "paid", "not_available", "free-paid"))),    // Price validation
		validation.Field(&f.AvgRating, validation.When(f.AvgRating != "", validation.In("1", "2", "3", "4", "5"))),                 // AvgRating validation
		validation.Field(&f.Unit, validation.When(f.Unit != "", validation.In("km", "mi", "ft"))),                                  // Unit validation
		validation.Field(&f.EventDistanceOrder, validation.When(f.EventDistanceOrder != "", validation.In("closest", "farthest"))), // EventDistanceOrder validation

		validation.Field(&f.EventIds, validation.When(f.EventIds != "", validation.By(func(value interface{}) error {
			eventIdsStr := value.(string)
			eventIds := strings.Split(eventIdsStr, ",")
			f.ParsedEventIds = make([]string, 0, len(eventIds))
			for _, eventId := range eventIds {
				eventId = strings.TrimSpace(eventId)
				if eventId != "" {
					quotedIds := fmt.Sprintf("'%s'", eventId)
					f.ParsedEventIds = append(f.ParsedEventIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.NotEventIds, validation.When(f.NotEventIds != "", validation.By(func(value interface{}) error {
			notEventIdsStr := value.(string)
			notEventIds := strings.Split(notEventIdsStr, ",")
			f.ParsedNotEventIds = make([]string, 0, len(notEventIds))
			for _, notEventId := range notEventIds {
				notEventId = strings.TrimSpace(notEventId)
				if notEventId != "" {
					quotedIds := fmt.Sprintf("'%s'", notEventId)
					f.ParsedNotEventIds = append(f.ParsedNotEventIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.SourceEventIds, validation.When(f.SourceEventIds != "", validation.By(func(value interface{}) error {
			sourceEventIdsStr := value.(string)
			sourceEventIds := strings.Split(sourceEventIdsStr, ",")
			f.ParsedSourceEventIds = make([]string, 0, len(sourceEventIds))
			for _, sourceEventId := range sourceEventIds {
				sourceEventId = strings.TrimSpace(sourceEventId)
				if sourceEventId != "" {
					quotedIds := fmt.Sprintf("'%s'", sourceEventId)
					f.ParsedSourceEventIds = append(f.ParsedSourceEventIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.View, validation.By(func(value interface{}) error {
			viewStr := value.(string)
			viewLower := strings.ToLower(strings.TrimSpace(viewStr))

			validViews := map[string]View{
				"list":     ViewList,
				"agg":      ViewAgg,
				"map":      ViewMap,
				"detail":   ViewDetail,
				"calendar": ViewCalendar,
				"tracker":  ViewTracker,
				"promote":  ViewPromote,
			}

			if view, exists := validViews[viewLower]; exists {
				f.ParsedView = []string{string(view)}
				f.View = viewLower
			} else {
				validOptions := []string{"list", "map", "detail", "calendar", "tracker", "promote"}
				return validation.NewError("invalid_view", "Invalid view value: "+viewStr+". Valid options are: "+strings.Join(validOptions, ", "))
			}
			return nil
		})),

		validation.Field(&f.Frequency, validation.When(f.Frequency != "", validation.In("Weekly", "Monthly", "Quarterly", "Bi-annual", "Annual", "Biennial", "Triennial", "Quinquennial", "One-time", "Quadrennial"))), // Frequency validation

		validation.Field(&f.Visibility, validation.When(f.Visibility != "", validation.In("open", "private", "draft"))), // Visibility validation

		validation.Field(&f.Mode, validation.When(f.Mode != "", validation.By(func(value interface{}) error {
			modeStr := value.(string)
			modes := strings.Split(modeStr, ",")
			if len(modes) > 0 {
				mode := strings.TrimSpace(modes[0])
				if mode != "online" && mode != "physical" && mode != "hybrid" {
					return validation.NewError("invalid_mode", "Invalid mode value: "+mode+". Must be 'online', 'physical', or 'hybrid'")
				}
				f.ParsedMode = &mode
			}
			return nil
		}))),

		validation.Field(&f.AudienceSpread, validation.When(f.AudienceSpread != "", validation.By(func(value interface{}) error {
			audienceSpreadStr := value.(string)
			audienceSpreads := strings.Split(audienceSpreadStr, ",")
			f.ParsedAudienceSpread = make([]string, 0, len(audienceSpreads))
			for _, audienceSpread := range audienceSpreads {
				audienceSpread = strings.TrimSpace(audienceSpread)
				if audienceSpread != "" {
					f.ParsedAudienceSpread = append(f.ParsedAudienceSpread, audienceSpread)
				}
			}
			return nil
		}))),

		validation.Field(&f.EstimatedVisitors, validation.When(f.EstimatedVisitors != "", validation.In("Nano", "Micro", "Small", "Medium", "Large", "Mega", "Ultra"))), // EstimatedVisitors validation

		validation.Field(&f.EstimatedExhibitors, validation.When(f.EstimatedExhibitors != "", validation.In("0-100", "100-500", "500-1000", "1000"))),

		validation.Field(&f.Maturity, validation.When(f.Maturity != "", validation.In("new", "growing", "established", "flagship"))), // Maturity validation

		validation.Field(&f.Status, validation.When(f.Status != "", validation.By(func(value interface{}) error {
			statusStr := value.(string)
			statuses := strings.Split(statusStr, ",")
			f.ParsedStatus = make([]string, 0, len(statuses))
			validStatuses := map[string]string{
				"active":    "A",
				"cancelled": "C",
				"postponed": "P",
			}
			for _, status := range statuses {
				status = strings.TrimSpace(strings.ToLower(status))
				if status != "" {
					if dbCode, exists := validStatuses[status]; exists {
						f.ParsedStatus = append(f.ParsedStatus, dbCode)
					} else {
						return validation.NewError("invalid_status", "Invalid status value: "+status+". Valid values are: active, cancelled, postponed")
					}
				}
			}
			return nil
		}))),

		validation.Field(&f.ToAggregate, validation.When(f.ToAggregate != "", validation.By(func(value interface{}) error {
			aggStr := value.(string)
			aggs := strings.Split(aggStr, ",")
			if len(aggs) > 4 {
				return validation.NewError("too_many_aggregations", "Maximum 4 aggregation fields allowed")
			}
			f.ParsedToAggregate = make([]string, 0, len(aggs))
			validAggs := []string{"country", "city", "month", "date", "category", "tag"}
			for _, agg := range aggs {
				agg = strings.TrimSpace(agg)
				if agg != "" {
					valid := false
					for _, validAgg := range validAggs {
						if agg == validAgg {
							valid = true
							break
						}
					}
					if !valid {
						return validation.NewError("invalid_aggregation", "Invalid aggregation field: "+agg+". Valid fields are: "+strings.Join(validAggs, ", "))
					}
					f.ParsedToAggregate = append(f.ParsedToAggregate, agg)
				}
			}
			return nil
		}))),

		validation.Field(&f.Category, validation.When(f.Category != "", validation.By(func(value interface{}) error {
			categoryStr := value.(string)
			categories := strings.Split(categoryStr, "|:")
			f.ParsedCategory = make([]string, 0, len(categories))
			for _, category := range categories {
				category = strings.TrimSpace(category)
				if category != "" {
					f.ParsedCategory = append(f.ParsedCategory, category)
				}
			}
			return nil
		}))),

		validation.Field(&f.AudienceZone, validation.When(f.AudienceZone != "", validation.By(func(value interface{}) error {
			audienceZoneStr := value.(string)
			audienceZones := strings.Split(audienceZoneStr, ",")
			validAudienceZones := []string{"Local", "Regional", "National", "International"}
			f.ParsedAudienceZone = make([]string, 0, len(audienceZones))
			var invalidAudienceZones []string
			for _, audienceZone := range audienceZones {
				audienceZone = strings.TrimSpace(audienceZone)
				if audienceZone != "" {
					valid := false
					for _, validAudienceZone := range validAudienceZones {
						if audienceZone == validAudienceZone {
							valid = true
							break
						}
					}
					if !valid {
						invalidAudienceZones = append(invalidAudienceZones, audienceZone)
						continue
					}
					f.ParsedAudienceZone = append(f.ParsedAudienceZone, audienceZone)
				}
			}
			if len(invalidAudienceZones) > 0 {
				return validation.NewError("invalid_audience_zone", "Invalid audience zone value: "+strings.Join(invalidAudienceZones, ", ")+". Valid values are: "+strings.Join(validAudienceZones, ", "))
			}
			return nil
		}))),

		validation.Field(&f.DesignationIds, validation.When(f.DesignationIds != "", validation.By(func(value interface{}) error {
			designationIdStr := value.(string)
			designationIds := strings.Split(designationIdStr, ",")
			f.ParsedDesignationId = make([]string, 0, len(designationIds))
			for _, designationId := range designationIds {
				designationId = strings.TrimSpace(designationId)
				if designationId != "" {
					f.ParsedDesignationId = append(f.ParsedDesignationId, designationId)
				}
			}
			return nil
		}))),

		validation.Field(&f.SeniorityIds, validation.When(f.SeniorityIds != "", validation.By(func(value interface{}) error {
			seniorityIdStr := value.(string)
			seniorityIds := strings.Split(seniorityIdStr, ",")
			f.ParsedSeniorityId = make([]string, 0, len(seniorityIds))
			for _, seniorityId := range seniorityIds {
				seniorityId = strings.TrimSpace(seniorityId)
				if seniorityId != "" {
					f.ParsedSeniorityId = append(f.ParsedSeniorityId, seniorityId)
				}
			}
			return nil
		}))),

		validation.Field(&f.State, validation.When(f.State != "", validation.By(func(value interface{}) error {
			stateStr := value.(string)
			states := strings.Split(stateStr, ",")
			f.ParsedState = make([]string, 0, len(states))
			for _, state := range states {
				state = strings.TrimSpace(state)
				if state != "" {
					f.ParsedState = append(f.ParsedState, state)
				}
			}
			return nil
		}))),

		validation.Field(&f.CompanyState, validation.When(f.CompanyState != "", validation.By(func(value interface{}) error {
			companyStateStr := value.(string)
			companyStates := strings.Split(companyStateStr, ",")
			f.ParsedCompanyState = make([]string, 0, len(companyStates))
			for _, companyState := range companyStates {
				companyState = strings.TrimSpace(companyState)
				if companyState != "" {
					f.ParsedCompanyState = append(f.ParsedCompanyState, companyState)
				}
			}
			return nil
		}))),

		validation.Field(&f.CompanyCity, validation.When(f.CompanyCity != "", validation.By(func(value interface{}) error {
			companyCityStr := value.(string)
			companyCities := strings.Split(companyCityStr, ",")
			f.ParsedCompanyCity = make([]string, 0, len(companyCities))
			for _, companyCity := range companyCities {
				companyCity = strings.TrimSpace(companyCity)
				if companyCity != "" {
					f.ParsedCompanyCity = append(f.ParsedCompanyCity, companyCity)
				}
			}
			return nil
		}))),

		validation.Field(&f.CompanyDomain, validation.When(f.CompanyDomain != "", validation.By(func(value interface{}) error {
			companyDomainStr := value.(string)
			companyDomains := strings.Split(companyDomainStr, ",")
			f.ParsedCompanyDomain = make([]string, 0, len(companyDomains))
			for _, companyDomain := range companyDomains {
				companyDomain = strings.TrimSpace(companyDomain)
				if companyDomain != "" {
					f.ParsedCompanyDomain = append(f.ParsedCompanyDomain, companyDomain)
				}
			}
			return nil
		}))),

		validation.Field(&f.CompanyCountry, validation.When(f.CompanyCountry != "", validation.By(func(value interface{}) error {
			companyCountryStr := value.(string)
			companyCountries := strings.Split(companyCountryStr, ",")
			f.ParsedCompanyCountry = make([]string, 0, len(companyCountries))
			for _, companyCountry := range companyCountries {
				companyCountry = strings.TrimSpace(companyCountry)
				if companyCountry != "" {
					f.ParsedCompanyCountry = append(f.ParsedCompanyCountry, companyCountry)
				}
			}
			return nil
		}))),

		validation.Field(&f.VisitorState, validation.When(f.VisitorState != "", validation.By(func(value interface{}) error {
			visitorStateStr := value.(string)
			visitorStates := strings.Split(visitorStateStr, ",")
			f.ParsedVisitorState = make([]string, 0, len(visitorStates))
			for _, visitorState := range visitorStates {
				visitorState = strings.TrimSpace(visitorState)
				if visitorState != "" {
					f.ParsedVisitorState = append(f.ParsedVisitorState, visitorState)
				}
			}
			return nil
		}))),

		validation.Field(&f.SpeakerState, validation.When(f.SpeakerState != "", validation.By(func(value interface{}) error {
			speakerStateStr := value.(string)
			speakerStates := strings.Split(speakerStateStr, ",")
			f.ParsedSpeakerState = make([]string, 0, len(speakerStates))
			for _, speakerState := range speakerStates {
				speakerState = strings.TrimSpace(speakerState)
				if speakerState != "" {
					f.ParsedSpeakerState = append(f.ParsedSpeakerState, speakerState)
				}
			}
			return nil
		}))),

		validation.Field(&f.City, validation.When(f.City != "", validation.By(func(value interface{}) error {
			cityStr := value.(string)
			cities := strings.Split(cityStr, ",")
			f.ParsedCity = make([]string, 0, len(cities))
			for _, city := range cities {
				city = strings.TrimSpace(city)
				if city != "" {
					f.ParsedCity = append(f.ParsedCity, city)
				}
			}
			return nil
		}))),

		validation.Field(&f.Country, validation.When(f.Country != "", validation.By(func(value interface{}) error {
			countryStr := value.(string)
			countries := strings.Split(countryStr, ",")
			f.ParsedCountry = make([]string, 0, len(countries))
			for _, country := range countries {
				country = strings.TrimSpace(country)
				if country != "" {
					f.ParsedCountry = append(f.ParsedCountry, country)
				}
			}
			return nil
		}))),

		validation.Field(&f.ExhibitorState, validation.When(f.ExhibitorState != "", validation.By(func(value interface{}) error {
			exhibitorStateStr := value.(string)
			exhibitorStates := strings.Split(exhibitorStateStr, ",")
			f.ParsedExhibitorState = make([]string, 0, len(exhibitorStates))
			for _, exhibitorState := range exhibitorStates {
				exhibitorState = strings.TrimSpace(exhibitorState)
				if exhibitorState != "" {
					f.ParsedExhibitorState = append(f.ParsedExhibitorState, exhibitorState)
				}
			}
			return nil
		}))),

		validation.Field(&f.Products, validation.When(f.Products != "", validation.By(func(value interface{}) error {
			productsStr := value.(string)
			products := strings.Split(productsStr, "|:")
			f.ParsedProducts = make([]string, 0, len(products))
			for _, product := range products {
				product = strings.TrimSpace(product)
				if product != "" {
					f.ParsedProducts = append(f.ParsedProducts, product)
				}
			}
			return nil
		}))),

		validation.Field(&f.Type, validation.When(f.Type != "", validation.By(func(value interface{}) error {
			typeStr := value.(string)
			types := strings.Split(typeStr, ",")
			f.ParsedType = make([]string, 0, len(types))
			for _, t := range types {
				t = strings.TrimSpace(t)
				if t != "" {
					f.ParsedType = append(f.ParsedType, t)
				}
			}
			return nil
		}))),

		validation.Field(&f.Venue, validation.When(f.Venue != "", validation.By(func(value interface{}) error {
			venueStr := value.(string)
			venues := strings.Split(venueStr, ",")
			f.ParsedVenue = make([]string, 0, len(venues))
			for _, venue := range venues {
				venue = strings.TrimSpace(venue)
				if venue != "" {
					f.ParsedVenue = append(f.ParsedVenue, venue)
				}
			}
			return nil
		}))),

		validation.Field(&f.Company, validation.When(f.Company != "", validation.By(func(value interface{}) error {
			companyStr := value.(string)
			companies := strings.Split(companyStr, ",")
			f.ParsedCompany = make([]string, 0, len(companies))
			for _, company := range companies {
				company = strings.TrimSpace(company)
				if company != "" {
					f.ParsedCompany = append(f.ParsedCompany, company)
				}
			}
			return nil
		}))),

		validation.Field(&f.SponsorState, validation.When(f.SponsorState != "", validation.By(func(value interface{}) error {
			sponsorStateStr := value.(string)
			sponsorStates := strings.Split(sponsorStateStr, ",")
			f.ParsedSponsorState = make([]string, 0, len(sponsorStates))
			for _, sponsorState := range sponsorStates {
				sponsorState = strings.TrimSpace(sponsorState)
				if sponsorState != "" {
					f.ParsedSponsorState = append(f.ParsedSponsorState, sponsorState)
				}
			}
			return nil
		}))),

		validation.Field(&f.VisitorState, validation.When(f.VisitorState != "", validation.By(func(value interface{}) error {
			visitorStateStr := value.(string)
			visitorStates := strings.Split(visitorStateStr, ",")
			f.ParsedVisitorState = make([]string, 0, len(visitorStates))
			for _, visitorState := range visitorStates {
				visitorState = strings.TrimSpace(visitorState)
				if visitorState != "" {
					f.ParsedVisitorState = append(f.ParsedVisitorState, visitorState)
				}
			}
			return nil
		}))),

		validation.Field(&f.Keywords, validation.When(f.Keywords != "", validation.By(func(value interface{}) error {
			keywordsStr := value.(string)
			keywords := strings.Split(keywordsStr, ",")
			var include, exclude []string
			for _, keyword := range keywords {
				keyword = strings.TrimSpace(keyword)
				if keyword == "" {
					continue
				}
				if strings.HasPrefix(keyword, "-") {
					exclude = append(exclude, strings.TrimSpace(keyword[1:]))
				} else {
					include = append(include, keyword)
				}
			}
			if len(include) > 0 || len(exclude) > 0 {
				f.ParsedKeywords = &Keywords{Include: include, Exclude: exclude}
			}
			return nil
		}))),

		validation.Field(&f.IsBranded, validation.When(f.IsBranded != "", validation.By(func(value interface{}) error {
			isBrandedStr := value.(string)
			val, err := strconv.ParseBool(isBrandedStr)
			if err != nil {
				return validation.NewError("invalid_bool", "IsBranded must be a valid boolean value")
			}
			f.ParsedIsBranded = &val
			return nil
		}))),

		validation.Field(&f.Lat, validation.When(f.Lon != "", validation.Required.Error("Latitude is required when longitude is provided"))),
		validation.Field(&f.Lon, validation.When(f.Lat != "", validation.Required.Error("Longitude is required when latitude is provided"))),
		validation.Field(&f.VenueLatitude, validation.When(f.VenueLongitude != "", validation.Required.Error("Venue latitude is required when venue longitude is provided"))),
		validation.Field(&f.VenueLongitude, validation.When(f.VenueLatitude != "", validation.Required.Error("Venue longitude is required when venue latitude is provided"))),
		validation.Field(&f.ToAggregate, validation.When(f.View != "" && strings.Contains(f.View, "agg"), validation.Required.Error("toAggregate field is required when view includes 'agg'"))),

		validation.Field(&f.StartGte, validateAndNormalizeDate(&f.StartGte, "start.gte")),
		validation.Field(&f.StartLte, validateAndNormalizeDate(&f.StartLte, "start.lte")),
		validation.Field(&f.StartGt, validateAndNormalizeDate(&f.StartGt, "start.gt")),
		validation.Field(&f.StartLt, validateAndNormalizeDate(&f.StartLt, "start.lt")),
		validation.Field(&f.EndGte, validateAndNormalizeDate(&f.EndGte, "end.gte")),
		validation.Field(&f.EndLte, validateAndNormalizeDate(&f.EndLte, "end.lte")),
		validation.Field(&f.EndGt, validateAndNormalizeDate(&f.EndGt, "end.gt")),
		validation.Field(&f.EndLt, validateAndNormalizeDate(&f.EndLt, "end.lt")),
		validation.Field(&f.ActiveGte, validateAndNormalizeDate(&f.ActiveGte, "active.gte")),
		validation.Field(&f.ActiveLte, validateAndNormalizeDate(&f.ActiveLte, "active.lte")),
		validation.Field(&f.ActiveGt, validateAndNormalizeDate(&f.ActiveGt, "active.gt")),
		validation.Field(&f.ActiveLt, validateAndNormalizeDate(&f.ActiveLt, "active.lt")),
		validation.Field(&f.CreatedAt, validateAndNormalizeDate(&f.CreatedAt, "createdAt")),

		validation.Field(&f.Dates, validation.When(f.Dates != "", validation.By(func(value interface{}) error {
			datesStr := value.(string)
			if datesStr == "" {
				return nil
			}

			var dateRanges [][]interface{}
			if err := json.Unmarshal([]byte(datesStr), &dateRanges); err != nil {
				return validation.NewError("invalid_dates_json", "Invalid JSON format for dates: "+err.Error())
			}

			if len(dateRanges) == 0 {
				return validation.NewError("empty_dates", "Dates array cannot be empty")
			}

			f.ParsedDates = make([]DateRange, 0, len(dateRanges))

			for i, rangeItem := range dateRanges {
				if len(rangeItem) != 2 {
					return validation.NewError("invalid_date_range", fmt.Sprintf("Date range at index %d must have exactly 2 elements [start, end]", i))
				}

				var startDate, endDate *string

				if rangeItem[0] != nil {
					startStr, ok := rangeItem[0].(string)
					if !ok {
						return validation.NewError("invalid_start_date", fmt.Sprintf("Start date at index %d must be a string or null", i))
					}
					startStr = strings.TrimSpace(startStr)
					if startStr != "" {
						parsedDate, err := time.Parse("2006-01-02", startStr)
						if err != nil {
							parsedDate, err = time.Parse(time.RFC3339, startStr)
							if err != nil {
								return validation.NewError("invalid_start_date_format", fmt.Sprintf("Start date at index %d must be in YYYY-MM-DD format: %s", i, startStr))
							}
						}
						normalized := parsedDate.Format("2006-01-02")
						startDate = &normalized
					}
				}

				if rangeItem[1] != nil {
					endStr, ok := rangeItem[1].(string)
					if !ok {
						return validation.NewError("invalid_end_date", fmt.Sprintf("End date at index %d must be a string or null", i))
					}
					endStr = strings.TrimSpace(endStr)
					if endStr != "" {
						parsedDate, err := time.Parse("2006-01-02", endStr)
						if err != nil {
							parsedDate, err = time.Parse(time.RFC3339, endStr)
							if err != nil {
								return validation.NewError("invalid_end_date_format", fmt.Sprintf("End date at index %d must be in YYYY-MM-DD format: %s", i, endStr))
							}
						}
						normalized := parsedDate.Format("2006-01-02")
						endDate = &normalized
					}
				}

				if startDate == nil && endDate == nil {
					return validation.NewError("empty_date_range", fmt.Sprintf("Date range at index %d must have at least one of start or end date", i))
				}
				f.ParsedDates = append(f.ParsedDates, DateRange{startDate, endDate})
			}

			return nil
		}))),

		validation.Field(&f.EventRanking, validation.When(f.EventRanking != "", validation.By(func(value interface{}) error {
			eventRankingStr := value.(string)
			eventRankings := strings.Split(eventRankingStr, ",")
			if len(eventRankings) > 1 {
				return validation.NewError("multiple_event_ranking", "Only one eventRanking value is allowed")
			}
			f.ParsedEventRanking = make([]string, 0, len(eventRankings))
			validRankings := []string{"100", "500", "1000"}
			for _, ranking := range eventRankings {
				ranking = strings.TrimSpace(ranking)
				if ranking != "" {
					valid := false
					for _, validRanking := range validRankings {
						if ranking == validRanking {
							valid = true
							break
						}
					}
					if !valid {
						return validation.NewError("invalid_event_ranking", "Invalid eventRanking value: "+ranking+". Valid values are: 100, 500, 1000")
					}
					f.ParsedEventRanking = append(f.ParsedEventRanking, ranking)
				}
			}
			return nil
		}))),

		validation.Field(&f.JobComposite, validation.When(f.JobComposite != "", validation.By(func(value interface{}) error {
			jobCompositeStr := value.(string)
			jobComposites := strings.Split(jobCompositeStr, ",")
			f.ParsedJobComposite = make([]string, 0, len(jobComposites))
			for _, jobComposite := range jobComposites {
				jobComposite = strings.TrimSpace(jobComposite)
				if jobComposite != "" {
					f.ParsedJobComposite = append(f.ParsedJobComposite, jobComposite)
				}
			}
			return nil
		}))),

		validation.Field(&f.Published, validation.When(f.Published != "", validation.By(func(value interface{}) error {
			publishedStr := value.(string)
			publishedStr = strings.ReplaceAll(publishedStr, "&", ",")
			publishedValues := strings.Split(publishedStr, ",")
			f.ParsedPublished = make([]string, 0, len(publishedValues))
			validPublishedValues := map[string]bool{
				"1": true,
				"2": true,
			}
			for _, published := range publishedValues {
				published = strings.TrimSpace(published)
				if published != "" {
					if !validPublishedValues[published] {
						return validation.NewError("invalid_published", "Invalid published value: "+published+". Valid values are: 1, 2")
					}
					f.ParsedPublished = append(f.ParsedPublished, published)
				}
			}
			return nil
		}))),

		validation.Field(&f.EventTypeGroup, validation.When(f.EventTypeGroup != "", validation.By(func(value interface{}) error {
			eventTypeGroupStr := value.(string)
			eventTypeGroupLower := strings.ToLower(strings.TrimSpace(eventTypeGroupStr))

			validGroups := map[string]Groups{
				"social":     GroupSocial,
				"business":   GroupBusiness,
				"unattended": GroupUnattended,
			}

			if group, exists := validGroups[eventTypeGroupLower]; exists {
				f.ParsedEventTypeGroup = &group
				f.EventTypeGroup = eventTypeGroupLower
			} else {
				validOptions := []string{"social", "business", "unattended"}
				return validation.NewError("invalid_event_type_group", "Invalid event type group: "+eventTypeGroupStr+". Valid options are: "+strings.Join(validOptions, ", "))
			}
			return nil
		}))),

		validation.Field(&f.ViewBound, validation.When(f.ViewBound != "", validation.By(func(value interface{}) error {
			viewBoundStr := value.(string)
			viewBoundStr = strings.TrimSpace(viewBoundStr)
			if viewBoundStr == "" {
				return nil
			}

			var viewBound ViewBound
			if err := json.Unmarshal([]byte(viewBoundStr), &viewBound); err != nil {
				return validation.NewError("invalid_view_bound", "Invalid viewBound JSON: "+err.Error())
			}

			boundTypeLower := strings.ToLower(strings.TrimSpace(string(viewBound.BoundType)))
			validBoundTypes := map[string]BoundType{
				"point": BoundTypePoint,
				"box":   BoundTypeBox,
			}

			boundType, exists := validBoundTypes[boundTypeLower]
			if !exists {
				return validation.NewError("invalid_bound_type", "Invalid boundType: "+string(viewBound.BoundType)+". Valid options are: point, box")
			}
			viewBound.BoundType = boundType

			switch boundType {
			case BoundTypePoint:
				var geoCoords GeoCoordinates
				if err := json.Unmarshal(viewBound.Coordinates, &geoCoords); err != nil {
					return validation.NewError("invalid_coordinates", "Invalid coordinates for point type: "+err.Error())
				}
				if geoCoords.Latitude < -90 || geoCoords.Latitude > 90 {
					return validation.NewError("invalid_latitude", "Latitude must be between -90 and 90")
				}
				if geoCoords.Longitude < -180 || geoCoords.Longitude > 180 {
					return validation.NewError("invalid_longitude", "Longitude must be between -180 and 180")
				}
				if geoCoords.Radius != nil && *geoCoords.Radius > 10 {
					return validation.NewError("invalid_radius", "Radius must be <= 10")
				}
			case BoundTypeBox:
				var boxCoords []float64
				if err := json.Unmarshal(viewBound.Coordinates, &boxCoords); err != nil {
					return validation.NewError("invalid_coordinates", "Invalid coordinates for box type: "+err.Error())
				}
				if len(boxCoords) != 4 {
					return validation.NewError("invalid_box_coordinates", "Box coordinates must have exactly 4 numbers")
				}
			default:
				return validation.NewError("invalid_bound_type", "Invalid boundType: "+string(viewBound.BoundType)+". Valid options are: point, box")
			}

			f.ParsedViewBound = &viewBound
			return nil
		}))),
	)
}

type SearchEventsRequest struct {
	FilterDataDto
	PaginationDto
	ResponseDataDto
	// UserID       string        `json:"user_id"`
	APIID string `json:"api_id" form:"api_id"`
}

func (s *SearchEventsRequest) Validate() error {
	if err := s.FilterDataDto.Validate(); err != nil {
		return err
	}
	if err := s.PaginationDto.Validate(); err != nil {
		return err
	}
	return validation.ValidateStruct(s,
		validation.Field(&s.APIID, validation.Required, validation.Match(regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`))),
	)
}
