package models

import (
	"encoding/json"
	"fmt"
	"log"
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
	Include         []string `json:"include"`
	Exclude         []string `json:"exclude"`
	IncludeForQuery []string `json:"-"`
	ExcludeForQuery []string `json:"-"`
}

func cleanKeywordForQuery(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	segments := strings.Split(s, "_")
	var kept []string
	for _, seg := range segments {
		if len(seg) > 1 {
			kept = append(kept, seg)
			continue
		}
		if len(seg) == 1 {
			c := seg[0]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
				kept = append(kept, seg)
			}
		}
	}
	return strings.Join(kept, "_")
}

func KeywordPartsForMatch(cleaned string) []string {
	if cleaned == "" {
		return nil
	}
	var parts []string
	for _, p := range strings.Split(cleaned, "_") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) >= 2 {
		return parts
	}
	return []string{cleaned}
}

type DateRange [2]*string

type RatingRange struct {
	Start float64
	End   float64
}

type Groups string

const (
	GroupSocial     Groups = "social"
	GroupBusiness   Groups = "business"
	GroupUnattended Groups = "unattended"
)

const NilEventUUID = "00000000-0000-0000-0000-000000000000"

var EventTypeById = map[string]string{
	"e5283caa-f655-504b-8e44-49ae0edb3faa": "festival",
	"bffa5040-c654-5991-a1c5-0610e2c0ec74": "sport",
	"69cf1329-0c71-5dae-b7a9-838c5712bce0": "concert",
	"94fcb56e-2838-5d74-9092-e582d873a03e": "stage-performance",
	"3a3609e5-56df-5a8b-ad47-c9e168eb4f59": "community-group",
	"9b5524b4-60f5-5478-b3f0-38e2e12e3981": "tradeshows",
	"4de48054-46fb-5452-a23f-8aac6c00592e": "conferences",
	"ad7c83a5-b8fc-5109-a159-9306848de22c": "workshops",
	"5b37e581-53f7-5dcf-8177-c6a43774b168": "holiday",
	"9caca041-25c6-5340-9522-78d354b58ca2": "specialty-shows",
}

type View string

const (
	ViewList     View = "list"
	ViewAgg      View = "agg"
	ViewMap      View = "map"
	ViewDetail   View = "detail"
	ViewCalendar View = "calendar"
	ViewTracker  View = "tracker"
	ViewPromote  View = "promote"
	ViewCount    View = "count"
	ViewTrends   View = "trends"
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
	ToEvent     bool            `json:"toEvent,omitempty"`
	Unit        string          `json:"unit,omitempty"`
}

type AlertSearchGroupBy string

const (
	AlertSearchGroupByDay       AlertSearchGroupBy = "day"
	AlertSearchGroupByAlertType AlertSearchGroupBy = "alertType"
)

type CountGroup string

const (
	CountGroupYear                  CountGroup = "year"
	CountGroupMonth                 CountGroup = "month"
	CountGroupWeek                  CountGroup = "week"
	CountGroupDay                   CountGroup = "day"
	CountGroupCountry               CountGroup = "country"
	CountGroupState                 CountGroup = "state"
	CountGroupCity                  CountGroup = "city"
	CountGroupAlertType             CountGroup = "alertType"
	CountGroupEventTypeGroup        CountGroup = "eventTypeGroup"
	CountGroupStatus                CountGroup = "status"
	CountGroupEventType             CountGroup = "eventType"
	CountGroupCategory              CountGroup = "category"
	CountGroupPredictedAttendance   CountGroup = "predictedAttendance"
	CountGroupInboundEstimate       CountGroup = "inboundEstimate"
	CountGroupInternationalEstimate CountGroup = "internationalEstimate"
	CountGroupEventCount            CountGroup = "eventCount"
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

type JobCompositeConditions struct {
	Logic         string   `json:"logic,omitempty"` // "AND" or "OR"
	DepartmentIds []string `json:"departmentIds,omitempty"`
	SeniorityIds  []string `json:"seniorityIds,omitempty"`
}

type JobCompositeProperty struct {
	Department []string `json:"department,omitempty"`
	Role       []string `json:"role,omitempty"`
	Name       []string `json:"name,omitempty"`
}

type JobCompositeFilter struct {
	DesignationIds []string                `json:"designationIds,omitempty"`
	Logic          string                  `json:"logic,omitempty"` // defaults to "AND"
	Conditions     *JobCompositeConditions `json:"conditions,omitempty"`
	PropertyIds    []string                `json:"propertyIds,omitempty"`
	Property       *JobCompositeProperty   `json:"property,omitempty"`
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
	LocationIds    string `json:"locationIds,omitempty" form:"locationIds"`

	CountryIds  string `json:"countryIds,omitempty" form:"countryIds"`
	StateIds    string `json:"stateIds,omitempty" form:"stateIds"`
	CityIds     string `json:"cityIds,omitempty" form:"cityIds"`
	VenueIds    string `json:"venueIds,omitempty" form:"venueIds"`
	CategoryIds string `json:"categoryIds,omitempty" form:"categoryIds"`

	SearchByEntity  string `json:"searchByEntity,omitempty" form:"searchByEntity"`
	AdvanceSearchBy string `json:"advanceSearchBy,omitempty" form:"advanceSearchBy"` // Can be set directly or from searchByEntity mapping
	GroupBy         string `json:"groupBy,omitempty" form:"groupBy"`
	GetNew          *bool  `json:"getNew,omitempty" form:"getNew"`
	EventGroupCount string `json:"eventGroupCount,omitempty" form:"eventGroupCount"`

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

	JobComposite string `json:"jobComposite,omitempty" form:"jobComposite"`
	// DesignationIds string `json:"designationIds,omitempty" form:"designationIds"`
	// SeniorityIds string `json:"seniorityIds,omitempty" form:"seniorityIds"`

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

	EndGte        string `json:"end.gte,omitempty" form:"end.gte"`
	EndLte        string `json:"end.lte,omitempty" form:"end.lte"`
	StartGte      string `json:"start.gte,omitempty" form:"start.gte"`
	StartLte      string `json:"start.lte,omitempty" form:"start.lte"`
	StartGt       string `json:"start.gt,omitempty" form:"start.gt"`
	EndGt         string `json:"end.gt,omitempty" form:"end.gt"`
	StartLt       string `json:"start.lt,omitempty" form:"start.lt"`
	EndLt         string `json:"end.lt,omitempty" form:"end.lt"`
	CreatedAt     string `json:"createdAt,omitempty" form:"createdAt"`
	Dates         string `json:"dates,omitempty" form:"dates"`
	PastBetween   string `json:"pastBetween,omitempty" form:"pastBetween"`
	ActiveBetween string `json:"activeBetween,omitempty" form:"activeBetween"`

	ActiveGte string `json:"active.gte,omitempty" form:"active.gte"`
	ActiveLte string `json:"active.lte,omitempty" form:"active.lte"`
	ActiveGt  string `json:"active.gt,omitempty" form:"active.gt"`
	ActiveLt  string `json:"active.lt,omitempty" form:"active.lt"`

	Forecasted string `json:"forecasted,omitempty" form:"forecasted"`

	Type       string `json:"type,omitempty" form:"type"`
	EventTypes string `json:"eventTypes,omitempty" form:"eventTypes"`
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
	Regions            string `json:"regions,omitempty" form:"regions"`

	Company        string `json:"company,omitempty" form:"company"`
	CompanyCountry string `json:"companyCountry,omitempty" form:"companyCountry"`
	CompanyDomain  string `json:"companyDomain,omitempty" form:"companyDomain"`
	CompanyCity    string `json:"companyCity,omitempty" form:"companyCity"`
	CompanyState   string `json:"companyState,omitempty" form:"companyState"`

	CompanyId      string `json:"companyId,omitempty" form:"companyId"`
	CompanyName    string `json:"companyName,omitempty" form:"companyName"`
	CompanyWebsite string `json:"companyWebsite,omitempty" form:"companyWebsite"`

	View                string `json:"view,omitempty" form:"view"`
	CalendarType        string `json:"calendar_type,omitempty" form:"calendar_type"`
	TrackerDates        string `json:"trackerDates,omitempty" form:"trackerDates"`
	DateView            string `json:"dateView,omitempty" form:"dateView"`
	Columns             string `json:"columns,omitempty" form:"columns"`
	GroupByTrends       string `json:"groupByTrends,omitempty" form:"groupByTrends"`
	Source              string `json:"source,omitempty" form:"source"`
	ShowCount           bool   `json:"showCount,omitempty" form:"showCount"`
	Frequency           string `json:"frequency,omitempty" form:"frequency"`
	Visibility          string `json:"visibility,omitempty" form:"visibility"`
	Mode                string `json:"mode,omitempty" form:"mode"`
	EstimatedVisitors   string `json:"estimatedVisitors,omitempty" form:"estimatedVisitors"`
	EstimatedExhibitors string `json:"estimatedExhibitors,omitempty" form:"estimatedExhibitors"`
	IsBranded           string `json:"isBranded,omitempty" form:"isBranded"`
	IsSeries            string `json:"isSeries,omitempty" form:"isSeries"`
	Maturity            string `json:"maturity,omitempty" form:"maturity"`
	Status              string `json:"status,omitempty" form:"status"`
	Published           string `json:"published,omitempty" form:"published"`
	EditionType         string `json:"editionType,omitempty" form:"editionType"`
	EventTypeGroup      string `json:"eventTypeGroup,omitempty" form:"eventTypeGroup"`
	ViewBound           string `json:"viewBound,omitempty" form:"viewBound"`
	ViewBounds          string `json:"viewBounds,omitempty" form:"viewBounds"`

	VenueLatitude  string `json:"venueLatitude,omitempty" form:"venueLatitude"`
	VenueLongitude string `json:"venueLongitude,omitempty" form:"venueLongitude"`

	ToAggregate string `json:"toAggregate,omitempty" form:"toAggregate"`

	EventRanking   string `json:"eventRanking,omitempty" form:"eventRanking"`
	AudienceZone   string `json:"audienceZone,omitempty" form:"audienceZone"`
	EventEstimate  bool   `json:"eventEstimate,omitempty" form:"eventEstimate"`
	ImpactScore    bool   `json:"impactScore,omitempty" form:"impactScore"`
	AudienceSpread string `json:"audienceSpread,omitempty" form:"audienceSpread"`
	EventAudience  string `json:"eventAudience,omitempty" form:"eventAudience"`

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

	UserId          string `json:"userId,omitempty" form:"userId"`
	UserName        string `json:"userName,omitempty" form:"userName"`
	UserCompanyName string `json:"userCompanyName,omitempty" form:"userCompanyName"`

	ParsedCategory           []string            `json:"-"`
	ParsedLocationIds        []string            `json:"-"`
	ParsedCity               []string            `json:"-"`
	ParsedCountry            []string            `json:"-"`
	ParsedProducts           []string            `json:"-"`
	ParsedType               []string            `json:"-"`
	ParsedEventTypes         []string            `json:"-"`
	ParsedVenue              []string            `json:"-"`
	ParsedCompany            []string            `json:"-"`
	ParsedView               []string            `json:"-"`
	ParsedToAggregate        []string            `json:"-"`
	ParsedKeywords           *Keywords           `json:"-"`
	ParsedIsBranded          *bool               `json:"-"`
	ParsedIsSeries           *bool               `json:"-"`
	ParsedMode               []string            `json:"-"`
	ParsedStatus             []string            `json:"-"`
	ParsedState              []string            `json:"-"`
	ParsedCompanyState       []string            `json:"-"`
	ParsedCompanyCity        []string            `json:"-"`
	ParsedCompanyDomain      []string            `json:"-"`
	ParsedCompanyCountry     []string            `json:"-"`
	ParsedSpeakerState       []string            `json:"-"`
	ParsedExhibitorState     []string            `json:"-"`
	ParsedSponsorState       []string            `json:"-"`
	ParsedVisitorState       []string            `json:"-"`
	ParsedJobComposite       []string            `json:"-"`
	ParsedJobCompositeFilter *JobCompositeFilter `json:"-"`
	ParsedEventRanking       []string            `json:"-"`
	ParsedAudienceZone       []string            `json:"-"`
	ParsedAudienceSpread     []string            `json:"-"`
	ParsedEventAudience      []int               `json:"-"`
	ParsedPublished          []string            `json:"-"`
	ParsedEditionType        []string            `json:"-"`
	ParsedVisibility         []string            `json:"-"`
	ParsedEventTypeGroup     *Groups             `json:"-"`
	ParsedDesignationId      []string            `json:"-"`
	ParsedSeniorityId        []string            `json:"-"`
	ParsedViewBound          *ViewBound          `json:"-"`
	ParsedViewBounds         []*ViewBound        `json:"-"`
	ParsedEventIds           []string            `json:"-"`
	ParsedNotEventIds        []string            `json:"-"`
	ParsedSourceEventIds     []string            `json:"-"`
	ParsedGroupBy            []CountGroup        `json:"-"`
	ParsedGetNew             *bool               `json:"-"`
	ParsedEventGroupCount    *bool               `json:"-"`
	ParsedDates              []DateRange         `json:"-"`
	ParsedPastBetween        *struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"-"`
	ParsedActiveBetween *struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"-"`
	ParsedAvgRating           []RatingRange `json:"-"`
	ParsedTrackerDates        []string      `json:"-"`
	ParsedCalendarType        *string       `json:"-"`
	ParsedDateView            *string       `json:"-"`
	ParsedColumns             []string      `json:"-"`
	ParsedGroupByTrends       *string       `json:"-"`
	ParsedRegions             []string      `json:"-"`
	ParsedCountryIds          []string      `json:"-"`
	ParsedStateIds            []string      `json:"-"`
	ParsedCityIds             []string      `json:"-"`
	ParsedVenueIds            []string      `json:"-"`
	ParsedCategoryIds         []string      `json:"-"`
	ParsedUserId              []string      `json:"-"`
	ParsedUserName            []string      `json:"-"`
	ParsedUserCompanyName     []string      `json:"-"`
	ParsedCompanyId           []string      `json:"-"`
	ParsedCompanyName         []string      `json:"-"`
	ParsedFrequency           []string      `json:"-"`
	ParsedCompanyWebsite      []string      `json:"-"`
	ParsedSearchByEntity      []string      `json:"-"`
	ParsedAdvancedSearchBy    []string      `json:"-"` // Parsed version of AdvanceSearchBy
	ParsedPrice               []string      `json:"-"`
	ParsedEstimatedVisitors   []string      `json:"-"`
	ParsedEstimatedExhibitors []string      `json:"-"`
	ParsedMaturity            []string      `json:"-"`
}

func (f *FilterDataDto) SetDefaultValues() {
	if f.Radius == "" {
		f.Radius = "5"
	}
	if f.Unit == "" {
		f.Unit = "km"
	}
	// changes made due to GEO/GTM requests
	if f.Published == "" {
		f.Published = "1,2"
	}
	if strings.ToLower(f.View) == "calendar" && f.CalendarType == "" {
		f.CalendarType = "month"
	}
	// Default editionType to current
	if f.EditionType == "" {
		f.EditionType = "current"
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

func GetEventTypeGroupsFromIDs(eventTypeIDs []string) map[string]bool {
	eventTypeGroups := map[string]string{
		"festival":          "social",
		"sport":             "social",
		"concert":           "social",
		"stage-performance": "social",
		"community-group":   "social",
		"tradeshows":        "business",
		"conferences":       "business",
		"workshops":         "business",
		"holiday":           "unattended",
		"specialty-shows":   "social",
	}

	groups := make(map[string]bool)
	for _, eventTypeID := range eventTypeIDs {
		if slug, exists := EventTypeById[eventTypeID]; exists {
			if group, exists := eventTypeGroups[slug]; exists {
				groups[group] = true
			}
		}
	}
	return groups
}

func GetAllEventTypeIDsByGroup(eventTypeGroup Groups) []string {
	eventTypeGroups := map[string]string{
		"festival":          "social",
		"sport":             "social",
		"concert":           "social",
		"stage-performance": "social",
		"community-group":   "social",
		"tradeshows":        "business",
		"conferences":       "business",
		"workshops":         "business",
		"holiday":           "unattended",
		"specialty-shows":   "social",
	}

	targetGroup := string(eventTypeGroup)
	eventTypeIDs := make([]string, 0)

	for eventTypeID, slug := range EventTypeById {
		if group, exists := eventTypeGroups[slug]; exists && group == targetGroup {
			eventTypeIDs = append(eventTypeIDs, eventTypeID)
		}
	}

	return eventTypeIDs
}

func filterEventTypesByGroup(eventTypeIDs []string, eventTypeGroup Groups) []string {
	if len(eventTypeIDs) == 0 {
		return []string{}
	}

	targetGroup := string(eventTypeGroup)

	filteredEventTypes := make([]string, 0)
	for _, eventTypeID := range eventTypeIDs {
		eventTypeGroups := GetEventTypeGroupsFromIDs([]string{eventTypeID})
		if eventTypeGroups[targetGroup] {
			filteredEventTypes = append(filteredEventTypes, eventTypeID)
		}
	}

	return filteredEventTypes
}

func (f *FilterDataDto) applyEventTypeBasedMappings(wasPublishedExplicitlyProvided, wasEventAudienceExplicitlyProvided bool) {
	if len(f.ParsedEventTypes) == 0 {
		return
	}

	groups := GetEventTypeGroupsFromIDs(f.ParsedEventTypes)
	hasHoliday := groups["unattended"]
	hasSocial := groups["social"]
	hasBusiness := groups["business"]

	if !wasPublishedExplicitlyProvided {
		publishedSet := make(map[string]bool)

		if hasHoliday {
			publishedSet["4"] = true
		}
		if hasSocial || hasBusiness {
			publishedSet["1"] = true
			publishedSet["2"] = true
		}
		if hasHoliday && (hasSocial || hasBusiness) {
			publishedSet["1"] = true
			publishedSet["2"] = true
			publishedSet["4"] = true
		}

		if len(publishedSet) > 0 {
			f.ParsedPublished = make([]string, 0, len(publishedSet))
			for pub := range publishedSet {
				f.ParsedPublished = append(f.ParsedPublished, pub)
			}
		}
	}

	// Apply eventAudience mapping only if eventAudience is not explicitly provided
	if !wasEventAudienceExplicitlyProvided {
		audienceSet := make(map[int]bool)

		if hasSocial {
			audienceSet[10100] = true // B2C
		}
		if hasBusiness {
			audienceSet[11000] = true // B2B
		}
		if hasSocial && hasBusiness {
			audienceSet[10100] = true // B2C
			audienceSet[11000] = true // B2B
		}

		if len(audienceSet) > 0 {
			f.ParsedEventAudience = make([]int, 0, len(audienceSet))
			for aud := range audienceSet {
				f.ParsedEventAudience = append(f.ParsedEventAudience, aud)
			}
		}
	}
}

func combineRatingRanges(ranges []RatingRange) []RatingRange {
	if len(ranges) == 0 {
		return ranges
	}

	for i := 0; i < len(ranges)-1; i++ {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[i].Start > ranges[j].Start {
				ranges[i], ranges[j] = ranges[j], ranges[i]
			}
		}
	}

	combined := []RatingRange{ranges[0]}
	for i := 1; i < len(ranges); i++ {
		last := &combined[len(combined)-1]
		current := ranges[i]

		if current.Start <= last.End {
			if current.End > last.End {
				last.End = current.End
			}
		} else {
			combined = append(combined, current)
		}
	}

	return combined
}

func (f *FilterDataDto) Validate() error {
	wasPublishedExplicitlyProvided := f.Published != ""
	wasEventAudienceExplicitlyProvided := f.EventAudience != ""

	f.SetDefaultValues()

	if f.ActiveGte != "" && f.ActiveLte != "" {
		activeGteNorm := strings.TrimSpace(f.ActiveGte)
		if activeGteNorm == "1990-01-01" || activeGteNorm == "1970-01-01" {
			f.ActiveGte = ""
		}
	}

	err := validation.ValidateStruct(f,
		validation.Field(&f.Price, validation.When(f.Price != "", validation.By(func(value interface{}) error {
			priceStr := value.(string)
			if priceStr == "" {
				return nil
			}

			prices := strings.Split(priceStr, ",")
			f.ParsedPrice = make([]string, 0, len(prices))

			validPrices := map[string]string{
				"free":          "free",
				"paid":          "paid",
				"free_and_paid": "free_and_paid",
			}

			var invalidPrices []string
			seenPrices := make(map[string]bool)
			for _, price := range prices {
				price = strings.TrimSpace(price)
				if price != "" {
					priceLower := strings.ToLower(price)
					if normalizedValue, exists := validPrices[priceLower]; exists {
						if !seenPrices[normalizedValue] {
							f.ParsedPrice = append(f.ParsedPrice, normalizedValue)
							seenPrices[normalizedValue] = true
						}
					} else {
						invalidPrices = append(invalidPrices, price)
					}
				}
			}

			if len(invalidPrices) > 0 {
				validOptions := []string{"free", "paid", "free_and_paid"}
				return validation.NewError("invalid_price", "Invalid price value(s): "+strings.Join(invalidPrices, ", ")+". Valid values are: "+strings.Join(validOptions, ", "))
			}

			if len(f.ParsedPrice) == 0 {
				return validation.NewError("empty_price", "price cannot be empty after parsing")
			}

			return nil
		}))), // Price validation
		validation.Field(&f.AvgRating, validation.When(f.AvgRating != "", validation.By(func(value interface{}) error {
			avgRatingStr := value.(string)
			avgRatingStr = strings.TrimSpace(avgRatingStr)
			if avgRatingStr == "" {
				return nil
			}

			ratingRanges := strings.Split(avgRatingStr, ",")
			validRanges := map[string]RatingRange{
				"0-1": {Start: 0, End: 1},
				"1-2": {Start: 1, End: 2},
				"2-3": {Start: 2, End: 3},
				"3-4": {Start: 3, End: 4},
				"4-5": {Start: 4, End: 5},
			}

			parsedRanges := make([]RatingRange, 0)
			for _, ratingRange := range ratingRanges {
				ratingRange = strings.TrimSpace(ratingRange)
				if ratingRange == "" {
					continue
				}

				if validRange, exists := validRanges[ratingRange]; exists {
					parsedRanges = append(parsedRanges, validRange)
				} else {
					return validation.NewError("invalid_avg_rating", "Invalid avgRating range: "+ratingRange+". Valid ranges are: 0-1, 1-2, 2-3, 3-4, 4-5")
				}
			}

			if len(parsedRanges) > 0 {
				f.ParsedAvgRating = combineRatingRanges(parsedRanges)
			}

			return nil
		}))), // AvgRating validation
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
			for i, sourceEventId := range sourceEventIds {
				sourceEventId = strings.TrimSpace(sourceEventId)
				if sourceEventId != "" {
					if i == 0 && sourceEventId == "-1" {
						sourceEventId = "0"
					}
					quotedIds := fmt.Sprintf("'%s'", sourceEventId)
					f.ParsedSourceEventIds = append(f.ParsedSourceEventIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.LocationIds, validation.When(f.LocationIds != "", validation.By(func(value interface{}) error {
			locationIdsStr := value.(string)
			locationIds := strings.Split(locationIdsStr, ",")
			f.ParsedLocationIds = make([]string, 0, len(locationIds))
			for _, locationId := range locationIds {
				locationId = strings.TrimSpace(locationId)
				if locationId != "" {
					quotedIds := fmt.Sprintf("'%s'", locationId)
					f.ParsedLocationIds = append(f.ParsedLocationIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.CountryIds, validation.When(f.CountryIds != "", validation.By(func(value interface{}) error {
			countryIdsStr := value.(string)
			countryIds := strings.Split(countryIdsStr, ",")
			f.ParsedCountryIds = make([]string, 0, len(countryIds))
			for _, countryId := range countryIds {
				countryId = strings.TrimSpace(countryId)
				if countryId != "" {
					quotedIds := fmt.Sprintf("'%s'", countryId)
					f.ParsedCountryIds = append(f.ParsedCountryIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.StateIds, validation.When(f.StateIds != "", validation.By(func(value interface{}) error {
			stateIdsStr := value.(string)
			stateIds := strings.Split(stateIdsStr, ",")
			f.ParsedStateIds = make([]string, 0, len(stateIds))
			for _, stateId := range stateIds {
				stateId = strings.TrimSpace(stateId)
				if stateId != "" {
					quotedIds := fmt.Sprintf("'%s'", stateId)
					f.ParsedStateIds = append(f.ParsedStateIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.CityIds, validation.When(f.CityIds != "", validation.By(func(value interface{}) error {
			cityIdsStr := value.(string)
			cityIds := strings.Split(cityIdsStr, ",")
			f.ParsedCityIds = make([]string, 0, len(cityIds))
			for _, cityId := range cityIds {
				cityId = strings.TrimSpace(cityId)
				if cityId != "" {
					quotedIds := fmt.Sprintf("'%s'", cityId)
					f.ParsedCityIds = append(f.ParsedCityIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.VenueIds, validation.When(f.VenueIds != "", validation.By(func(value interface{}) error {
			venueIdsStr := value.(string)
			venueIds := strings.Split(venueIdsStr, ",")
			f.ParsedVenueIds = make([]string, 0, len(venueIds))
			for _, venueId := range venueIds {
				venueId = strings.TrimSpace(venueId)
				if venueId != "" {
					quotedIds := fmt.Sprintf("'%s'", venueId)
					f.ParsedVenueIds = append(f.ParsedVenueIds, quotedIds)
				}
			}
			return nil
		}))),

		validation.Field(&f.CategoryIds, validation.When(f.CategoryIds != "", validation.By(func(value interface{}) error {
			categoryIdsStr := value.(string)
			categoryIds := strings.Split(categoryIdsStr, ",")
			f.ParsedCategoryIds = make([]string, 0, len(categoryIds))
			for _, categoryId := range categoryIds {
				categoryId = strings.TrimSpace(categoryId)
				if categoryId != "" {
					quotedIds := fmt.Sprintf("'%s'", categoryId)
					f.ParsedCategoryIds = append(f.ParsedCategoryIds, quotedIds)
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
				"count":    ViewCount,
				"trends":   ViewTrends,
			}

			if view, exists := validViews[viewLower]; exists {
				f.ParsedView = []string{string(view)}
				f.View = viewLower
			} else {
				validOptions := []string{"list", "agg", "map", "detail", "calendar", "tracker", "promote", "count", "trends"}
				return validation.NewError("invalid_view", "Invalid view value: "+viewStr+". Valid options are: "+strings.Join(validOptions, ", "))
			}
			return nil
		})),

		validation.Field(&f.CalendarType, validation.When(f.CalendarType != "", validation.By(func(value interface{}) error {
			calendarTypeStr := value.(string)
			calendarTypeLower := strings.ToLower(strings.TrimSpace(calendarTypeStr))

			validCalendarTypes := map[string]bool{
				"day":   true,
				"week":  true,
				"month": true,
				"year":  true,
			}

			if validCalendarTypes[calendarTypeLower] {
				f.ParsedCalendarType = &calendarTypeLower
				f.CalendarType = calendarTypeLower
			} else {
				validOptions := []string{"day", "week", "month", "year"}
				return validation.NewError("invalid_calendar_type", "Invalid calendar_type value: "+calendarTypeStr+". Valid options are: "+strings.Join(validOptions, ", "))
			}
			return nil
		}))),

		validation.Field(&f.TrackerDates, validation.When(f.TrackerDates != "", validation.By(func(value interface{}) error {
			trackerDatesStr := value.(string)
			if trackerDatesStr == "" {
				return nil
			}

			dates := strings.Split(trackerDatesStr, ",")
			if len(dates) != 2 {
				return validation.NewError("invalid_tracker_dates", "trackerDates must contain exactly 2 dates separated by comma (start,end)")
			}

			f.ParsedTrackerDates = make([]string, 0, 2)
			for i, dateStr := range dates {
				dateStr = strings.TrimSpace(dateStr)
				if dateStr == "" {
					return validation.NewError("invalid_tracker_date", fmt.Sprintf("Tracker date at index %d cannot be empty", i))
				}

				// Validate date format (YYYY-MM-DD)
				parsedDate, err := time.Parse("2006-01-02", dateStr)
				if err != nil {
					parsedDate, err = time.Parse(time.RFC3339, dateStr)
					if err != nil {
						return validation.NewError("invalid_tracker_date_format", fmt.Sprintf("Tracker date at index %d must be in YYYY-MM-DD format: %s", i, dateStr))
					}
				}

				normalizedDate := parsedDate.Format("2006-01-02")
				f.ParsedTrackerDates = append(f.ParsedTrackerDates, normalizedDate)
			}

			return nil
		}))),

		validation.Field(&f.GroupBy, validation.When(f.GroupBy != "", validation.By(func(value interface{}) error {
			groupByStr := value.(string)
			if groupByStr == "" {
				return nil
			}

			groupByItems := strings.Split(groupByStr, ",")
			f.ParsedGroupBy = make([]CountGroup, 0, len(groupByItems))

			validCountGroups := map[string]CountGroup{
				"year":                  CountGroupYear,
				"month":                 CountGroupMonth,
				"week":                  CountGroupWeek,
				"day":                   CountGroupDay,
				"country":               CountGroupCountry,
				"state":                 CountGroupState,
				"city":                  CountGroupCity,
				"alertType":             CountGroupAlertType,
				"eventTypeGroup":        CountGroupEventTypeGroup,
				"status":                CountGroupStatus,
				"eventType":             CountGroupEventType,
				"category":              CountGroupCategory,
				"predictedAttendance":   CountGroupPredictedAttendance,
				"inboundEstimate":       CountGroupInboundEstimate,
				"internationalEstimate": CountGroupInternationalEstimate,
				"eventCount":            CountGroupEventCount,
			}

			var invalidOptions []string
			for _, item := range groupByItems {
				item = strings.TrimSpace(item)
				if item != "" {
					if countGroup, exists := validCountGroups[item]; exists {
						f.ParsedGroupBy = append(f.ParsedGroupBy, countGroup)
					} else {
						invalidOptions = append(invalidOptions, item)
					}
				}
			}

			if len(invalidOptions) > 0 {
				validOptions := []string{"year", "month", "week", "day", "country", "state", "city", "alertType", "eventTypeGroup", "status", "eventType", "category", "predictedAttendance", "inboundEstimate", "internationalEstimate", "eventCount"}
				return validation.NewError("invalid_groupBy", "Invalid groupBy option: "+strings.Join(invalidOptions, ", ")+". Valid options are: "+strings.Join(validOptions, ", "))
			}

			return nil
		}))),

		validation.Field(&f.GetNew, validation.By(func(value interface{}) error {
			if f.GetNew != nil {
				f.ParsedGetNew = f.GetNew
			}
			return nil
		})),

		validation.Field(&f.EventGroupCount, validation.When(f.EventGroupCount != "", validation.By(func(value interface{}) error {
			eventGroupCountStr := value.(string)
			eventGroupCountStr = strings.ToLower(strings.TrimSpace(eventGroupCountStr))
			switch eventGroupCountStr {
			case "true":
				parsedValue := true
				f.ParsedEventGroupCount = &parsedValue
			case "false":
				parsedValue := false
				f.ParsedEventGroupCount = &parsedValue
			default:
				return validation.NewError("invalid_event_group_count", "Invalid eventGroupCount value. Must be 'true' or 'false'")
			}
			return nil
		}))),

		validation.Field(&f.Frequency, validation.When(f.Frequency != "", validation.By(func(value interface{}) error {
			frequencyStr := value.(string)
			if frequencyStr == "" {
				return nil
			}

			frequencyMap := map[string]string{
				"weekly":       "Weekly",
				"monthly":      "Monthly",
				"quarterly":    "Quarterly",
				"bi-annual":    "Bi-annual",
				"annual":       "Annual",
				"biennial":     "Biennial",
				"triennial":    "Triennial",
				"quinquennial": "Quinquennial",
				"one-time":     "One-time",
				"quadrennial":  "Quadrennial",
			}

			frequencies := strings.Split(frequencyStr, ",")
			f.ParsedFrequency = make([]string, 0, len(frequencies))

			for _, freq := range frequencies {
				freq = strings.TrimSpace(freq)
				if freq == "" {
					continue
				}

				frequencyLower := strings.ToLower(freq)
				if properCase, exists := frequencyMap[frequencyLower]; exists {
					f.ParsedFrequency = append(f.ParsedFrequency, properCase)
				} else {
					validValues := []string{"Weekly", "Monthly", "Quarterly", "Bi-annual", "Annual", "Biennial", "Triennial", "Quinquennial", "One-time", "Quadrennial"}
					return validation.NewError("invalid_frequency", "Invalid frequency value: "+freq+". Valid values are: "+strings.Join(validValues, ", "))
				}
			}

			if len(f.ParsedFrequency) == 0 {
				return validation.NewError("empty_frequency", "frequency cannot be empty after parsing")
			}

			return nil
		}))),

		validation.Field(&f.Visibility, validation.By(func(value interface{}) error {
			visibilityStr := value.(string)
			validVisibilities := map[string]bool{
				"open":    true,
				"private": true,
				"draft":   true,
			}

			f.ParsedVisibility = []string{"open"}

			if visibilityStr != "" {
				visibilities := strings.Split(visibilityStr, ",")
				visibilitySet := map[string]bool{"open": true} // Track to avoid duplicates

				for _, visibility := range visibilities {
					visibility = strings.TrimSpace(strings.ToLower(visibility))
					if visibility != "" {
						if !validVisibilities[visibility] {
							return validation.NewError("invalid_visibility", "Invalid visibility value: "+visibility+". Valid values are: open, private, draft")
						}
						if !visibilitySet[visibility] {
							f.ParsedVisibility = append(f.ParsedVisibility, visibility)
							visibilitySet[visibility] = true
						}
					}
				}
			}
			return nil
		})), // Visibility validation

		validation.Field(&f.Mode, validation.When(f.Mode != "", validation.By(func(value interface{}) error {
			modeStr := value.(string)
			if modeStr == "" {
				return nil
			}

			modes := strings.Split(modeStr, ",")
			f.ParsedMode = make([]string, 0, len(modes))
			validModes := map[string]string{
				"online":    "ONLINE",
				"in_person": "OFFLINE",
				"offline":   "OFFLINE",
				"hybrid":    "HYBRID",
			}

			var invalidModes []string
			seenModes := make(map[string]bool)
			for _, mode := range modes {
				mode = strings.TrimSpace(strings.ToLower(mode))
				if mode != "" {
					if dbValue, exists := validModes[mode]; exists {
						if !seenModes[dbValue] {
							f.ParsedMode = append(f.ParsedMode, dbValue)
							seenModes[dbValue] = true
						}
					} else {
						invalidModes = append(invalidModes, mode)
					}
				}
			}

			if len(invalidModes) > 0 {
				validOptions := []string{"online", "in_person", "offline", "hybrid"}
				return validation.NewError("invalid_mode", "Invalid mode value(s): "+strings.Join(invalidModes, ", ")+". Valid values are: "+strings.Join(validOptions, ", "))
			}

			if len(f.ParsedMode) == 0 {
				return validation.NewError("empty_mode", "mode cannot be empty after parsing")
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

		validation.Field(&f.EventAudience, validation.When(f.EventAudience != "", validation.By(func(value interface{}) error {
			eventAudienceStr := value.(string)
			eventAudiences := strings.Split(eventAudienceStr, ",")
			f.ParsedEventAudience = make([]int, 0)

			validAudiences := map[string]int{
				"b2b": 11000,
				"b2c": 10100,
			}

			var audienceValues []int

			for _, eventAudience := range eventAudiences {
				eventAudience = strings.TrimSpace(strings.ToLower(eventAudience))
				if eventAudience == "" {
					continue
				}

				if val, exists := validAudiences[eventAudience]; exists {
					audienceValues = append(audienceValues, val)
				} else {
					validOptions := []string{"B2B", "B2C"}
					return validation.NewError("invalid_event_audience", "Invalid eventAudience value: "+eventAudience+". Valid options are: "+strings.Join(validOptions, ", "))
				}
			}

			if len(audienceValues) > 0 {
				seen := make(map[int]bool)
				for _, val := range audienceValues {
					if !seen[val] {
						f.ParsedEventAudience = append(f.ParsedEventAudience, val)
						seen[val] = true
					}
				}
			}

			return nil
		}))),

		validation.Field(&f.Regions, validation.When(f.Regions != "", validation.By(func(value interface{}) error {
			regionsStr := value.(string)
			regions := strings.Split(regionsStr, ",")
			f.ParsedRegions = make([]string, 0, len(regions))
			for _, region := range regions {
				region = strings.TrimSpace(region)
				if region != "" {
					f.ParsedRegions = append(f.ParsedRegions, region)
				}
			}
			return nil
		}))),

		validation.Field(&f.EstimatedVisitors, validation.When(f.EstimatedVisitors != "", validation.By(func(value interface{}) error {
			estimatedVisitorsStr := value.(string)
			if estimatedVisitorsStr == "" {
				return nil
			}

			estimatedVisitors := strings.Split(estimatedVisitorsStr, ",")
			f.ParsedEstimatedVisitors = make([]string, 0, len(estimatedVisitors))

			validValues := map[string]string{
				"nano":   "Nano",
				"micro":  "Micro",
				"small":  "Small",
				"medium": "Medium",
				"large":  "Large",
				"mega":   "Mega",
				"ultra":  "Ultra",
			}

			var invalidValues []string
			seenValues := make(map[string]bool)
			for _, ev := range estimatedVisitors {
				ev = strings.TrimSpace(ev)
				if ev != "" {
					evLower := strings.ToLower(ev)
					if normalizedValue, exists := validValues[evLower]; exists {
						if !seenValues[normalizedValue] {
							f.ParsedEstimatedVisitors = append(f.ParsedEstimatedVisitors, normalizedValue)
							seenValues[normalizedValue] = true
						}
					} else {
						invalidValues = append(invalidValues, ev)
					}
				}
			}

			if len(invalidValues) > 0 {
				validOptions := []string{"Nano", "Micro", "Small", "Medium", "Large", "Mega", "Ultra"}
				return validation.NewError("invalid_estimated_visitors", "Invalid estimatedVisitors value(s): "+strings.Join(invalidValues, ", ")+". Valid values are: "+strings.Join(validOptions, ", "))
			}

			if len(f.ParsedEstimatedVisitors) == 0 {
				return validation.NewError("empty_estimated_visitors", "estimatedVisitors cannot be empty after parsing")
			}

			return nil
		}))), // EstimatedVisitors validation

		validation.Field(&f.EstimatedExhibitors, validation.When(f.EstimatedExhibitors != "", validation.By(func(value interface{}) error {
			estimatedExhibitorsStr := value.(string)
			if estimatedExhibitorsStr == "" {
				return nil
			}

			estimatedExhibitors := strings.Split(estimatedExhibitorsStr, ",")
			f.ParsedEstimatedExhibitors = make([]string, 0, len(estimatedExhibitors))

			predefinedValues := map[string]bool{
				"0-100":    true,
				"100-500":  true,
				"500-1000": true,
				"1000":     true,
			}

			var invalidValues []string
			seenValues := make(map[string]bool)
			for _, ee := range estimatedExhibitors {
				ee = strings.TrimSpace(ee)
				if ee != "" {
					if predefinedValues[ee] {
						if !seenValues[ee] {
							f.ParsedEstimatedExhibitors = append(f.ParsedEstimatedExhibitors, ee)
							seenValues[ee] = true
						}
					} else {
						invalidValues = append(invalidValues, ee)
					}
				}
			}

			if len(invalidValues) > 0 {
				return validation.NewError("invalid_estimated_exhibitors", "Invalid estimatedExhibitors value(s): "+strings.Join(invalidValues, ", ")+". Valid values are: 0-100, 100-500, 500-1000, 1000")
			}

			if len(f.ParsedEstimatedExhibitors) == 0 {
				return validation.NewError("empty_estimated_exhibitors", "estimatedExhibitors cannot be empty after parsing")
			}

			return nil
		}))), // EstimatedExhibitors validation

		validation.Field(&f.Maturity, validation.When(f.Maturity != "", validation.By(func(value interface{}) error {
			maturityStr := value.(string)
			if maturityStr == "" {
				return nil
			}

			maturities := strings.Split(maturityStr, ",")
			f.ParsedMaturity = make([]string, 0, len(maturities))
			validMaturities := map[string]string{
				"new":         "new",
				"growing":     "growing",
				"established": "established",
				"flagship":    "flagship",
			}

			var invalidMaturities []string
			seenMaturities := make(map[string]bool)
			for _, maturity := range maturities {
				maturity = strings.TrimSpace(maturity)
				if maturity != "" {
					maturityLower := strings.ToLower(maturity)
					if normalizedValue, exists := validMaturities[maturityLower]; exists {
						if !seenMaturities[normalizedValue] {
							f.ParsedMaturity = append(f.ParsedMaturity, normalizedValue)
							seenMaturities[normalizedValue] = true
						}
					} else {
						invalidMaturities = append(invalidMaturities, maturity)
					}
				}
			}

			if len(invalidMaturities) > 0 {
				validOptions := []string{"new", "growing", "established", "flagship"}
				return validation.NewError("invalid_maturity", "Invalid maturity value(s): "+strings.Join(invalidMaturities, ", ")+". Valid values are: "+strings.Join(validOptions, ", "))
			}

			if len(f.ParsedMaturity) == 0 {
				return validation.NewError("empty_maturity", "maturity cannot be empty after parsing")
			}

			return nil
		}))), // Maturity validation

		validation.Field(&f.Status, validation.When(f.Status != "", validation.By(func(value interface{}) error {
			statusStr := value.(string)
			statuses := strings.Split(statusStr, ",")
			f.ParsedStatus = make([]string, 0, len(statuses))
			validStatuses := map[string]string{
				"active":     "A",
				"cancelled":  "C",
				"postponed":  "P",
				"predicted":  "R",
				"unverified": "U",
			}
			for _, status := range statuses {
				status = strings.TrimSpace(strings.ToLower(status))
				if status != "" {
					if dbCode, exists := validStatuses[status]; exists {
						f.ParsedStatus = append(f.ParsedStatus, dbCode)
					} else {
						return validation.NewError("invalid_status", "Invalid status value: "+status+". Valid values are: active, cancelled, postponed, predicted")
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

		// validation.Field(&f.DesignationIds, validation.When(f.DesignationIds != "", validation.By(func(value interface{}) error {
		// 	designationIdStr := value.(string)
		// 	designationIds := strings.Split(designationIdStr, ",")
		// 	f.ParsedDesignationId = make([]string, 0, len(designationIds))
		// 	for _, designationId := range designationIds {
		// 		designationId = strings.TrimSpace(designationId)
		// 		if designationId != "" {
		// 			f.ParsedDesignationId = append(f.ParsedDesignationId, designationId)
		// 		}
		// 	}
		// 	return nil
		// }))),

		// validation.Field(&f.SeniorityIds, validation.When(f.SeniorityIds != "", validation.By(func(value interface{}) error {
		// 	seniorityIdStr := value.(string)
		// 	seniorityIds := strings.Split(seniorityIdStr, ",")
		// 	f.ParsedSeniorityId = make([]string, 0, len(seniorityIds))
		// 	for _, seniorityId := range seniorityIds {
		// 		seniorityId = strings.TrimSpace(seniorityId)
		// 		if seniorityId != "" {
		// 			f.ParsedSeniorityId = append(f.ParsedSeniorityId, seniorityId)
		// 		}
		// 	}
		// 	return nil
		// }))),

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

		validation.Field(&f.EventTypes, validation.When(f.EventTypes != "", validation.By(func(value interface{}) error {
			eventTypesStr := value.(string)
			eventTypes := strings.Split(eventTypesStr, ",")
			f.ParsedEventTypes = make([]string, 0, len(eventTypes))
			for _, et := range eventTypes {
				et = strings.TrimSpace(et)
				if et != "" {
					matched, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, et)
					if !matched {
						return validation.NewError("invalid_event_type_uuid", "Invalid event type UUID format: "+et)
					}
					f.ParsedEventTypes = append(f.ParsedEventTypes, et)
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
			keywords := strings.Split(value.(string), ",")
			var include, exclude, includeForQuery, excludeForQuery []string
			for _, kw := range keywords {
				kw = strings.TrimSpace(kw)
				if kw == "" {
					continue
				}
				isExclude := strings.HasPrefix(kw, "-")
				raw := strings.TrimSpace(strings.TrimPrefix(kw, "-"))
				if raw == "" {
					continue
				}
				cleaned := cleanKeywordForQuery(raw)
				if cleaned == "" {
					continue
				}
				display := strings.ReplaceAll(raw, "_", " ")
				if isExclude {
					exclude = append(exclude, display)
					excludeForQuery = append(excludeForQuery, cleaned)
				} else {
					include = append(include, display)
					includeForQuery = append(includeForQuery, cleaned)
				}
			}
			if len(include) > 0 || len(exclude) > 0 {
				f.ParsedKeywords = &Keywords{
					Include:         include,
					Exclude:         exclude,
					IncludeForQuery: includeForQuery,
					ExcludeForQuery: excludeForQuery,
				}
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

		validation.Field(&f.IsSeries, validation.When(f.IsSeries != "", validation.By(func(value interface{}) error {
			isSeriesStr := value.(string)
			val, err := strconv.ParseBool(isSeriesStr)
			if err != nil {
				return validation.NewError("invalid_bool", "IsSeries must be a valid boolean value")
			}
			f.ParsedIsSeries = &val
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

		validation.Field(&f.PastBetween, validation.When(f.PastBetween != "", validation.By(func(value interface{}) error {
			pastBetweenStr := value.(string)
			if pastBetweenStr == "" {
				return nil
			}

			dates := strings.Split(pastBetweenStr, ",")
			if len(dates) != 2 {
				return validation.NewError("invalid_past_between", "pastBetween must contain exactly 2 dates separated by comma (start,end)")
			}

			var startDate, endDate string

			startStr := strings.TrimSpace(dates[0])
			if startStr == "" {
				return validation.NewError("invalid_past_between_start", "Start date cannot be empty")
			}
			parsedStart, err := time.Parse("2006-01-02", startStr)
			if err != nil {
				parsedStart, err = time.Parse(time.RFC3339, startStr)
				if err != nil {
					return validation.NewError("invalid_past_between_start_format", "Start date must be in YYYY-MM-DD format: "+startStr)
				}
			}
			startDate = parsedStart.Format("2006-01-02")

			endStr := strings.TrimSpace(dates[1])
			if endStr == "" {
				return validation.NewError("invalid_past_between_end", "End date cannot be empty")
			}
			parsedEnd, err := time.Parse("2006-01-02", endStr)
			if err != nil {
				parsedEnd, err = time.Parse(time.RFC3339, endStr)
				if err != nil {
					return validation.NewError("invalid_past_between_end_format", "End date must be in YYYY-MM-DD format: "+endStr)
				}
			}
			endDate = parsedEnd.Format("2006-01-02")

			f.ParsedPastBetween = &struct {
				Start string `json:"start"`
				End   string `json:"end"`
			}{
				Start: startDate,
				End:   endDate,
			}

			return nil
		}))),

		validation.Field(&f.ActiveBetween, validation.When(f.ActiveBetween != "", validation.By(func(value interface{}) error {
			activeBetweenStr := value.(string)
			if activeBetweenStr == "" {
				return nil
			}

			dates := strings.Split(activeBetweenStr, ",")
			if len(dates) != 2 {
				return validation.NewError("invalid_active_between", "activeBetween must contain exactly 2 dates separated by comma (start,end)")
			}

			var startDate, endDate string

			startStr := strings.TrimSpace(dates[0])
			if startStr == "" {
				return validation.NewError("invalid_active_between_start", "Start date cannot be empty")
			}
			parsedStart, err := time.Parse("2006-01-02", startStr)
			if err != nil {
				parsedStart, err = time.Parse(time.RFC3339, startStr)
				if err != nil {
					return validation.NewError("invalid_active_between_start_format", "Start date must be in YYYY-MM-DD format: "+startStr)
				}
			}
			startDate = parsedStart.Format("2006-01-02")

			endStr := strings.TrimSpace(dates[1])
			if endStr == "" {
				return validation.NewError("invalid_active_between_end", "End date cannot be empty")
			}
			parsedEnd, err := time.Parse("2006-01-02", endStr)
			if err != nil {
				parsedEnd, err = time.Parse(time.RFC3339, endStr)
				if err != nil {
					return validation.NewError("invalid_active_between_end_format", "End date must be in YYYY-MM-DD format: "+endStr)
				}
			}
			endDate = parsedEnd.Format("2006-01-02")

			f.ParsedActiveBetween = &struct {
				Start string `json:"start"`
				End   string `json:"end"`
			}{
				Start: startDate,
				End:   endDate,
			}

			return nil
		}))),

		validation.Field(&f.Forecasted, validation.When(f.Forecasted != "", validation.In("only", "included").Error("forecasted must be either 'only' or 'included'"))),

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
			jobCompositeStr = strings.TrimSpace(jobCompositeStr)

			if strings.HasPrefix(jobCompositeStr, "{") {
				var jobCompositeFilter JobCompositeFilter
				if err := json.Unmarshal([]byte(jobCompositeStr), &jobCompositeFilter); err != nil {
					return validation.NewError("invalid_job_composite_json", "Invalid jobComposite JSON format: "+err.Error())
				}

				if jobCompositeFilter.Logic != "" && jobCompositeFilter.Logic != "AND" && jobCompositeFilter.Logic != "OR" {
					return validation.NewError("invalid_job_composite_logic", "Invalid logic value. Must be 'AND' or 'OR'")
				}

				if jobCompositeFilter.Logic == "" {
					jobCompositeFilter.Logic = "AND"
				}

				if jobCompositeFilter.Conditions != nil {
					if jobCompositeFilter.Conditions.Logic != "" && jobCompositeFilter.Conditions.Logic != "AND" && jobCompositeFilter.Conditions.Logic != "OR" {
						return validation.NewError("invalid_job_composite_conditions_logic", "Invalid conditions logic value. Must be 'AND' or 'OR'")
					}

					hasDepartmentOrSeniority := len(jobCompositeFilter.Conditions.DepartmentIds) > 0 || len(jobCompositeFilter.Conditions.SeniorityIds) > 0
					if hasDepartmentOrSeniority && jobCompositeFilter.Conditions.Logic == "" {
						return validation.NewError("missing_job_composite_conditions_logic", "logic is required when departmentIds or seniorityIds is provided in conditions")
					}

					if jobCompositeFilter.Conditions.Logic == "" && hasDepartmentOrSeniority {
						jobCompositeFilter.Conditions.Logic = "AND"
					}
				}

				f.ParsedJobCompositeFilter = &jobCompositeFilter
			} else {
				// Parse as simple comma-separated string (backward compatibility)
				jobComposites := strings.Split(jobCompositeStr, ",")
				f.ParsedJobComposite = make([]string, 0, len(jobComposites))
				for _, jobComposite := range jobComposites {
					jobComposite = strings.TrimSpace(jobComposite)
					if jobComposite != "" {
						f.ParsedJobComposite = append(f.ParsedJobComposite, jobComposite)
					}
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
				"4": true,
			}
			for _, published := range publishedValues {
				published = strings.TrimSpace(published)
				if published != "" {
					if !validPublishedValues[published] {
						return validation.NewError("invalid_published", "Invalid published value: "+published+". Valid values are: 1, 2, 4")
					}
					f.ParsedPublished = append(f.ParsedPublished, published)
				}
			}
			return nil
		}))),

		validation.Field(&f.EditionType, validation.By(func(value interface{}) error {
			editionTypeStr := value.(string)
			if editionTypeStr == "" {
				editionTypeStr = "current"
			}
			editionTypes := strings.Split(editionTypeStr, ",")
			f.ParsedEditionType = make([]string, 0, len(editionTypes))
			validEditionTypes := map[string][]string{
				"all":     {"current_edition", "past_edition", "future_edition"},
				"current": {"current_edition"},
				"past":    {"past_edition"},
				"future":  {"future_edition"},
			}
			seenDbValues := make(map[string]bool)
			for _, editionType := range editionTypes {
				editionType = strings.TrimSpace(strings.ToLower(editionType))
				if editionType != "" {
					if dbValues, exists := validEditionTypes[editionType]; exists {
						for _, dbValue := range dbValues {
							if !seenDbValues[dbValue] {
								f.ParsedEditionType = append(f.ParsedEditionType, dbValue)
								seenDbValues[dbValue] = true
							}
						}
					} else {
						return validation.NewError("invalid_edition_type", "Invalid editionType value: "+editionType+". Valid values are: all, current, past, future")
					}
				}
			}
			if len(f.ParsedEditionType) == 0 {
				f.ParsedEditionType = []string{"current_edition"}
			}
			return nil
		})),

		validation.Field(&f.EventTypeGroup, validation.When(f.EventTypeGroup != "", validation.By(func(value interface{}) error {
			eventTypeGroupStr := value.(string)
			eventTypeGroupLower := strings.ToLower(strings.TrimSpace(eventTypeGroupStr))
			log.Println("eventTypeGroupLower", eventTypeGroupLower)
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

			if len(viewBoundStr) >= 2 && viewBoundStr[0] == '"' && viewBoundStr[len(viewBoundStr)-1] == '"' {
				var jsonStr string
				if err := json.Unmarshal([]byte(viewBoundStr), &jsonStr); err == nil {
					viewBoundStr = jsonStr
				}
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
				if geoCoords.Radius == nil {
					defaultRadius := 10.0
					geoCoords.Radius = &defaultRadius
				}
				// if *geoCoords.Radius > 10 {
				// 	return validation.NewError("invalid_radius", "Radius must be <= 10")
				// }
				if viewBound.Unit == "" {
					viewBound.Unit = "km"
				}
				validUnits := map[string]bool{
					"km": true,
					"mi": true,
					"ft": true,
				}
				if !validUnits[viewBound.Unit] {
					return validation.NewError("invalid_unit", "Invalid unit: "+viewBound.Unit+". Valid options are: km, mi, ft")
				}
				updatedCoords, err := json.Marshal(geoCoords)
				if err != nil {
					return validation.NewError("invalid_coordinates", "Failed to update coordinates: "+err.Error())
				}
				viewBound.Coordinates = updatedCoords
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

		validation.Field(&f.ViewBounds, validation.When(f.ViewBounds != "", validation.By(func(value interface{}) error {
			viewBoundsStr := value.(string)
			viewBoundsStr = strings.TrimSpace(viewBoundsStr)
			if viewBoundsStr == "" {
				return nil
			}

			if len(viewBoundsStr) >= 2 && viewBoundsStr[0] == '"' && viewBoundsStr[len(viewBoundsStr)-1] == '"' {
				var jsonStr string
				if err := json.Unmarshal([]byte(viewBoundsStr), &jsonStr); err == nil {
					viewBoundsStr = jsonStr
				}
			}

			var viewBoundsArray []ViewBound

			if strings.Contains(viewBoundsStr, "<sep>") {
				parts := strings.Split(viewBoundsStr, "<sep>")
				viewBoundsArray = make([]ViewBound, 0, len(parts))
				for i, part := range parts {
					part = strings.TrimSpace(part)
					if part == "" {
						continue
					}
					var viewBound ViewBound
					if err := json.Unmarshal([]byte(part), &viewBound); err != nil {
						return validation.NewError("invalid_view_bounds", fmt.Sprintf("Invalid viewBounds JSON object at index %d: %s", i, err.Error()))
					}
					viewBoundsArray = append(viewBoundsArray, viewBound)
				}
			} else {
				// Try to parse as JSON array (standard format)
				if err := json.Unmarshal([]byte(viewBoundsStr), &viewBoundsArray); err != nil {
					return validation.NewError("invalid_view_bounds", "Invalid viewBounds JSON array: "+err.Error())
				}
			}

			if len(viewBoundsArray) == 0 {
				return validation.NewError("empty_view_bounds", "viewBounds array cannot be empty")
			}

			f.ParsedViewBounds = make([]*ViewBound, 0, len(viewBoundsArray))

			for i, viewBound := range viewBoundsArray {
				boundTypeLower := strings.ToLower(strings.TrimSpace(string(viewBound.BoundType)))
				validBoundTypes := map[string]BoundType{
					"point": BoundTypePoint,
					"box":   BoundTypeBox,
				}

				boundType, exists := validBoundTypes[boundTypeLower]
				if !exists {
					return validation.NewError("invalid_bound_type", fmt.Sprintf("Invalid boundType at index %d: %s. Valid options are: point, box", i, string(viewBound.BoundType)))
				}
				viewBound.BoundType = boundType

				switch boundType {
				case BoundTypePoint:
					var geoCoords GeoCoordinates
					if err := json.Unmarshal(viewBound.Coordinates, &geoCoords); err != nil {
						return validation.NewError("invalid_coordinates", fmt.Sprintf("Invalid coordinates for point type at index %d: %s", i, err.Error()))
					}
					if geoCoords.Latitude < -90 || geoCoords.Latitude > 90 {
						return validation.NewError("invalid_latitude", fmt.Sprintf("Latitude must be between -90 and 90 at index %d", i))
					}
					if geoCoords.Longitude < -180 || geoCoords.Longitude > 180 {
						return validation.NewError("invalid_longitude", fmt.Sprintf("Longitude must be between -180 and 180 at index %d", i))
					}
					if geoCoords.Radius == nil {
						return validation.NewError("missing_radius", fmt.Sprintf("Radius is required for point type at index %d", i))
					}
					if *geoCoords.Radius <= 0 {
						return validation.NewError("invalid_radius", fmt.Sprintf("Radius must be greater than 0 at index %d", i))
					}
					if viewBound.Unit == "" {
						viewBound.Unit = "km"
					}
					validUnits := map[string]bool{
						"km": true,
						"mi": true,
						"ft": true,
					}
					if !validUnits[viewBound.Unit] {
						return validation.NewError("invalid_unit", fmt.Sprintf("Invalid unit at index %d: %s. Valid options are: km, mi, ft", i, viewBound.Unit))
					}
					updatedCoords, err := json.Marshal(geoCoords)
					if err != nil {
						return validation.NewError("invalid_coordinates", fmt.Sprintf("Failed to update coordinates at index %d: %s", i, err.Error()))
					}
					viewBound.Coordinates = updatedCoords
				// case BoundTypeBox:
				// 	var boxCoords []float64
				// 	if err := json.Unmarshal(viewBound.Coordinates, &boxCoords); err != nil {
				// 		return validation.NewError("invalid_coordinates", fmt.Sprintf("Invalid coordinates for box type at index %d: %s", i, err.Error()))
				// 	}
				// 	if len(boxCoords) != 4 {
				// 		return validation.NewError("invalid_box_coordinates", fmt.Sprintf("Box coordinates at index %d must have exactly 4 numbers [minLng, minLat, maxLng, maxLat]", i))
				// 	}
				// 	// Validate box coordinates: minLng, minLat, maxLng, maxLat
				// 	minLng, minLat, maxLng, maxLat := boxCoords[0], boxCoords[1], boxCoords[2], boxCoords[3]
				// 	if minLng < -180 || minLng > 180 || maxLng < -180 || maxLng > 180 {
				// 		return validation.NewError("invalid_box_longitude", fmt.Sprintf("Longitude values at index %d must be between -180 and 180", i))
				// 	}
				// 	if minLat < -90 || minLat > 90 || maxLat < -90 || maxLat > 90 {
				// 		return validation.NewError("invalid_box_latitude", fmt.Sprintf("Latitude values at index %d must be between -90 and 90", i))
				// 	}
				// 	if minLng >= maxLng {
				// 		return validation.NewError("invalid_box_range", fmt.Sprintf("minLng must be less than maxLng at index %d", i))
				// 	}
				// 	if minLat >= maxLat {
				// 		return validation.NewError("invalid_box_range", fmt.Sprintf("minLat must be less than maxLat at index %d", i))
				// 	}
				default:
					return validation.NewError("invalid_bound_type", fmt.Sprintf("Invalid boundType at index %d: %s. Valid options are: point, box", i, string(viewBound.BoundType)))
				}

				f.ParsedViewBounds = append(f.ParsedViewBounds, &viewBound)
			}

			return nil
		}))),

		validation.Field(&f.DateView, validation.When(f.View == "trends", validation.Required.Error("dateView is required for trends view"), validation.By(func(value interface{}) error {
			dateViewStr := value.(string)
			dateViewLower := strings.ToLower(strings.TrimSpace(dateViewStr))
			validDateViews := map[string]bool{
				"day":   true,
				"week":  true,
				"month": true,
				"year":  true,
			}
			if validDateViews[dateViewLower] {
				f.ParsedDateView = &dateViewLower
				f.DateView = dateViewLower
			} else {
				validOptions := []string{"day", "week", "month", "year"}
				return validation.NewError("invalid_dateView", "Invalid dateView value: "+dateViewStr+". Valid options are: "+strings.Join(validOptions, ", "))
			}
			return nil
		}))),

		validation.Field(&f.Columns, validation.When(f.View == "trends", validation.Required.Error("columns is required for trends view"), validation.By(func(value interface{}) error {
			columnsStr := value.(string)
			if columnsStr == "" {
				return validation.NewError("columns_required", "columns is required for trends view")
			}

			columns := strings.Split(columnsStr, ",")
			f.ParsedColumns = make([]string, 0, len(columns))
			validColumns := map[string]bool{
				"impactScore":           true,
				"predictedAttendance":   true,
				"economicImpact":        true,
				"eventCount":            true,
				"inboundEstimate":       true,
				"internationalEstimate": true,
				"hotel":                 true,
				"food":                  true,
				"entertainment":         true,
				"airline":               true,
				"transport":             true,
				"utilitie":              true,
			}

			var invalidOptions []string
			for _, col := range columns {
				col = strings.TrimSpace(col)
				if col != "" {
					if validColumns[col] {
						f.ParsedColumns = append(f.ParsedColumns, col)
					} else {
						invalidOptions = append(invalidOptions, col)
					}
				}
			}

			if len(f.ParsedColumns) == 0 {
				return validation.NewError("columns_empty", "At least one valid column is required for trends view")
			}

			if len(invalidOptions) > 0 {
				validOptions := []string{"impactScore", "predictedAttendance", "economicImpact", "eventCount", "inboundEstimate", "internationalEstimate", "hotel", "food", "entertainment", "airline", "transport", "utilitie"}
				return validation.NewError("invalid_column", "Invalid columns option: "+strings.Join(invalidOptions, ", ")+". Valid options are: "+strings.Join(validOptions, ", "))
			}

			return nil
		}))),

		validation.Field(&f.GroupByTrends, validation.When(f.View == "trends", validation.Required.Error("groupByTrends is required for trends view"), validation.By(func(value interface{}) error {
			groupByTrendsStr := value.(string)
			groupByTrendsLower := strings.ToLower(strings.TrimSpace(groupByTrendsStr))
			if groupByTrendsLower == "date" {
				f.ParsedGroupByTrends = &groupByTrendsLower
				f.GroupByTrends = groupByTrendsLower
			} else {
				return validation.NewError("invalid_groupByTrends", "Invalid groupByTrends value: "+groupByTrendsStr+". Valid option is: date")
			}
			return nil
		}))),

		validation.Field(&f.Source, validation.When(f.View == "trends" && f.Source != "", validation.By(func(value interface{}) error {
			sourceStr := value.(string)
			sourceLower := strings.ToLower(strings.TrimSpace(sourceStr))
			validSources := map[string]bool{
				"geo": true,
				"gtm": true,
			}
			if validSources[sourceLower] {
				f.Source = sourceLower
			} else {
				return validation.NewError("invalid_source", "Invalid source value: "+sourceStr+". Valid options are: geo, gtm")
			}
			return nil
		}))),

		validation.Field(&f.UserId, validation.When(f.UserId != "", validation.By(func(value interface{}) error {
			userIdStr := value.(string)
			if userIdStr == "" {
				return nil
			}

			// Parse comma-separated user IDs
			userIds := strings.Split(userIdStr, ",")
			f.ParsedUserId = make([]string, 0, len(userIds))
			for _, userId := range userIds {
				userId = strings.TrimSpace(userId)
				if userId != "" {
					f.ParsedUserId = append(f.ParsedUserId, userId)
				}
			}

			if len(f.ParsedUserId) == 0 {
				return validation.NewError("empty_user_id", "userId cannot be empty after parsing")
			}

			return nil
		}))),

		validation.Field(&f.UserName, validation.When(f.UserName != "", validation.By(func(value interface{}) error {
			userNameStr := value.(string)
			if userNameStr == "" {
				return nil
			}

			userNames := strings.Split(userNameStr, ",")
			f.ParsedUserName = make([]string, 0, len(userNames))
			for _, userName := range userNames {
				userName = strings.TrimSpace(userName)
				if userName != "" {
					f.ParsedUserName = append(f.ParsedUserName, userName)
				}
			}

			if len(f.ParsedUserName) == 0 {
				return validation.NewError("empty_user_name", "userName cannot be empty after parsing")
			}

			// searchByEntity is required for userName
			if f.SearchByEntity == "" {
				return validation.NewError("entity_required", "searchByEntity is required when userName is provided")
			}

			searchByEntityLower := strings.ToLower(strings.TrimSpace(f.SearchByEntity))
			if searchByEntityLower != "user" && searchByEntityLower != "speaker" {
				return validation.NewError("invalid_entity_for_user_name", "searchByEntity must be 'user' or 'speaker' when userName is provided")
			}

			return nil
		}))),

		validation.Field(&f.UserCompanyName, validation.When(f.UserCompanyName != "", validation.By(func(value interface{}) error {
			userCompanyNameStr := value.(string)
			if userCompanyNameStr == "" {
				return nil
			}

			companyNames := strings.Split(userCompanyNameStr, ",")
			f.ParsedUserCompanyName = make([]string, 0, len(companyNames))
			for _, companyName := range companyNames {
				companyName = strings.TrimSpace(companyName)
				if companyName != "" {
					f.ParsedUserCompanyName = append(f.ParsedUserCompanyName, companyName)
				}
			}

			if len(f.ParsedUserCompanyName) == 0 {
				return validation.NewError("empty_user_company_name", "userCompanyName cannot be empty after parsing")
			}

			// searchByEntity is required for userCompanyName
			if f.SearchByEntity == "" {
				return validation.NewError("entity_required", "searchByEntity is required when userCompanyName is provided")
			}

			searchByEntityLower := strings.ToLower(strings.TrimSpace(f.SearchByEntity))
			if searchByEntityLower != "user" && searchByEntityLower != "speaker" {
				return validation.NewError("invalid_entity_for_user_company_name", "searchByEntity must be 'user' or 'speaker' when userCompanyName is provided")
			}

			return nil
		}))),

		validation.Field(&f.CompanyId, validation.When(f.CompanyId != "", validation.By(func(value interface{}) error {
			companyIdStr := value.(string)
			if companyIdStr == "" {
				return nil
			}

			companyIds := strings.Split(companyIdStr, ",")
			f.ParsedCompanyId = make([]string, 0, len(companyIds))
			for _, companyId := range companyIds {
				companyId = strings.TrimSpace(companyId)
				if companyId != "" {
					f.ParsedCompanyId = append(f.ParsedCompanyId, companyId)
				}
			}

			if len(f.ParsedCompanyId) == 0 {
				return validation.NewError("empty_company_id", "companyId cannot be empty after parsing")
			}

			return nil
		}))),

		validation.Field(&f.CompanyName, validation.When(f.CompanyName != "", validation.By(func(value interface{}) error {
			companyNameStr := value.(string)
			if companyNameStr == "" {
				return nil
			}

			companyNames := strings.Split(companyNameStr, ",")
			f.ParsedCompanyName = make([]string, 0, len(companyNames))
			for _, companyName := range companyNames {
				companyName = strings.TrimSpace(companyName)
				if companyName != "" {
					f.ParsedCompanyName = append(f.ParsedCompanyName, companyName)
				}
			}

			if len(f.ParsedCompanyName) == 0 {
				return validation.NewError("empty_company_name", "companyName cannot be empty after parsing")
			}

			// searchByEntity is required for companyName
			if f.SearchByEntity == "" {
				return validation.NewError("entity_required", "searchByEntity is required when companyName is provided")
			}

			return nil
		}))),

		validation.Field(&f.CompanyWebsite, validation.When(f.CompanyWebsite != "", validation.By(func(value interface{}) error {
			companyWebsiteStr := value.(string)
			if companyWebsiteStr == "" {
				return nil
			}

			companyWebsites := strings.Split(companyWebsiteStr, ",")
			f.ParsedCompanyWebsite = make([]string, 0, len(companyWebsites))
			for _, companyWebsite := range companyWebsites {
				companyWebsite = strings.TrimSpace(companyWebsite)
				if companyWebsite != "" {
					f.ParsedCompanyWebsite = append(f.ParsedCompanyWebsite, companyWebsite)
				}
			}

			if len(f.ParsedCompanyWebsite) == 0 {
				return validation.NewError("empty_company_website", "companyWebsite cannot be empty after parsing")
			}

			// searchByEntity is required for companyWebsite
			if f.SearchByEntity == "" {
				return validation.NewError("entity_required", "searchByEntity is required when companyWebsite is provided")
			}

			return nil
		}))),

		validation.Field(&f.SearchByEntity, validation.When(f.SearchByEntity != "", validation.By(func(value interface{}) error {
			searchByEntityStr := value.(string)
			if searchByEntityStr == "" {
				return nil
			}

			searchByEntityParts := strings.Split(searchByEntityStr, ",")
			validEntities := map[string]bool{
				"company":                      true,
				"user":                         true,
				"speaker":                      true,
				"visitor":                      true,
				"event":                        true,
				"keywords":                     true,
				"eventestimatecount":           true,
				"economicimpactbreakdowncount": true,
				"audience":                     true,
			}

			// Allow "event" when groupBy is used
			hasGroupBy := f.GroupBy != ""
			if hasGroupBy {
				validEntities["event"] = true
			}

			parsedValues := make([]string, 0, len(searchByEntityParts))
			var invalidValues []string
			for _, part := range searchByEntityParts {
				partLower := strings.ToLower(strings.TrimSpace(part))
				if partLower != "" {
					if validEntities[partLower] {
						exists := false
						for _, existing := range parsedValues {
							if existing == partLower {
								exists = true
								break
							}
						}
						if !exists {
							parsedValues = append(parsedValues, partLower)
						}
					} else {
						invalidValues = append(invalidValues, part)
					}
				}
			}

			if len(invalidValues) > 0 {
				validOptions := []string{"company", "user", "speaker", "visitor", "event", "keywords", "eventEstimateCount", "economicImpactBreakdownCount", "audience"}
				if hasGroupBy {
					validOptions = append(validOptions, "event")
				}
				return validation.NewError("invalid_search_by_entity", "Invalid searchByEntity value(s): "+strings.Join(invalidValues, ", ")+". Valid options are: "+strings.Join(validOptions, ", "))
			}

			if len(parsedValues) == 0 {
				return validation.NewError("empty_search_by_entity", "searchByEntity cannot be empty after parsing")
			}

			f.ParsedSearchByEntity = parsedValues

			if f.AdvanceSearchBy == "" {
				var advanceSearchByParts []string
				for _, searchByEntityVal := range parsedValues {
					switch searchByEntityVal {
					case "company":
						advanceSearchByParts = append(advanceSearchByParts, "exhibitor", "sponsor", "organizer")
					case "user":
						advanceSearchByParts = append(advanceSearchByParts, "speaker", "visitor")
					case "speaker":
						advanceSearchByParts = append(advanceSearchByParts, "speaker")
					case "visitor":
						advanceSearchByParts = append(advanceSearchByParts, "visitor")
					case "event", "keywords", "eventestimatecount", "economicimpactbreakdowncount", "audience":
						// These don't map to advanceSearchBy
					}
				}

				if len(advanceSearchByParts) > 0 {
					uniqueParts := make(map[string]bool)
					for _, part := range advanceSearchByParts {
						uniqueParts[part] = true
					}
					finalParts := make([]string, 0, len(uniqueParts))
					for part := range uniqueParts {
						finalParts = append(finalParts, part)
					}
					f.AdvanceSearchBy = strings.Join(finalParts, ",")
				} else {
					f.AdvanceSearchBy = ""
				}

				if f.AdvanceSearchBy != "" {
					entities := strings.Split(f.AdvanceSearchBy, ",")
					f.ParsedAdvancedSearchBy = make([]string, 0, len(entities))
					for _, entity := range entities {
						entity = strings.TrimSpace(strings.ToLower(entity))
						if entity != "" {
							exists := false
							for _, existing := range f.ParsedAdvancedSearchBy {
								if existing == entity {
									exists = true
									break
								}
							}
							if !exists {
								f.ParsedAdvancedSearchBy = append(f.ParsedAdvancedSearchBy, entity)
							}
						}
					}
				} else {
					f.ParsedAdvancedSearchBy = []string{}
				}
			}

			return nil
		}))),

		validation.Field(&f.AdvanceSearchBy, validation.When(f.AdvanceSearchBy != "", validation.By(func(value interface{}) error {
			advancedSearchByStr := value.(string)
			if advancedSearchByStr == "" {
				return nil
			}

			entities := strings.Split(advancedSearchByStr, ",")
			f.ParsedAdvancedSearchBy = make([]string, 0, len(entities))
			validEntities := map[string]bool{
				"visitor":   true,
				"speaker":   true,
				"exhibitor": true,
				"sponsor":   true,
				"organizer": true,
			}

			var invalidEntities []string
			for _, entity := range entities {
				entity = strings.TrimSpace(strings.ToLower(entity))
				if entity != "" {
					if validEntities[entity] {
						exists := false
						for _, existing := range f.ParsedAdvancedSearchBy {
							if existing == entity {
								exists = true
								break
							}
						}
						if !exists {
							f.ParsedAdvancedSearchBy = append(f.ParsedAdvancedSearchBy, entity)
						}
					} else {
						invalidEntities = append(invalidEntities, entity)
					}
				}
			}

			if len(invalidEntities) > 0 {
				validOptions := []string{"visitor", "speaker", "exhibitor", "sponsor", "organizer"}
				return validation.NewError("invalid_advanced_search_by", "Invalid advancedSearchBy value(s): "+strings.Join(invalidEntities, ", ")+". Valid options are: "+strings.Join(validOptions, ", "))
			}

			if len(f.ParsedAdvancedSearchBy) == 0 {
				return validation.NewError("empty_advanced_search_by", "advancedSearchBy cannot be empty after parsing")
			}

			// Normalize AdvanceSearchBy to lowercase
			f.AdvanceSearchBy = strings.ToLower(strings.TrimSpace(advancedSearchByStr))

			return nil
		}))),
	)

	if err != nil {
		return err
	}

	if len(f.ParsedUserId) > 0 && len(f.ParsedAdvancedSearchBy) == 0 && f.SearchByEntity == "" {
		f.AdvanceSearchBy = "speaker,visitor"
		entities := strings.Split(f.AdvanceSearchBy, ",")
		f.ParsedAdvancedSearchBy = make([]string, 0, len(entities))
		for _, entity := range entities {
			entity = strings.TrimSpace(strings.ToLower(entity))
			if entity != "" {
				exists := false
				for _, existing := range f.ParsedAdvancedSearchBy {
					if existing == entity {
						exists = true
						break
					}
				}
				if !exists {
					f.ParsedAdvancedSearchBy = append(f.ParsedAdvancedSearchBy, entity)
				}
			}
		}
	}

	if len(f.ParsedCompanyId) > 0 && len(f.ParsedAdvancedSearchBy) == 0 && f.SearchByEntity == "" {
		f.AdvanceSearchBy = "exhibitor,sponsor,organizer"
		entities := strings.Split(f.AdvanceSearchBy, ",")
		f.ParsedAdvancedSearchBy = make([]string, 0, len(entities))
		for _, entity := range entities {
			entity = strings.TrimSpace(strings.ToLower(entity))
			if entity != "" {
				exists := false
				for _, existing := range f.ParsedAdvancedSearchBy {
					if existing == entity {
						exists = true
						break
					}
				}
				if !exists {
					f.ParsedAdvancedSearchBy = append(f.ParsedAdvancedSearchBy, entity)
				}
			}
		}
	}

	if f.ParsedEventTypeGroup != nil {
		if len(f.ParsedEventTypes) > 0 {
			filteredEventTypes := filterEventTypesByGroup(f.ParsedEventTypes, *f.ParsedEventTypeGroup)
			if len(filteredEventTypes) == 0 {
				f.ParsedEventTypes = nil
				f.ParsedEventIds = []string{"'" + NilEventUUID + "'"}
			} else {
				f.ParsedEventTypes = filteredEventTypes
			}
		} else {
			allEventTypes := GetAllEventTypeIDsByGroup(*f.ParsedEventTypeGroup)
			if len(allEventTypes) == 0 {
				return validation.NewError("no_event_types_for_group", "No event types found for the specified event type group")
			}
			f.ParsedEventTypes = allEventTypes
		}
	}

	f.applyEventTypeBasedMappings(wasPublishedExplicitlyProvided, wasEventAudienceExplicitlyProvided)

	return nil
}

type SearchEventsRequest struct {
	FilterDataDto
	PaginationDto
	ResponseDataDto
	// UserID       string        `json:"user_id"`
	APIID      string `json:"api_id" form:"api_id"`
	ShowValues string `json:"showValues,omitempty" form:"showValues"`
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
