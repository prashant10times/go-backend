package models

import (
	"regexp"
	"strconv"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

// ResponseDataDto represents the response data structure for event queries
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

type FilterDataDto struct {
	Q        string `json:"q,omitempty" form:"q"`
	Keywords string `json:"keywords,omitempty" form:"keywords"`
	Category string `json:"category,omitempty" form:"category"`
	City     string `json:"city,omitempty" form:"city"`
	State    string `json:"state,omitempty" form:"state"`
	Country  string `json:"country,omitempty" form:"country"`
	Products string `json:"products,omitempty" form:"products"`

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

	EndGte   string `json:"end.gte,omitempty" form:"end.gte"`
	EndLte   string `json:"end.lte,omitempty" form:"end.lte"`
	StartGte string `json:"start.gte,omitempty" form:"start.gte"`
	StartLte string `json:"start.lte,omitempty" form:"start.lte"`
	StartGt  string `json:"start.gt,omitempty" form:"start.gt"`
	EndGt    string `json:"end.gt,omitempty" form:"end.gt"`
	StartLt  string `json:"start.lt,omitempty" form:"start.lt"`
	EndLt    string `json:"end.lt,omitempty" form:"end.lt"`

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

	VenueLatitude  string `json:"venueLatitude,omitempty" form:"venueLatitude"`
	VenueLongitude string `json:"venueLongitude,omitempty" form:"venueLongitude"`

	ToAggregate string `json:"toAggregate,omitempty" form:"toAggregate"`

	EventRanking string `json:"eventRanking,omitempty" form:"eventRanking"`

	ParsedCategory       []string  `json:"-"`
	ParsedCity           []string  `json:"-"`
	ParsedCountry        []string  `json:"-"`
	ParsedProducts       []string  `json:"-"`
	ParsedType           []string  `json:"-"`
	ParsedVenue          []string  `json:"-"`
	ParsedCompany        []string  `json:"-"`
	ParsedView           []string  `json:"-"`
	ParsedToAggregate    []string  `json:"-"`
	ParsedKeywords       *Keywords `json:"-"`
	ParsedIsBranded      *bool     `json:"-"`
	ParsedMode           *string   `json:"-"`
	ParsedStatus         []string  `json:"-"`
	ParsedState          []string  `json:"-"`
	ParsedSpeakerState   []string  `json:"-"`
	ParsedExhibitorState []string  `json:"-"`
	ParsedSponsorState   []string  `json:"-"`
	ParsedVisitorState   []string  `json:"-"`
	ParsedEventRanking   []string  `json:"-"`
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
}

// Validate validates the FilterDataDto
func (f *FilterDataDto) Validate() error {
	f.SetDefaultValues()

	return validation.ValidateStruct(f,
		validation.Field(&f.Price, validation.When(f.Price != "", validation.In("free", "paid", "not_available", "free-paid"))),    // Price validation
		validation.Field(&f.AvgRating, validation.When(f.AvgRating != "", validation.In("1", "2", "3", "4", "5"))),                 // AvgRating validation
		validation.Field(&f.Unit, validation.When(f.Unit != "", validation.In("km", "mi", "ft"))),                                  // Unit validation
		validation.Field(&f.EventDistanceOrder, validation.When(f.EventDistanceOrder != "", validation.In("closest", "farthest"))), // EventDistanceOrder validation

		validation.Field(&f.View, validation.Required, validation.By(func(value interface{}) error {
			viewStr := value.(string)
			views := strings.Split(viewStr, ",")
			f.ParsedView = make([]string, 0, len(views))
			for _, view := range views {
				view = strings.TrimSpace(view)
				if view != "" {
					if view != "list" && view != "agg" {
						return validation.NewError("invalid_view", "Invalid view value: "+view+". Must be 'list' or 'agg'")
					}
					f.ParsedView = append(f.ParsedView, view)
				}
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
