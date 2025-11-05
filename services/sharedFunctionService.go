package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"search-event-go/models"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elliotchance/orderedmap"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type SharedFunctionService struct {
	db *gorm.DB
}
type OrderedJSONMap struct {
	Keys   []string
	Values map[string]interface{}
}

func (o OrderedJSONMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("{")

	for i, key := range o.Keys {
		if i > 0 {
			buf.WriteString(",")
		}

		keyBytes, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteString(":")

		valueBytes, err := json.Marshal(o.Values[key])
		if err != nil {
			return nil, err
		}
		buf.Write(valueBytes)
	}

	buf.WriteString("}")
	return buf.Bytes(), nil
}

func NewSharedFunctionService(db *gorm.DB) *SharedFunctionService {
	return &SharedFunctionService{
		db: db,
	}
}

func (s *SharedFunctionService) logApiUsage(userId, apiId, endpoint string, responseTime float64, ipAddress string, statusCode int, filterFields models.FilterDataDto, pagination models.PaginationDto, responseFields models.ResponseDataDto, errorMessage *string) error {

	payload := map[string]interface{}{
		"filterFields":   filterFields,
		"responseFields": responseFields,
		"pagination":     pagination,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal payload: %v", err)
		payloadJSON = nil
	}

	apiUsageLog := models.APIUsageLog{
		ID:              uuid.New().String(),
		UserID:          uuid.MustParse(userId).String(),
		APIID:           uuid.MustParse(apiId).String(),
		Endpoint:        endpoint,
		ErrorMessage:    errorMessage,
		Payload:         (*datatypes.JSON)(&payloadJSON),
		IPAddress:       ipAddress,
		StatusCode:      &statusCode,
		APIResponseTime: &responseTime,
	}

	err = s.db.Create(&apiUsageLog).Error
	if err != nil {
		err2 := s.db.Model(&models.UserAPIAccess{}).
			Where("user_id = ? AND api_id = ?", userId, apiId).
			Update("daily_limit", gorm.Expr("daily_limit + 1")).Error
		if err2 != nil {
			return err2
		}
		return err
	}

	return nil
}

type QuotaAndFilterResult struct {
	AllowedFilters            []string
	AllowedAdvancedParameters []string
}

func (s *SharedFunctionService) quotaAndFilterVerification(userId, apiId string) (*QuotaAndFilterResult, error) {
	var result QuotaAndFilterResult

	err := s.db.Transaction(func(tx *gorm.DB) error {

		var updated int64
		err := tx.Model(&models.UserAPIAccess{}).
			Where("user_id = ? AND api_id = ? AND daily_limit > 0", userId, apiId).
			Update("daily_limit", gorm.Expr("daily_limit - 1")).Count(&updated).Error
		if err != nil {
			return err
		}

		if updated == 0 {
			return gorm.ErrRecordNotFound
		}

		var basicFilters []models.APIFilter
		err = tx.Where("api_id = ? AND is_active = ? AND filter_type = ?", apiId, true, "BASIC").
			Select("filter_name").
			Find(&basicFilters).Error
		if err != nil {
			return err
		}

		var userFilterAccess []struct {
			HasAccess  bool   `gorm:"column:has_access"`
			FilterType string `gorm:"column:filter_type"`
			FilterName string `gorm:"column:filter_name"`
			IsPaid     bool   `gorm:"column:is_paid"`
		}

		err = tx.Model(&models.UserFilterAccess{}).
			Select("ufa.has_access, f.filter_type, f.filter_name, f.is_paid").
			Joins(`JOIN "ApiFilter" f ON ufa.filter_id = f.id`).
			Where("ufa.user_id = ? AND ufa.has_access = ? AND f.api_id = ? AND f.is_active = ? AND f.filter_type = ?",
				userId, true, apiId, true, "ADVANCED").
			Table(`"UserFilterAccess" AS ufa`).
			Scan(&userFilterAccess).Error
		if err != nil {
			return err
		}
		log.Printf("userFilterAccess: %v", userFilterAccess)

		var basicFilterNames []string
		for _, filter := range basicFilters {
			basicFilterNames = append(basicFilterNames, filter.FilterName)
		}
		var allowedAdvancedFilters []string
		for _, access := range userFilterAccess {
			if access.IsPaid {
				allowedAdvancedFilters = append(allowedAdvancedFilters, access.FilterName)
			}
		}
		log.Printf("allowedAdvancedFilters: %v", allowedAdvancedFilters)

		result.AllowedFilters = append(basicFilterNames, allowedAdvancedFilters...)

		var userParameterAccess []struct {
			HasAccess     bool   `gorm:"column:has_access"`
			ParameterName string `gorm:"column:parameter_name"`
			ParameterType string `gorm:"column:parameter_type"`
			IsPaid        bool   `gorm:"column:is_paid"`
		}
		err = tx.Model(&models.UserParameterAccess{}).
			Select("upa.has_access, p.parameter_name, p.parameter_type, p.is_paid").
			Joins(`JOIN "ApiParameter" p ON upa.parameter_id = p.id`).
			Where("upa.user_id = ? AND upa.has_access = ? AND p.api_id = ? AND p.is_active = ? AND p.parameter_type = ?",
				userId, true, apiId, true, "ADVANCED").
			Table(`"UserParameterAccess" AS upa`).
			Scan(&userParameterAccess).Error
		if err != nil {
			return err
		}

		for _, access := range userParameterAccess {
			if access.IsPaid {
				result.AllowedAdvancedParameters = append(result.AllowedAdvancedParameters, access.ParameterName)
			}
		}

		return nil
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &QuotaExceededError{Message: "Daily limit reached"}
		}
		return nil, err
	}

	return &result, nil
}

type QuotaExceededError struct {
	Message string
}

func (e *QuotaExceededError) Error() string {
	return e.Message
}

type SortClause struct {
	Field string `json:"field"`
	Order string `json:"order"`
}

func (s *SharedFunctionService) parseSortFields(sort string, filterFields models.FilterDataDto) ([]SortClause, error) {
	if sort == "" && filterFields.Q == "" {
		return []SortClause{
			{
				Field: "id",
				Order: "asc",
			},
		}, nil
	}

	sortFields := strings.Split(sort, ",")
	var sortClauses []SortClause

	for _, field := range sortFields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		isDescending := strings.HasPrefix(field, "-")
		cleanField := strings.TrimPrefix(field, "-")

		dbField, exists := models.SortFieldMap[cleanField]
		if !exists {
			return nil, &InvalidSortFieldError{Field: cleanField}
		}

		order := "asc"
		if isDescending {
			order = "desc"
		}

		sortClauses = append(sortClauses, SortClause{
			Field: dbField,
			Order: order,
		})
	}

	return sortClauses, nil
}

type InvalidSortFieldError struct {
	Field string
}

func (e *InvalidSortFieldError) Error() string {
	return "Invalid sort field: " + e.Field
}

func (s *SharedFunctionService) buildOrderByClause(sortClause []SortClause, needsAnyJoin bool) (string, error) {
	if len(sortClause) == 0 {
		return "", nil
	}

	var orderByParts []string
	for _, sort := range sortClause {
		if sort.Field == "" {
			continue
		}

		fieldName := models.SortFieldMap[sort.Field]
		if fieldName == "" {
			fieldName = sort.Field
		}

		if needsAnyJoin {
			fieldName = fmt.Sprintf("ee.%s", fieldName)
		}

		orderByPart := fmt.Sprintf("%s %s", fieldName, strings.ToUpper(sort.Order))
		orderByParts = append(orderByParts, orderByPart)
	}

	if len(orderByParts) > 0 {
		return fmt.Sprintf("ORDER BY %s", strings.Join(orderByParts, ", ")), nil
	}

	return "", nil
}

var fieldMapping = map[string]string{
	"types":                  "type",
	"start_date":             "start",
	"end_date":               "end",
	"event_name":             "name",
	"edition_city_name":      "city",
	"edition_country":        "country",
	"tags":                   "tags",
	"event_description":      "description",
	"event_logo":             "logo",
	"categories":             "categories",
	"event_avgRating":        "avgRating",
	"event_uuid":             "id",
	"event_followers":        "followers",
	"estimatedExhibitors":    "estimatedExhibitors",
	"exhibitors_lower_bound": "exhibitors_lower_bound",
	"exhibitors_upper_bound": "exhibitors_upper_bound",
	"audienceZone":           "audienceZone",
}

func (s *SharedFunctionService) renameClickHouseEventKeys(event map[string]interface{}) map[string]interface{} {
	renamed := make(map[string]interface{})
	for dbField, responseField := range fieldMapping {
		if val, exists := event[dbField]; exists {
			renamed[responseField] = val
		}
	}
	return renamed
}

func (s *SharedFunctionService) clickHouseResponseNameChange(eventData []map[string]interface{}) ([]map[string]interface{}, error) {
	var renamedData []map[string]interface{}

	for _, item := range eventData {
		renamed := s.renameClickHouseEventKeys(item)
		renamedData = append(renamedData, renamed)
	}

	return renamedData, nil
}

func (s *SharedFunctionService) BuildClickhouseListViewResponse(eventData []map[string]interface{}, pagination models.PaginationDto, c *fiber.Ctx) (interface{}, error) {
	response := fiber.Map{
		"count":    len(eventData),
		"next":     s.getPaginationURL(pagination.Limit, pagination.Offset, "next", c),
		"previous": s.getPaginationURL(pagination.Limit, pagination.Offset, "previous", c),
		"data":     eventData,
	}
	return response, nil
}

func (s *SharedFunctionService) getPaginationURL(limit, offset int, paginationType string, c *fiber.Ctx) *string {
	baseURL := fmt.Sprintf("%s://%s%s", c.Protocol(), c.Hostname(), c.Path())

	var newOffset int
	switch paginationType {
	case "next":
		newOffset = offset + limit
	case "previous":
		newOffset = offset - limit
		if newOffset < 0 {
			return nil
		}
	default:
		return nil
	}

	queryParams := make(map[string]string)
	c.Request().URI().QueryArgs().VisitAll(func(key, value []byte) {
		queryParams[string(key)] = string(value)
	})

	delete(queryParams, "limit")
	delete(queryParams, "offset")

	var queryParts []string
	for key, value := range queryParams {
		queryParts = append(queryParts, fmt.Sprintf("%s=%s", key, value))
	}
	queryParts = append(queryParts, fmt.Sprintf("limit=%d", limit))
	queryParts = append(queryParts, fmt.Sprintf("offset=%d", newOffset))

	queryString := strings.Join(queryParts, "&")
	url := fmt.Sprintf("%s?%s", baseURL, queryString)

	return &url
}

func (s *SharedFunctionService) determineQueryType(filterFields models.FilterDataDto) (string, error) {
	nonFilterKeys := map[string]bool{
		"Radius":             true,
		"Unit":               true,
		"View":               true,
		"EventDistanceOrder": true,
		"ToAggregate":        true,
	}

	hasActualFilters := false
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		fieldName := fieldType.Name

		if strings.HasPrefix(fieldName, "Parsed") {
			continue
		}
		if nonFilterKeys[fieldName] {
			continue
		}

		if field.Kind() == reflect.String {
			fieldValue := field.String()
			if fieldValue != "" {
				hasActualFilters = true
			}
		} else if field.Kind() == reflect.Bool {
			if field.Bool() {
				hasActualFilters = true
			}
		}
	}

	isAggregationView := strings.Contains(filterFields.View, "agg")

	log.Printf("hasActualFilters: %v, isAggregationView: %v, View: '%s'", hasActualFilters, isAggregationView, filterFields.View)

	if !hasActualFilters && !isAggregationView {
		log.Printf("Query type determined: DEFAULT_LIST")
		return "DEFAULT_LIST", nil
	} else if hasActualFilters && !isAggregationView {
		log.Printf("Query type determined: FILTERED_LIST")
		return "FILTERED_LIST", nil
	} else if isAggregationView {
		log.Printf("Query type determined: DEFAULT_AGGREGATION")
		return "DEFAULT_AGGREGATION", nil
	}

	log.Printf("Falling through to default return")
	return "DEFAULT_LIST", nil
}

type ClickHouseQueryResult struct {
	WhereClause                   string
	SearchClause                  string
	DistanceOrderClause           string
	NeedsVisitorJoin              bool
	NeedsSpeakerJoin              bool
	NeedsExhibitorJoin            bool
	NeedsSponsorJoin              bool
	NeedsAnyJoin                  bool
	NeedsCategoryJoin             bool
	NeedsTypeJoin                 bool
	NeedsEventRankingJoin         bool
	needsDesignationJoin          bool
	needsAudienceSpreadJoin       bool
	VisitorJoinClause             string
	SpeakerJoinClause             string
	VisitorWhereConditions        []string
	SpeakerWhereConditions        []string
	ExhibitorWhereConditions      []string
	SponsorWhereConditions        []string
	CategoryWhereConditions       []string
	TypeWhereConditions           []string
	EventRankingWhereConditions   []string
	JobCompositeWhereConditions   []string
	AudienceSpreadWhereConditions []string
}

func (s *SharedFunctionService) buildClickHouseQuery(filterFields models.FilterDataDto) (*ClickHouseQueryResult, error) {
	result := &ClickHouseQueryResult{
		VisitorWhereConditions:        make([]string, 0),
		SpeakerWhereConditions:        make([]string, 0),
		ExhibitorWhereConditions:      make([]string, 0),
		SponsorWhereConditions:        make([]string, 0),
		CategoryWhereConditions:       make([]string, 0),
		TypeWhereConditions:           make([]string, 0),
		EventRankingWhereConditions:   make([]string, 0),
		JobCompositeWhereConditions:   make([]string, 0),
		AudienceSpreadWhereConditions: make([]string, 0),
	}

	whereConditions := make([]string, 0)

	escapeSqlValue := func(value interface{}) string {
		if value == nil {
			return ""
		}
		str := fmt.Sprintf("%v", value)
		escaped := strings.ReplaceAll(str, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}

	addUserFilters := func(filterKey string, dbField string, conditions *[]string) {
		value := s.getFilterValue(filterFields, filterKey)
		if value == "" {
			return
		}

		var values []string
		if strings.Contains(value, ",") {
			parts := strings.Split(value, ",")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					values = append(values, escapeSqlValue(part))
				}
			}
		} else {
			values = []string{escapeSqlValue(value)}
		}

		if len(values) == 0 {
			return
		}

		switch dbField {
		case "user_name":
			var equalityConditions []string
			for _, val := range values {
				cleanVal := strings.Trim(val, "'")
				equalityConditions = append(equalityConditions, fmt.Sprintf("%s = '%s'", dbField, cleanVal))
			}
			*conditions = append(*conditions, fmt.Sprintf("(%s)", strings.Join(equalityConditions, " OR ")))
		case "user_company":
			var likeConditions []string
			for _, val := range values {
				cleanVal := strings.Trim(val, "'")
				likeConditions = append(likeConditions, fmt.Sprintf("%s LIKE '%s%%'", dbField, cleanVal))
			}
			*conditions = append(*conditions, fmt.Sprintf("(%s)", strings.Join(likeConditions, " OR ")))
		default:
			*conditions = append(*conditions, fmt.Sprintf("%s IN (%s)", dbField, strings.Join(values, ",")))
		}
	}

	visitorFilters := []string{"VisitorDesignation", "VisitorCountry", "VisitorCompany", "VisitorCity", "VisitorName", "VisitorState"}
	hasVisitorFilters := false
	for _, filter := range visitorFilters {
		if s.hasFilterValue(filterFields, filter) {
			hasVisitorFilters = true
			break
		}
	}
	if hasVisitorFilters {
		result.NeedsVisitorJoin = true
		addUserFilters("VisitorDesignation", "user_designation", &result.VisitorWhereConditions)
		addUserFilters("VisitorCountry", "user_country", &result.VisitorWhereConditions)
		addUserFilters("VisitorCity", "user_city_name", &result.VisitorWhereConditions)
		addUserFilters("VisitorState", "user_state", &result.VisitorWhereConditions)
		addUserFilters("VisitorCompany", "user_company", &result.VisitorWhereConditions)
		addUserFilters("VisitorName", "user_name", &result.VisitorWhereConditions)
	}

	speakerFilters := []string{"SpeakerDesignation", "SpeakerCountry", "SpeakerCompany", "SpeakerCity", "SpeakerName", "SpeakerState"}
	hasSpeakerFilters := false
	for _, filter := range speakerFilters {
		if s.hasFilterValue(filterFields, filter) {
			hasSpeakerFilters = true
			break
		}
	}
	if hasSpeakerFilters {
		result.NeedsSpeakerJoin = true
		addUserFilters("SpeakerDesignation", "user_designation", &result.SpeakerWhereConditions)
		addUserFilters("SpeakerCountry", "user_country", &result.SpeakerWhereConditions)
		addUserFilters("SpeakerCity", "user_city_name", &result.SpeakerWhereConditions)
		addUserFilters("SpeakerState", "user_state_name", &result.SpeakerWhereConditions)
		addUserFilters("SpeakerCompany", "user_company", &result.SpeakerWhereConditions)
		addUserFilters("SpeakerName", "user_name", &result.SpeakerWhereConditions)
	}

	exhibitorFilters := []string{"ExhibitorName", "ExhibitorWebsite", "ExhibitorDomain", "ExhibitorCountry", "ExhibitorCity", "ExhibitorFacebook", "ExhibitorTwitter", "ExhibitorLinkedin", "ExhibitorState"}
	hasExhibitorFilters := false
	for _, filter := range exhibitorFilters {
		if s.hasFilterValue(filterFields, filter) {
			hasExhibitorFilters = true
			break
		}
	}
	if hasExhibitorFilters {
		result.NeedsExhibitorJoin = true
		addUserFilters("ExhibitorName", "company_id_name", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorWebsite", "company_website", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorDomain", "company_domain", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorCountry", "company_country", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorCity", "company_city_name", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorState", "company_state_name", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorFacebook", "facebook_id", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorTwitter", "twitter_id", &result.ExhibitorWhereConditions)
		addUserFilters("ExhibitorLinkedin", "linkedin_id", &result.ExhibitorWhereConditions)
	}

	sponsorFilters := []string{"SponsorName", "SponsorWebsite", "SponsorDomain", "SponsorCountry", "SponsorCity", "SponsorFacebook", "SponsorTwitter", "SponsorLinkedin", "SponsorState"}
	hasSponsorFilters := false
	for _, filter := range sponsorFilters {
		if s.hasFilterValue(filterFields, filter) {
			hasSponsorFilters = true
			break
		}
	}
	if hasSponsorFilters {
		result.NeedsSponsorJoin = true
		addUserFilters("SponsorName", "company_id_name", &result.SponsorWhereConditions)
		addUserFilters("SponsorWebsite", "company_website", &result.SponsorWhereConditions)
		addUserFilters("SponsorDomain", "company_domain", &result.SponsorWhereConditions)
		addUserFilters("SponsorCountry", "company_country", &result.SponsorWhereConditions)
		addUserFilters("SponsorCity", "company_city_name", &result.SponsorWhereConditions)
		addUserFilters("SponsorState", "company_state_name", &result.SponsorWhereConditions)
		addUserFilters("SponsorFacebook", "facebook_id", &result.SponsorWhereConditions)
		addUserFilters("SponsorTwitter", "twitter_id", &result.SponsorWhereConditions)
		addUserFilters("SponsorLinkedin", "linkedin_id", &result.SponsorWhereConditions)
	}

	if len(filterFields.ParsedCategory) > 0 {
		result.NeedsCategoryJoin = true
		categories := make([]string, len(filterFields.ParsedCategory))
		for i, cat := range filterFields.ParsedCategory {
			categories[i] = escapeSqlValue(cat)
		}
		result.CategoryWhereConditions = append(result.CategoryWhereConditions, fmt.Sprintf("name IN (%s) AND is_group = 1", strings.Join(categories, ",")))
	}

	if len(filterFields.ParsedProducts) > 0 {
		result.NeedsCategoryJoin = true
		products := make([]string, len(filterFields.ParsedProducts))
		for i, product := range filterFields.ParsedProducts {
			products[i] = escapeSqlValue(product)
		}
		result.CategoryWhereConditions = append(result.CategoryWhereConditions, fmt.Sprintf("name IN (%s) AND is_group = 0", strings.Join(products, ",")))
	}

	if len(filterFields.ParsedType) > 0 {
		result.NeedsTypeJoin = true
		types := make([]string, len(filterFields.ParsedType))
		for i, t := range filterFields.ParsedType {
			types[i] = escapeSqlValue(t)
		}
		result.TypeWhereConditions = append(result.TypeWhereConditions, fmt.Sprintf("name IN (%s)", strings.Join(types, ",")))
	}

	if len(filterFields.ParsedEventRanking) > 0 {
		result.NeedsEventRankingJoin = true
		eventRankingValue := filterFields.ParsedEventRanking[0]
		result.EventRankingWhereConditions = append(result.EventRankingWhereConditions, fmt.Sprintf("event_rank <= %s", eventRankingValue))
	}

	if len(filterFields.ParsedJobComposite) > 0 {
		result.needsDesignationJoin = true
		jobComposites := make([]string, len(filterFields.ParsedJobComposite))
		for i, jobComposite := range filterFields.ParsedJobComposite {
			jobComposites[i] = escapeSqlValue(jobComposite)
		}
		result.JobCompositeWhereConditions = append(result.JobCompositeWhereConditions, fmt.Sprintf("display_name IN (%s) AND total_visitors >= 5", strings.Join(jobComposites, ",")))
	}

	if len(filterFields.ParsedAudienceSpread) > 0 {
		result.needsAudienceSpreadJoin = true
		var jsonConditions []string
		for _, audienceSpread := range filterFields.ParsedAudienceSpread {
			escapedISO := escapeSqlValue(audienceSpread)
			jsonConditions = append(jsonConditions, fmt.Sprintf("arrayExists(x -> x.cntry_id = %s AND x.total_count >= 5, user_by_cntry)", escapedISO))
		}
		result.AudienceSpreadWhereConditions = append(result.AudienceSpreadWhereConditions, fmt.Sprintf("(%s)", strings.Join(jsonConditions, " AND ")))
	}

	result.NeedsAnyJoin = result.NeedsVisitorJoin || result.NeedsSpeakerJoin || result.NeedsExhibitorJoin || result.NeedsSponsorJoin || result.NeedsCategoryJoin || result.NeedsTypeJoin || result.NeedsEventRankingJoin || result.needsDesignationJoin || result.needsAudienceSpreadJoin

	s.addRangeFilters("following", "event_followers", &whereConditions, filterFields, false)
	s.addRangeFilters("speaker", "event_speaker", &whereConditions, filterFields, false)
	s.addRangeFilters("exhibitors", "event_exhibitor", &whereConditions, filterFields, false)
	s.addRangeFilters("editions", "event_editionsCount", &whereConditions, filterFields, false)
	s.addRangeFilters("start", "start_date", &whereConditions, filterFields, true)
	s.addRangeFilters("end", "end_date", &whereConditions, filterFields, true)
	s.addRangeFilters("inboundScore", "inboundScore", &whereConditions, filterFields, false)
	s.addRangeFilters("internationalScore", "internationalScore", &whereConditions, filterFields, false)
	s.addRangeFilters("trustScore", "repeatSentimentChangePercentage", &whereConditions, filterFields, false)

	s.addEstimatedExhibitorsFilter(&whereConditions, filterFields)

	s.addInFilter("country", "edition_country", &whereConditions, filterFields)
	s.addInFilter("venue", "venue_name", &whereConditions, filterFields)
	s.addInFilter("company", "company_name", &whereConditions, filterFields)
	s.addInFilter("companyCountry", "company_country", &whereConditions, filterFields)
	s.addInFilter("companyCity", "company_city_name", &whereConditions, filterFields)
	s.addInFilter("companyDomain", "company_domain", &whereConditions, filterFields)
	s.addInFilter("companyState", "company_state", &whereConditions, filterFields)

	if filterFields.Visibility != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("ee.edition_functionality = %s", escapeSqlValue(filterFields.Visibility)))
	}
	if filterFields.EstimatedVisitors != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("ee.event_estimatedVisitors = %s", escapeSqlValue(filterFields.EstimatedVisitors)))
	}

	if filterFields.EventEstimate {
		whereConditions = append(whereConditions, "ee.event_economic_value IS NOT NULL")
	}

	s.addActiveDateFilters(&whereConditions, filterFields)

	result.DistanceOrderClause = s.addGeographicFilters(&whereConditions, filterFields, func(field string) string {
		return fmt.Sprintf("ee.%s", field)
	})

	s.addAllEventFilters(&whereConditions, filterFields)

	result.SearchClause = s.buildSearchClause(filterFields)

	result.WhereClause = strings.Join(whereConditions, " AND ")

	return result, nil
}

type CTEAndJoinResult struct {
	CTEClauses     []string
	JoinConditions []string
}

func (s *SharedFunctionService) buildFilterCTEsAndJoins(
	needsVisitorJoin bool,
	needsSpeakerJoin bool,
	needsExhibitorJoin bool,
	needsSponsorJoin bool,
	needsCategoryJoin bool,
	needsTypeJoin bool,
	needsEventRankingJoin bool,
	needsDesignationJoin bool,
	needsAudienceSpreadJoin bool,
	visitorWhereConditions []string,
	speakerWhereConditions []string,
	exhibitorWhereConditions []string,
	sponsorWhereConditions []string,
	categoryWhereConditions []string,
	typeWhereConditions []string,
	eventRankingWhereConditions []string,
	jobCompositeWhereConditions []string,
	audienceSpreadWhereConditions []string,
	filterFields models.FilterDataDto,
) CTEAndJoinResult {
	result := CTEAndJoinResult{
		CTEClauses:     make([]string, 0),
		JoinConditions: make([]string, 0),
	}

	previousCTE := ""

	if needsVisitorJoin {
		visitorWhereClause := ""
		if len(visitorWhereConditions) > 0 {
			visitorWhereClause = fmt.Sprintf("WHERE %s", strings.Join(visitorWhereConditions, " AND "))
		}

		visitorCTE := fmt.Sprintf(`filtered_visitors AS (
			SELECT event_id
			FROM testing_db.event_visitors_ch
			%s
			GROUP BY event_id
		)`, visitorWhereClause)
		result.CTEClauses = append(result.CTEClauses, visitorCTE)
		previousCTE = "filtered_visitors"
	}

	if needsSpeakerJoin {
		speakerWhereClause := ""
		if len(speakerWhereConditions) > 0 {
			speakerWhereClause = fmt.Sprintf("WHERE %s", strings.Join(speakerWhereConditions, " AND "))
		}

		speakerQuery := fmt.Sprintf(`filtered_speakers AS (
			SELECT event_id
			FROM testing_db.event_speaker_ch
			%s`, speakerWhereClause)

		if previousCTE != "" {
			speakerQuery = fmt.Sprintf(`filtered_speakers AS (
				SELECT event_id
				FROM testing_db.event_speaker_ch
				WHERE event_id IN (SELECT event_id FROM %s)`, previousCTE)
			if len(speakerWhereConditions) > 0 {
				speakerQuery += fmt.Sprintf(`
				AND %s`, strings.Join(speakerWhereConditions, " AND "))
			}
		}

		speakerQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, speakerQuery)
		previousCTE = "filtered_speakers"
	}

	if needsExhibitorJoin {
		exhibitorWhereClause := ""
		if len(exhibitorWhereConditions) > 0 {
			exhibitorWhereClause = fmt.Sprintf("WHERE %s", strings.Join(exhibitorWhereConditions, " AND "))
		}

		exhibitorQuery := fmt.Sprintf(`filtered_exhibitors AS (
			SELECT event_id
			FROM testing_db.event_exhibitor_ch
			%s`, exhibitorWhereClause)

		if previousCTE != "" {
			exhibitorQuery = fmt.Sprintf(`filtered_exhibitors AS (
				SELECT event_id
				FROM testing_db.event_exhibitor_ch
				WHERE event_id IN (SELECT event_id FROM %s)`, previousCTE)
			if len(exhibitorWhereConditions) > 0 {
				exhibitorQuery += fmt.Sprintf(`
				AND %s`, strings.Join(exhibitorWhereConditions, " AND "))
			}
		}

		exhibitorQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, exhibitorQuery)
		previousCTE = "filtered_exhibitors"
	}

	if needsSponsorJoin {
		sponsorWhereClause := ""
		if len(sponsorWhereConditions) > 0 {
			sponsorWhereClause = fmt.Sprintf("WHERE %s", strings.Join(sponsorWhereConditions, " AND "))
		}

		sponsorQuery := fmt.Sprintf(`filtered_sponsors AS (
			SELECT event_id
			FROM testing_db.event_sponsors_ch
			%s`, sponsorWhereClause)

		if previousCTE != "" {
			sponsorQuery = fmt.Sprintf(`filtered_sponsors AS (
				SELECT event_id
				FROM testing_db.event_sponsors_ch
				WHERE event_id IN (SELECT event_id FROM %s)`, previousCTE)
			if len(sponsorWhereConditions) > 0 {
				sponsorQuery += fmt.Sprintf(`
				AND %s`, strings.Join(sponsorWhereConditions, " AND "))
			}
		}

		sponsorQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, sponsorQuery)
		previousCTE = "filtered_sponsors"
	}

	if needsCategoryJoin {
		categoryWhereClause := ""
		if len(categoryWhereConditions) > 0 {
			categoryWhereClause = fmt.Sprintf("WHERE %s", strings.Join(categoryWhereConditions, " AND "))
		}

		categoryQuery := fmt.Sprintf(`filtered_categories AS (
			SELECT event
			FROM testing_db.event_category_ch
			%s`, categoryWhereClause)

		if previousCTE != "" {
			categoryQuery = fmt.Sprintf(`filtered_categories AS (
				SELECT event
				FROM testing_db.event_category_ch
				WHERE event IN (SELECT event_id FROM %s)`, previousCTE)
			if len(categoryWhereConditions) > 0 {
				categoryQuery += fmt.Sprintf(`
				AND %s`, strings.Join(categoryWhereConditions, " AND "))
			}
		}

		categoryQuery += `
			GROUP BY event
		)`

		result.CTEClauses = append(result.CTEClauses, categoryQuery)
		previousCTE = "filtered_categories"
	}

	if needsTypeJoin {
		typeWhereClause := ""
		if len(typeWhereConditions) > 0 {
			typeWhereClause = fmt.Sprintf("WHERE %s", strings.Join(typeWhereConditions, " AND "))
		}

		typeQuery := fmt.Sprintf(`filtered_types AS (
			SELECT event_id
			FROM testing_db.event_type_ch
			%s`, typeWhereClause)

		if previousCTE != "" {
			typeQuery = fmt.Sprintf(`filtered_types AS (
				SELECT event_id
				FROM testing_db.event_type_ch
				WHERE event_id IN (SELECT event_id FROM %s)`, previousCTE)
			if len(typeWhereConditions) > 0 {
				typeQuery += fmt.Sprintf(`
				AND %s`, strings.Join(typeWhereConditions, " AND "))
			}
		}

		typeQuery += `
			GROUP BY event_id
			ORDER BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, typeQuery)
		previousCTE = "filtered_types"
	}

	if needsEventRankingJoin {
		today := time.Now().Format("2006-01-02")

		preEventFilterConditions := []string{
			s.buildPublishedCondition(filterFields),
			s.buildStatusCondition(filterFields),
			"edition_type = 'current_edition'",
		}
		hasUserEndDateFilter := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
			filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != ""
		if !hasUserEndDateFilter {
			preEventFilterConditions = append(preEventFilterConditions, fmt.Sprintf("end_date >= '%s'", today))
		}

		preEventFilterWhereClause := strings.Join(preEventFilterConditions, " AND ")

		if previousCTE != "" {
			var selectColumn string
			if previousCTE == "filtered_categories" {
				selectColumn = "event"
			} else {
				selectColumn = "event_id"
			}
			preEventFilterCTE := fmt.Sprintf(`pre_event_filter AS (
				SELECT event_id, edition_id
				FROM testing_db.event_edition_ch AS ee
				WHERE event_id IN (SELECT %s FROM %s)
				AND %s
				GROUP BY event_id, edition_id
				ORDER BY event_id ASC
			)`, selectColumn, previousCTE, preEventFilterWhereClause)
			result.CTEClauses = append(result.CTEClauses, preEventFilterCTE)
		} else {
			preEventFilterCTE := fmt.Sprintf(`pre_event_filter AS (
				SELECT event_id, edition_id
				FROM testing_db.event_edition_ch AS ee
				WHERE %s
				GROUP BY event_id, edition_id
				ORDER BY event_id ASC
			)`, preEventFilterWhereClause)
			result.CTEClauses = append(result.CTEClauses, preEventFilterCTE)
		}

		currentMonth := time.Now().Month()
		currentMonthCondition := fmt.Sprintf("MONTH(created) = %d", currentMonth)

		eventRankingConditions := []string{currentMonthCondition}

		hasCountryFilter := len(filterFields.ParsedCountry) > 0
		hasCategoryFilter := len(filterFields.ParsedCategory) > 0

		if hasCountryFilter {
			countries := make([]string, len(filterFields.ParsedCountry))
			for i, country := range filterFields.ParsedCountry {
				countries[i] = fmt.Sprintf("'%s'", country)
			}
			eventRankingConditions = append(eventRankingConditions, fmt.Sprintf("country IN (%s)", strings.Join(countries, ",")))
		}
		if hasCategoryFilter {
			categories := make([]string, len(filterFields.ParsedCategory))
			for i, category := range filterFields.ParsedCategory {
				categories[i] = fmt.Sprintf("'%s'", category)
			}
			eventRankingConditions = append(eventRankingConditions, fmt.Sprintf("category_name IN (%s)", strings.Join(categories, ",")))
		}
		if !hasCountryFilter && !hasCategoryFilter {
			eventRankingConditions = append(eventRankingConditions, "((country = '' AND category_name = ''))")
		}

		if len(eventRankingWhereConditions) > 0 {
			eventRankingConditions = append(eventRankingConditions, eventRankingWhereConditions...)
		}
		eventRankingLimit := filterFields.ParsedEventRanking[0]

		eventRankingQuery := fmt.Sprintf(`filtered_event_ranking AS (
			SELECT event_id
			FROM testing_db.event_ranking_ch
			WHERE event_id IN (SELECT event_id FROM pre_event_filter)
			AND %s
			GROUP BY event_id LIMIT %s
		)`, strings.Join(eventRankingConditions, " AND "), eventRankingLimit)

		result.CTEClauses = append(result.CTEClauses, eventRankingQuery)
		previousCTE = "filtered_event_ranking"
	}

	if needsDesignationJoin {
		jobCompositeWhereClause := ""
		if len(jobCompositeWhereConditions) > 0 {
			jobCompositeWhereClause = fmt.Sprintf("WHERE %s", strings.Join(jobCompositeWhereConditions, " AND "))
		}

		jobCompositeQuery := fmt.Sprintf(`filtered_designations AS (
			SELECT event_id
			FROM testing_db.event_designation_ch
			%s`, jobCompositeWhereClause)

		if previousCTE != "" {
			var selectColumn string
			if previousCTE == "filtered_categories" {
				selectColumn = "event"
			} else {
				selectColumn = "event_id"
			}
			jobCompositeQuery = fmt.Sprintf(`filtered_designations AS (
				SELECT event_id
				FROM testing_db.event_designation_ch
				WHERE event_id IN (SELECT %s FROM %s)`, selectColumn, previousCTE)
			if len(jobCompositeWhereConditions) > 0 {
				jobCompositeQuery += fmt.Sprintf(`
				AND %s`, strings.Join(jobCompositeWhereConditions, " AND "))
			}
		}

		jobCompositeQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, jobCompositeQuery)
		previousCTE = "filtered_designations"
	}

	if needsAudienceSpreadJoin {
		audienceSpreadWhereClause := ""
		if len(audienceSpreadWhereConditions) > 0 {
			audienceSpreadWhereClause = fmt.Sprintf("WHERE %s", strings.Join(audienceSpreadWhereConditions, " AND "))
		}

		audienceSpreadQuery := fmt.Sprintf(`filtered_audience_spread AS (
			SELECT event_id
			FROM testing_db.event_visitorSpread_ch
			%s`, audienceSpreadWhereClause)

		if previousCTE != "" {
			var selectColumn string
			if previousCTE == "filtered_categories" {
				selectColumn = "event"
			} else {
				selectColumn = "event_id"
			}
			audienceSpreadQuery = fmt.Sprintf(`filtered_audience_spread AS (
				SELECT event_id
				FROM testing_db.event_visitorSpread_ch
				WHERE event_id IN (SELECT %s FROM %s)`, selectColumn, previousCTE)
			if len(audienceSpreadWhereConditions) > 0 {
				audienceSpreadQuery += fmt.Sprintf(`
				AND %s`, strings.Join(audienceSpreadWhereConditions, " AND "))
			}
		}

		audienceSpreadQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, audienceSpreadQuery)
		previousCTE = "filtered_audience_spread"
	}

	if previousCTE != "" {
		var selectColumn string
		switch previousCTE {
		case "filtered_categories":
			selectColumn = "event"
		default:
			selectColumn = "event_id"
		}
		result.JoinConditions = append(result.JoinConditions, fmt.Sprintf("ee.event_id IN (SELECT %s FROM %s)", selectColumn, previousCTE))
	}

	return result
}

func (s *SharedFunctionService) fixOrderByForCTE(orderByClause string, useAliases bool) string {
	if orderByClause == "" {
		return ""
	}

	if useAliases && strings.Contains(orderByClause, "greatCircleDistance") &&
		(strings.Contains(orderByClause, "lat") || strings.Contains(orderByClause, "lon")) {
		fieldMappings := map[string]string{
			"event_id":        "id",
			"start_date":      "start",
			"end_date":        "end",
			"event_followers": "followers",
			"event_avgRating": "avgRating",
			"event_exhibitor": "exhibitors",
			"event_speaker":   "speakers",
			"event_sponsor":   "sponsors",
			"event_created":   "created",
		}

		fixedClause := orderByClause
		for dbField, mappedField := range fieldMappings {
			fixedClause = strings.ReplaceAll(fixedClause, fmt.Sprintf("ee.%s", dbField), mappedField)
			fixedClause = strings.ReplaceAll(fixedClause, fmt.Sprintf("ee.%s", mappedField), mappedField)
			fieldPattern := fmt.Sprintf("\\b%s\\b", dbField)
			re := regexp.MustCompile(fieldPattern)
			fixedClause = re.ReplaceAllString(fixedClause, mappedField)
		}
		fixedClause = regexp.MustCompile(`\bee\.`).ReplaceAllString(fixedClause, "")
		log.Println("fixedClause", fixedClause)
		return fixedClause
	}

	fieldMappings := map[string]string{
		"event_uuid":        "id",
		"start_date":        "start",
		"end_date":          "end",
		"event_followers":   "followers",
		"event_avgRating":   "avgRating",
		"event_exhibitor":   "exhibitors",
		"event_speaker":     "speakers",
		"event_sponsor":     "sponsors",
		"event_created":     "created",
		"exhibitors_mean":   "estimatedExhibitors",
		"edition_city_lat":  "lat",
		"edition_city_long": "lon",
		"venue_lat":         "venueLat",
		"venue_long":        "venueLon",
	}

	if !useAliases {
		fieldMappings = map[string]string{
			"id":                  "event_id",
			"start":               "start_date",
			"end":                 "end_date",
			"followers":           "event_followers",
			"avgRating":           "event_avgRating",
			"exhibitors":          "event_exhibitor",
			"speakers":            "event_speaker",
			"sponsors":            "event_sponsor",
			"created":             "event_created",
			"estimatedExhibitors": "exhibitors_mean",
			"lat":                 "edition_city_lat",
			"lon":                 "edition_city_long",
			"venueLat":            "venue_lat",
			"venueLon":            "venue_long",
		}
	}

	fixedClause := orderByClause

	for dbField, mappedField := range fieldMappings {
		fixedClause = strings.ReplaceAll(fixedClause, fmt.Sprintf("ee.%s", dbField), mappedField)
		fixedClause = strings.ReplaceAll(fixedClause, fmt.Sprintf("ee.%s", mappedField), mappedField)
		fieldPattern := fmt.Sprintf("\\b%s\\b", dbField)
		re := regexp.MustCompile(fieldPattern)
		fixedClause = re.ReplaceAllString(fixedClause, mappedField)
	}

	fixedClause = regexp.MustCompile(`\bee\.`).ReplaceAllString(fixedClause, "")
	return fixedClause
}

func (s *SharedFunctionService) hasFilterValue(filterFields models.FilterDataDto, fieldName string) bool {
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if fieldType.Name == fieldName {
			if field.Kind() == reflect.String {
				return field.String() != ""
			}
		}
	}
	return false
}

func (s *SharedFunctionService) addRangeFilters(filterKey string, dbField string, whereConditions *[]string, filterFields models.FilterDataDto, isString bool) {
	operators := []struct {
		suffix   string
		operator string
	}{
		{".gte", ">="},
		{".lte", "<="},
		{".gt", ">"},
		{".lt", "<"},
	}

	for _, op := range operators {
		value := s.getFilterValue(filterFields, filterKey+op.suffix)
		if value != "" {
			fieldName := fmt.Sprintf("ee.%s", dbField)
			if isString {
				*whereConditions = append(*whereConditions, fmt.Sprintf("%s %s '%s'", fieldName, op.operator, value))
			} else {
				*whereConditions = append(*whereConditions, fmt.Sprintf("%s %s %s", fieldName, op.operator, value))
			}
		}
	}
}

func (s *SharedFunctionService) addEstimatedExhibitorsFilter(whereConditions *[]string, filterFields models.FilterDataDto) {
	if filterFields.EstimatedExhibitors == "" {
		return
	}

	var gte, lte int
	switch filterFields.EstimatedExhibitors {
	case "0-100":
		gte, lte = 0, 100
	case "100-500":
		gte, lte = 100, 500
	case "500-1000":
		gte, lte = 500, 1000
	case "1000":
		gte, lte = 1000, 1000000
	default:
		return
	}

	condition := fmt.Sprintf("ee.exhibitors_mean IS NOT NULL AND ee.exhibitors_mean >= %d AND ee.exhibitors_mean <= %d", gte, lte)
	*whereConditions = append(*whereConditions, condition)
}

func (s *SharedFunctionService) addInFilter(filterKey string, dbField string, whereConditions *[]string, filterFields models.FilterDataDto) {
	values := s.getParsedArrayValue(filterFields, filterKey)
	if len(values) == 0 {
		value := s.getFilterValue(filterFields, filterKey)
		if value != "" {
			values = []string{value}
		}
	}

	if len(values) > 0 {
		escapedValues := make([]string, len(values))
		for i, val := range values {
			escapedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
		}
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.%s IN (%s)", dbField, strings.Join(escapedValues, ",")))
	}
}

func (s *SharedFunctionService) addActiveDateFilters(whereConditions *[]string, filterFields models.FilterDataDto) {
	activeFilters := []struct {
		key      string
		field    string
		operator string
	}{
		{"ActiveGte", "end_date", ">="},
		{"ActiveLte", "start_date", "<="},
		{"ActiveGt", "end_date", ">"},
		{"ActiveLt", "start_date", "<"},
	}

	for _, filter := range activeFilters {
		value := s.getFilterValue(filterFields, filter.key)
		if value != "" {
			*whereConditions = append(*whereConditions, fmt.Sprintf("ee.%s %s '%s'", filter.field, filter.operator, value))
		}
	}
}

func (s *SharedFunctionService) addGeographicFilters(whereConditions *[]string, filterFields models.FilterDataDto, addTableAlias func(string) string) string {
	distanceOrderClause := ""

	if filterFields.Lat != "" && filterFields.Lon != "" && filterFields.Radius != "" && filterFields.Unit != "" {
		lat, lon, radiusInMeters := s.parseCoordinates(filterFields.Lat, filterFields.Lon, filterFields.Radius, filterFields.Unit)
		*whereConditions = append(*whereConditions, fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f", lat, lon, addTableAlias("edition_city_lat"), addTableAlias("edition_city_long"), radiusInMeters))
		orderDirection := "ASC"
		if filterFields.EventDistanceOrder == "farthest" {
			orderDirection = "DESC"
		}
		distanceOrderClause = fmt.Sprintf("ORDER BY greatCircleDistance(%f, %f, lat, lon) %s", lat, lon, orderDirection)
	}

	if filterFields.VenueLatitude != "" && filterFields.VenueLongitude != "" && filterFields.Radius != "" && filterFields.Unit != "" {
		lat, lon, radiusInMeters := s.parseCoordinates(filterFields.VenueLatitude, filterFields.VenueLongitude, filterFields.Radius, filterFields.Unit)
		*whereConditions = append(*whereConditions, fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f", lat, lon, addTableAlias("venue_lat"), addTableAlias("venue_long"), radiusInMeters))
		if distanceOrderClause == "" {
			orderDirection := "ASC"
			if filterFields.EventDistanceOrder == "farthest" {
				orderDirection = "DESC"
			}
			distanceOrderClause = fmt.Sprintf("ORDER BY greatCircleDistance(%f, %f, venueLat, venueLon) %s", lat, lon, orderDirection)
		}
	}

	return distanceOrderClause
}

func (s *SharedFunctionService) addAllEventFilters(whereConditions *[]string, filterFields models.FilterDataDto) {
	if len(filterFields.ParsedCity) > 0 {
		escapedCities := make([]string, len(filterFields.ParsedCity))
		for i, city := range filterFields.ParsedCity {
			escapedCities[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(city, "'", "''"))
		}
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.edition_city_name IN (%s)", strings.Join(escapedCities, ",")))
	}

	if len(filterFields.ParsedState) > 0 {
		escapedStates := make([]string, len(filterFields.ParsedState))
		for i, state := range filterFields.ParsedState {
			escapedStates[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(state, "'", "''"))
		}
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.edition_city_state IN (%s)", strings.Join(escapedStates, ",")))
	}

	if filterFields.Price != "" {
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.event_pricing = '%s'", strings.ReplaceAll(filterFields.Price, "'", "''")))
	}

	if filterFields.Frequency != "" {
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.event_frequency = '%s'", strings.ReplaceAll(filterFields.Frequency, "'", "''")))
	}

	if filterFields.AvgRating != "" {
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.event_avgRating >= %s", filterFields.AvgRating))
	}

	if filterFields.ParsedMode != nil {
		mode := *filterFields.ParsedMode
		switch mode {
		case "hybrid":
			*whereConditions = append(*whereConditions, "ee.event_hybrid = '1'")
		case "online":
			*whereConditions = append(*whereConditions, "ee.edition_city = '1'")
		case "physical":
			*whereConditions = append(*whereConditions, "ee.edition_city != '1' AND ee.event_hybrid = '0'")
		}
	}

	if filterFields.ParsedIsBranded != nil {
		if *filterFields.ParsedIsBranded {
			*whereConditions = append(*whereConditions, "ee.isBranded = 1")
		} else {
			*whereConditions = append(*whereConditions, "ee.isBranded = 0")
		}
	}

	if filterFields.Maturity != "" {
		*whereConditions = append(*whereConditions, fmt.Sprintf("ee.maturity = '%s'", strings.ReplaceAll(filterFields.Maturity, "'", "''")))
	}

	if filterFields.ParsedAudienceZone != nil {
		audienceZones := filterFields.ParsedAudienceZone
		for _, audienceZone := range audienceZones {
			*whereConditions = append(*whereConditions, fmt.Sprintf("ee.audienceZone = '%s'", strings.ReplaceAll(audienceZone, "'", "''")))
		}
	}
}

func (s *SharedFunctionService) buildStatusCondition(filterFields models.FilterDataDto) string {
	if len(filterFields.ParsedStatus) == 0 {
		return "status != 'U'"
	}
	statuses := make([]string, len(filterFields.ParsedStatus))
	for i, status := range filterFields.ParsedStatus {
		statuses[i] = fmt.Sprintf("'%s'", status)
	}

	return fmt.Sprintf("status = %s", strings.Join(statuses, ","))
}

func (s *SharedFunctionService) buildPublishedCondition(filterFields models.FilterDataDto) string {
	if len(filterFields.ParsedPublished) == 0 {
		return "published = '1'"
	}
	if len(filterFields.ParsedPublished) == 1 {
		return fmt.Sprintf("published = '%s'", filterFields.ParsedPublished[0])
	}
	publishedValues := make([]string, len(filterFields.ParsedPublished))
	for i, published := range filterFields.ParsedPublished {
		publishedValues[i] = fmt.Sprintf("'%s'", published)
	}
	return fmt.Sprintf("published IN (%s)", strings.Join(publishedValues, ","))
}

func (s *SharedFunctionService) buildMultiDayDateSelect() string {
	return `formatDateTime(arrayJoin(arrayMap(x -> addDays(toDate(ee.start_date), x), range(0, dateDiff('day', toDate(ee.start_date), toDate(ee.end_date)) + 1))), '%Y-%m-%d') as date`
}

func (s *SharedFunctionService) buildMultiDayMonthSelect() string {
	return `formatDateTime(arrayJoin(arrayMap(x -> addDays(toDate(ee.start_date), x), range(0, dateDiff('day', toDate(ee.start_date), toDate(ee.end_date)) + 1))), '%Y-%m') as month`
}

func (s *SharedFunctionService) buildMultiDayFieldSelect(fields []string, fieldMapping map[string]string) string {
	var selects []string

	for _, field := range fields {
		switch field {
		case "date":
			selects = append(selects, s.buildMultiDayDateSelect())
		case "month":
			selects = append(selects, s.buildMultiDayMonthSelect())
		case "category":
			selects = append(selects, "c.name as category")
		case "tag":
			selects = append(selects, "t.name as tag")
		default:
			if dbField, exists := fieldMapping[field]; exists {
				selects = append(selects, dbField)
			}
		}
	}

	return strings.Join(selects, ",\n        ")
}

func (s *SharedFunctionService) needsMultiDayExpansion(fields []string) bool {
	for _, field := range fields {
		if field == "date" || field == "month" {
			return true
		}
	}
	return false
}

func (s *SharedFunctionService) buildSearchClause(filterFields models.FilterDataDto) string {
	var searchClause strings.Builder

	if filterFields.Q != "" {
		searchTerm := strings.ToLower(strings.ReplaceAll(filterFields.Q, "'", "''"))
		searchClause.WriteString(fmt.Sprintf("(position('%s' IN lower(ee.event_name)) > 0 OR position('%s' IN lower(ee.event_description)) > 0 OR position('%s' IN lower(ee.event_abbr_name)) > 0)", searchTerm, searchTerm, searchTerm))
	}

	if filterFields.ParsedKeywords != nil {
		keywords := filterFields.ParsedKeywords

		if len(keywords.Include) > 0 {
			var includeConditions []string
			for _, keyword := range keywords.Include {
				cleanKeyword := strings.ToLower(strings.ReplaceAll(keyword, "'", "''"))
				includeConditions = append(includeConditions, fmt.Sprintf("(position('%s' IN lower(ee.event_name)) > 0 OR position('%s' IN lower(ee.event_description)) > 0)", cleanKeyword, cleanKeyword))
			}
			if searchClause.Len() > 0 {
				searchClause.WriteString(fmt.Sprintf(" AND (%s)", strings.Join(includeConditions, " AND ")))
			} else {
				searchClause.WriteString(fmt.Sprintf("(%s)", strings.Join(includeConditions, " AND ")))
			}
		}

		if len(keywords.Exclude) > 0 {
			var excludeConditions []string
			for _, keyword := range keywords.Exclude {
				cleanKeyword := strings.ToLower(strings.ReplaceAll(keyword, "'", "''"))
				excludeConditions = append(excludeConditions, fmt.Sprintf("(position('%s' IN lower(ee.event_name)) = 0 AND position('%s' IN lower(ee.event_description)) = 0)", cleanKeyword, cleanKeyword))
			}
			if searchClause.Len() > 0 {
				searchClause.WriteString(fmt.Sprintf(" AND (%s)", strings.Join(excludeConditions, " AND ")))
			} else {
				searchClause.WriteString(fmt.Sprintf("(%s)", strings.Join(excludeConditions, " AND ")))
			}
		}
	}

	return searchClause.String()
}

func (s *SharedFunctionService) getFilterValue(filterFields models.FilterDataDto, fieldName string) string {
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		jsonTag := fieldType.Tag.Get("json")
		if jsonTag != "" {
			if strings.Contains(jsonTag, ",") {
				jsonTag = strings.Split(jsonTag, ",")[0]
			}
			if jsonTag == fieldName {
				if field.Kind() == reflect.String {
					return field.String()
				}
			}
		}

		if fieldType.Name == fieldName {
			if field.Kind() == reflect.String {
				return field.String()
			}
		}
	}
	return ""
}

func (s *SharedFunctionService) getParsedArrayValue(filterFields models.FilterDataDto, fieldName string) []string {
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if strings.EqualFold(fieldType.Name, "Parsed"+fieldName) {
			if field.Kind() == reflect.Slice {
				slice := field.Interface().([]string)
				return slice
			}
		}
	}
	return []string{}
}

func (s *SharedFunctionService) parseCoordinates(lat, lon, radius, unit string) (float64, float64, float64) {
	parsedLat, _ := strconv.ParseFloat(lat, 64)
	parsedLon, _ := strconv.ParseFloat(lon, 64)
	parsedRadius, _ := strconv.ParseFloat(radius, 64)

	radiusInMeters := parsedRadius
	conversionRates := map[string]float64{
		"km": 1000,
		"mi": 1609.34,
		"ft": 0.3048,
	}
	if rate, exists := conversionRates[unit]; exists {
		radiusInMeters = parsedRadius * rate
	}

	return parsedLat, parsedLon, radiusInMeters
}

func (s *SharedFunctionService) HandleNestedAggregation(filterFields models.FilterDataDto, pagination models.PaginationDto) (string, error) {

	aggregationFields := []string{}
	if filterFields.ToAggregate != "" {
		aggregationFields = strings.Split(filterFields.ToAggregate, ",")
		for i, field := range aggregationFields {
			aggregationFields[i] = strings.TrimSpace(field)
		}
	}

	if len(aggregationFields) == 0 {
		return "", nil
	}

	fieldOrder := []string{"country", "city", "month", "date", "category", "tag"}

	var selectedFieldIndices []int
	for _, field := range aggregationFields {
		for i, orderedField := range fieldOrder {
			if field == orderedField {
				selectedFieldIndices = append(selectedFieldIndices, i)
				break
			}
		}
	}

	for i := 0; i < len(selectedFieldIndices)-1; i++ {
		for j := i + 1; j < len(selectedFieldIndices); j++ {
			if selectedFieldIndices[i] > selectedFieldIndices[j] {
				selectedFieldIndices[i], selectedFieldIndices[j] = selectedFieldIndices[j], selectedFieldIndices[i]
			}
		}
	}

	if len(selectedFieldIndices) == 0 {
		return "", nil
	}

	parentField := fieldOrder[selectedFieldIndices[0]]
	var nestedFields []string
	for i := 1; i < len(selectedFieldIndices); i++ {
		nestedFields = append(nestedFields, fieldOrder[selectedFieldIndices[i]])
	}

	aggregationQuery, err := s.buildNestedAggregationQuery(parentField, nestedFields, pagination, filterFields)
	if err != nil {
		return "", err
	}

	return aggregationQuery, nil
}

func (s *SharedFunctionService) transformAggregationDataToNested(flatData []map[string]interface{}, aggregationFields []string) (interface{}, error) {
	if len(flatData) == 0 {
		return map[string]interface{}{}, nil
	}

	if len(aggregationFields) == 0 {
		aggregationFields = s.detectAggregationFields(flatData[0])
	} else {
	}

	if len(aggregationFields) == 0 {
		return map[string]interface{}{}, nil
	}

	return s.transformNestedQueryData(flatData, aggregationFields)
}

func (s *SharedFunctionService) transformNestedQueryData(flatData []map[string]interface{}, aggregationFields []string) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recovered in transformNestedQueryData: %v", r)
		}
	}()

	if len(aggregationFields) > 4 {
		log.Printf("WARNING: Aggregation with %d fields is not supported. Maximum supported is 4 fields.", len(aggregationFields))
		return map[string]interface{}{}, nil
	}

	parentField := aggregationFields[0]
	s.sortFlatDataByCount(flatData, parentField)

	result := orderedmap.NewOrderedMap()

	for itemIndex, item := range flatData {

		parentValue, exists := item[parentField]
		if !exists || parentValue == nil {
			continue
		}

		parentValueStr := fmt.Sprintf("%v", parentValue)
		parentCount := s.getCountFromItem(item, parentField)

		if _, exists := result.Get(parentValueStr); !exists {
			parentData := orderedmap.NewOrderedMap()
			parentData.Set(fmt.Sprintf("%sCount", parentField), parentCount)
			result.Set(parentValueStr, parentData)
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Panic recovered in nested processing for item %d: %v", itemIndex, r)
				}
			}()

			switch len(aggregationFields) {
			case 1:
				return
			case 2:
				s.processLevel2Data(item, result, parentValueStr, aggregationFields)
			case 3:
				s.processLevel3Data(item, result, parentValueStr, aggregationFields)
			case 4:
				s.processLevel4Data(item, result, parentValueStr, aggregationFields)
			}
		}()
	}

	return s.convertOrderedMapToSlice(result), nil
}

func (s *SharedFunctionService) processLevel2Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	level1Field := aggregationFields[1]
	nestedDataKey := fmt.Sprintf("%sData", level1Field)

	nestedDataArray, exists := item[nestedDataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := nestedDataArray.([]interface{}); ok {
		sortedDataSlice := s.sortArrayDataByCount(dataSlice)

		for _, item := range sortedDataSlice {
			if itemMap, ok := item.(map[string]interface{}); ok {
				itemName, _ := itemMap["field1Name"].(string)
				itemCount := s.parseIntFromInterface(itemMap["field1Count"])

				if itemName != "" {
					itemData := orderedmap.NewOrderedMap()
					itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
					level1Map.Set(itemName, itemData)
				}
			} else if itemArray, ok := item.([]interface{}); ok && len(itemArray) >= 2 {
				itemName := fmt.Sprintf("%v", itemArray[0])
				itemCount := s.parseIntFromInterface(itemArray[1])
				itemData := orderedmap.NewOrderedMap()
				itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
				level1Map.Set(itemName, itemData)
			} else if itemStr, ok := item.(string); ok {
				parts := strings.Split(itemStr, "|")
				if len(parts) >= 2 {
					itemName := strings.TrimSpace(parts[0])
					if itemCount, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						itemData := orderedmap.NewOrderedMap()
						itemData.Set(fmt.Sprintf("%sCount", level1Field), itemCount)
						level1Map.Set(itemName, itemData)
					}
				}
			}
		}
	}
}

func (s *SharedFunctionService) processLevel3Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	if len(aggregationFields) < 3 {
		return
	}

	level1Field := aggregationFields[1]
	level2Field := aggregationFields[2]
	level1DataKey := fmt.Sprintf("%sData", level1Field)

	level1DataArray, exists := item[level1DataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := level1DataArray.([]interface{}); ok {
		sortedDataSlice := s.sortDataSliceByCount(dataSlice, "field1Count")

		for _, level1Item := range sortedDataSlice {
			if level1DataMap, ok := level1Item.(map[string]interface{}); ok {
				level1Name, _ := level1DataMap["field1Name"].(string)
				level1Count := s.parseIntFromInterface(level1DataMap["field1Count"])
				level2Data, _ := level1DataMap["field2Data"].([]interface{})

				if level1Name != "" {
					level1Entry := orderedmap.NewOrderedMap()
					level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

					if len(level2Data) > 0 {
						level2Map := orderedmap.NewOrderedMap()
						sortedLevel2Data := s.sortDataSliceByCount(level2Data, "count")

						for _, level2Item := range sortedLevel2Data {
							if level2ItemMap, ok := level2Item.(map[string]interface{}); ok {
								level2Name, _ := level2ItemMap["value"].(string)
								level2Count := s.parseIntFromInterface(level2ItemMap["count"])

								if level2Name != "" {
									level2Entry := orderedmap.NewOrderedMap()
									level2Entry.Set(fmt.Sprintf("%sCount", level2Field), level2Count)
									level2Map.Set(level2Name, level2Entry)
								}
							}
						}
						if level2Map.Len() > 0 {
							level1Entry.Set(level2Field, level2Map)
						}
					}

					level1Map.Set(level1Name, level1Entry)
				}
			} else {
				unwrappedData := s.unwrapNestedArrays(level1Item)
				if level1ItemMap, ok := unwrappedData.(map[string]interface{}); ok {
					s.parseNestedLevel(level1ItemMap, level1Map, level1Field, level2Field)
				}
			}
		}
	}
}

func (s *SharedFunctionService) processLevel4Data(item map[string]interface{}, result *orderedmap.OrderedMap, parentKey string, aggregationFields []string) {
	if len(aggregationFields) < 4 {
		return
	}

	level1Field := aggregationFields[1]
	level2Field := aggregationFields[2]
	level3Field := aggregationFields[3]
	level1DataKey := fmt.Sprintf("%sData", level1Field)

	level1DataArray, exists := item[level1DataKey]
	if !exists {
		return
	}

	parentMapValue, _ := result.Get(parentKey)
	parentMap := parentMapValue.(*orderedmap.OrderedMap)

	if _, exists := parentMap.Get(level1Field); !exists {
		parentMap.Set(level1Field, orderedmap.NewOrderedMap())
	}

	level1MapValue, _ := parentMap.Get(level1Field)
	level1Map := level1MapValue.(*orderedmap.OrderedMap)

	if dataSlice, ok := level1DataArray.([]interface{}); ok {
		for _, level1Item := range dataSlice {
			if level1DataMap, ok := level1Item.(map[string]interface{}); ok {
				level1Name, _ := level1DataMap["field1Name"].(string)
				level1Count := s.parseIntFromInterface(level1DataMap["field1Count"])
				level2Data, _ := level1DataMap["field2Data"].([]interface{})

				if level1Name != "" {
					level1Entry := orderedmap.NewOrderedMap()
					level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

					if len(level2Data) > 0 {
						level2Map := orderedmap.NewOrderedMap()
						for _, level2Item := range level2Data {
							if level2ItemMap, ok := level2Item.(map[string]interface{}); ok {
								level2Name, _ := level2ItemMap["field2Name"].(string)
								level2Count := s.parseIntFromInterface(level2ItemMap["field2Count"])
								level3Data, _ := level2ItemMap["field3Data"].([]interface{})

								if level2Name == "" {
									level2Name, _ = level2ItemMap["value"].(string)
									level2Count = s.parseIntFromInterface(level2ItemMap["count"])
								}

								if level2Name != "" {
									level2Entry := orderedmap.NewOrderedMap()
									level2Entry.Set(fmt.Sprintf("%sCount", level2Field), level2Count)

									if len(level3Data) > 0 {
										level3Map := orderedmap.NewOrderedMap()
										for _, level3Item := range level3Data {
											if level3ItemMap, ok := level3Item.(map[string]interface{}); ok {
												level3Name, _ := level3ItemMap["value"].(string)
												level3Count := s.parseIntFromInterface(level3ItemMap["count"])

												if level3Name != "" {
													level3Entry := orderedmap.NewOrderedMap()
													level3Entry.Set(fmt.Sprintf("%sCount", level3Field), level3Count)
													level3Map.Set(level3Name, level3Entry)
												}
											}
										}
										if level3Map.Len() > 0 {
											level2Entry.Set(level3Field, level3Map)
										}
									}

									level2Map.Set(level2Name, level2Entry)
								}
							}
						}
						if level2Map.Len() > 0 {
							level1Entry.Set(level2Field, level2Map)
						}
					}

					level1Map.Set(level1Name, level1Entry)
				}
			} else if level1Str, ok := level1Item.(string); ok {
				if strings.Contains(level1Str, "|||||") {
					parts := strings.Split(level1Str, "|||||")
					if len(parts) >= 3 {
						level1Name := strings.TrimSpace(parts[0])
						if level1Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level2DataStr := strings.TrimSpace(parts[2])
							level1Entry := orderedmap.NewOrderedMap()
							level1Entry.Set(fmt.Sprintf("%sCount", level1Field), level1Count)

							if level2DataStr != "" {
								level2Map := orderedmap.NewOrderedMap()
								level2CountKey := fmt.Sprintf("%sCount", level2Field)

								level2Items := strings.Split(level2DataStr, " ")
								for _, level2ItemStr := range level2Items {
									level2ItemStr = strings.TrimSpace(level2ItemStr)
									if level2ItemStr == "" {
										continue
									}

									if strings.Contains(level2ItemStr, "|||") {
										level2Parts := strings.Split(level2ItemStr, "|||")
										if len(level2Parts) >= 3 {
											level2Name := strings.TrimSpace(level2Parts[0])
											if level2Count, err := strconv.Atoi(strings.TrimSpace(level2Parts[1])); err == nil {
												level3DataStr := strings.TrimSpace(level2Parts[2])
												level2Entry := orderedmap.NewOrderedMap()
												level2Entry.Set(level2CountKey, level2Count)

												if level3DataStr != "" {
													level3Map := orderedmap.NewOrderedMap()
													level3CountKey := fmt.Sprintf("%sCount", level3Field)

													level3Items := strings.Fields(level3DataStr)
													for _, level3Item := range level3Items {
														if strings.Contains(level3Item, "|") {
															level3Parts := strings.Split(level3Item, "|")
															if len(level3Parts) >= 2 {
																level3Name := strings.TrimSpace(level3Parts[0])
																if level3Count, err := strconv.Atoi(strings.TrimSpace(level3Parts[1])); err == nil {
																	level3Entry := orderedmap.NewOrderedMap()
																	level3Entry.Set(level3CountKey, level3Count)
																	level3Map.Set(level3Name, level3Entry)
																}
															}
														}
													}

													if level3Map.Len() > 0 {
														level2Entry.Set(level3Field, level3Map)
													}
												}

												level2Map.Set(level2Name, level2Entry)
											}
										}
									}
								}

								if level2Map.Len() > 0 {
									level1Entry.Set(level2Field, level2Map)
								}
							}

							level1Map.Set(level1Name, level1Entry)
						}
					}
				}
			} else {
				unwrappedData := s.unwrapNestedArrays(level1Item)
				if level1ItemMap, ok := unwrappedData.(map[string]interface{}); ok {
					s.parseNestedLevel4(level1ItemMap, level1Map, level1Field, level2Field, level3Field)
				}
			}
		}
	}
}

func (s *SharedFunctionService) parseIntFromInterface(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

func (s *SharedFunctionService) unwrapNestedArrays(data interface{}) interface{} {
	current := data

	for {
		if arr, ok := current.([]interface{}); ok && len(arr) > 0 {
			current = arr[0]
		} else {
			break
		}
	}

	return current
}

func (s *SharedFunctionService) parseNestedLevel(itemMap map[string]interface{}, parentMap *orderedmap.OrderedMap, level1Field, level2Field string) {
	var itemName string
	var itemCount int
	var nestedData []interface{}

	level1CountKey := fmt.Sprintf("%sCount", level1Field)
	level2DataKey := fmt.Sprintf("%sData", level2Field)

	for key, value := range itemMap {
		if key == level1CountKey {
			itemCount = s.parseIntFromInterface(value)
		} else if key == level2DataKey {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key == "" {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key != "country" && !strings.HasSuffix(key, "Count") && !strings.HasSuffix(key, "Data") {
			itemName = fmt.Sprintf("%v", value)
		}
	}

	if itemName == "" {
		for key, value := range itemMap {
			if key != "country" && key != "" &&
				!strings.HasSuffix(key, "Count") &&
				!strings.HasSuffix(key, "Data") {
				itemName = fmt.Sprintf("%v", value)
				break
			}
		}
	}

	if itemName == "" {
		return
	}

	itemData := orderedmap.NewOrderedMap()
	itemData.Set(level1CountKey, itemCount)

	if len(nestedData) > 0 {
		level2Map := orderedmap.NewOrderedMap()
		level2CountKey := fmt.Sprintf("%sCount", level2Field)

		sortedNestedData := s.sortStringArrayByCount(nestedData)

		for _, level2Item := range sortedNestedData {
			if level2Str, ok := level2Item.(string); ok {
				var parts []string
				if strings.Contains(level2Str, "|") {
					parts = strings.Split(level2Str, "|")
				} else {
					parts = strings.Fields(level2Str)
				}

				if len(parts) >= 2 {
					level2Name := strings.TrimSpace(parts[0])
					if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						level2Entry := orderedmap.NewOrderedMap()
						level2Entry.Set(level2CountKey, level2Count)
						level2Map.Set(level2Name, level2Entry)
					}
				}
			} else if level2MapItem, ok := level2Item.(map[string]interface{}); ok {
				for _, v := range level2MapItem {
					if vStr, ok := v.(string); ok {
						var parts []string
						if strings.Contains(vStr, "|") {
							parts = strings.Split(vStr, "|")
						} else {
							parts = strings.Fields(vStr)
						}

						if len(parts) >= 2 {
							level2Name := strings.TrimSpace(parts[0])
							if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
								level2Entry := orderedmap.NewOrderedMap()
								level2Entry.Set(level2CountKey, level2Count)
								level2Map.Set(level2Name, level2Entry)
							}
						}
					}
				}
			}
		}

		if level2Map.Len() > 0 {
			itemData.Set(level2Field, level2Map)
		}
	}

	parentMap.Set(itemName, itemData)
}

func (s *SharedFunctionService) parseNestedLevel4(itemMap map[string]interface{}, parentMap *orderedmap.OrderedMap, level1Field, level2Field, level3Field string) {
	var itemName string
	var itemCount int
	var nestedData []interface{}

	level1CountKey := fmt.Sprintf("%sCount", level1Field)
	level2DataKey := fmt.Sprintf("%sData", level2Field)

	for key, value := range itemMap {
		if key == level1CountKey {
			itemCount = s.parseIntFromInterface(value)
		} else if key == level2DataKey {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key == "" {
			if arr, ok := value.([]interface{}); ok {
				nestedData = arr
			}
		} else if key != "country" && !strings.HasSuffix(key, "Count") && !strings.HasSuffix(key, "Data") {
			itemName = fmt.Sprintf("%v", value)
		}
	}

	if itemName == "" {
		for key, value := range itemMap {
			if key != "country" && key != "" &&
				!strings.HasSuffix(key, "Count") &&
				!strings.HasSuffix(key, "Data") {
				itemName = fmt.Sprintf("%v", value)
				break
			}
		}
	}

	if itemName == "" {
		return
	}

	itemData := orderedmap.NewOrderedMap()
	itemData.Set(level1CountKey, itemCount)

	if len(nestedData) > 0 {
		level2Map := orderedmap.NewOrderedMap()
		level2CountKey := fmt.Sprintf("%sCount", level2Field)

		sortedNestedData := s.sortStringArrayByCount(nestedData)

		for _, level2Item := range sortedNestedData {
			if level2Str, ok := level2Item.(string); ok {
				if strings.Contains(level2Str, "|||") {
					parts := strings.Split(level2Str, "|||")
					if len(parts) >= 3 {
						level2Name := strings.TrimSpace(parts[0])
						if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level3DataStr := strings.TrimSpace(parts[2])
							level2Entry := orderedmap.NewOrderedMap()
							level2Entry.Set(level2CountKey, level2Count)

							if level3DataStr != "" {
								level3Map := orderedmap.NewOrderedMap()
								level3CountKey := fmt.Sprintf("%sCount", level3Field)

								level3Items := strings.Fields(level3DataStr)
								for _, level3Item := range level3Items {
									if strings.Contains(level3Item, "|") {
										level3Parts := strings.Split(level3Item, "|")
										if len(level3Parts) >= 2 {
											level3Name := strings.TrimSpace(level3Parts[0])
											if level3Count, err := strconv.Atoi(strings.TrimSpace(level3Parts[1])); err == nil {
												level3Entry := orderedmap.NewOrderedMap()
												level3Entry.Set(level3CountKey, level3Count)
												level3Map.Set(level3Name, level3Entry)
											}
										}
									}
								}

								if level3Map.Len() > 0 {
									level2Entry.Set(level3Field, level3Map)
								}
							}

							level2Map.Set(level2Name, level2Entry)
						}
					}
				} else {
					var parts []string
					if strings.Contains(level2Str, "|") {
						parts = strings.Split(level2Str, "|")
					} else {
						parts = strings.Fields(level2Str)
					}

					if len(parts) >= 2 {
						level2Name := strings.TrimSpace(parts[0])
						if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
							level2Entry := orderedmap.NewOrderedMap()
							level2Entry.Set(level2CountKey, level2Count)
							level2Map.Set(level2Name, level2Entry)
						}
					}
				}
			} else if level2MapItem, ok := level2Item.(map[string]interface{}); ok {
				for _, v := range level2MapItem {
					if vStr, ok := v.(string); ok {
						var parts []string
						if strings.Contains(vStr, "|") {
							parts = strings.Split(vStr, "|")
						} else {
							parts = strings.Fields(vStr)
						}

						if len(parts) >= 2 {
							level2Name := strings.TrimSpace(parts[0])
							if level2Count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
								level2Entry := orderedmap.NewOrderedMap()
								level2Entry.Set(level2CountKey, level2Count)
								level2Map.Set(level2Name, level2Entry)
							}
						}
					}
				}
			}
		}

		if level2Map.Len() > 0 {
			itemData.Set(level2Field, level2Map)
		}
	}

	parentMap.Set(itemName, itemData)
}

func (s *SharedFunctionService) convertOrderedMapToSlice(om *orderedmap.OrderedMap) []map[string]interface{} {
	var result []map[string]interface{}

	for _, key := range om.Keys() {
		value, _ := om.Get(key)
		keyStr := fmt.Sprintf("%v", key)

		item := map[string]interface{}{
			keyStr: s.convertOrderedMapToRegularMap(value),
		}
		result = append(result, item)
	}

	return result
}

func (s *SharedFunctionService) sortFlatDataByCount(flatData []map[string]interface{}, parentField string) {
	countKey := fmt.Sprintf("%sCount", parentField)

	sort.Slice(flatData, func(i, j int) bool {
		countI := 0
		countJ := 0

		if countValue, exists := flatData[i][countKey]; exists {
			if countInt, ok := countValue.(int); ok {
				countI = countInt
			}
		}

		if countValue, exists := flatData[j][countKey]; exists {
			if countInt, ok := countValue.(int); ok {
				countJ = countInt
			}
		}

		return countI > countJ
	})
}

func (s *SharedFunctionService) sortDataSliceByCount(dataSlice []interface{}, countFieldName string) []interface{} {

	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if itemMap, ok := sortedSlice[i].(map[string]interface{}); ok {
			if countValue, exists := itemMap[countFieldName]; exists {
				countI = s.parseIntFromInterface(countValue)
			}
		} else if itemArray, ok := sortedSlice[i].([]interface{}); ok && len(itemArray) >= 2 {
			countI = s.parseIntFromInterface(itemArray[1])
		}

		if itemMap, ok := sortedSlice[j].(map[string]interface{}); ok {
			if countValue, exists := itemMap[countFieldName]; exists {
				countJ = s.parseIntFromInterface(countValue)
			}
		} else if itemArray, ok := sortedSlice[j].([]interface{}); ok && len(itemArray) >= 2 {
			countJ = s.parseIntFromInterface(itemArray[1])
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *SharedFunctionService) sortStringArrayByCount(dataSlice []interface{}) []interface{} {
	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if strI, ok := sortedSlice[i].(string); ok {
			var parts []string
			if strings.Contains(strI, "|") {
				parts = strings.Split(strI, "|")
			} else {
				parts = strings.Fields(strI)
			}
			if len(parts) >= 2 {
				if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					countI = count
				}
			}
		}

		if strJ, ok := sortedSlice[j].(string); ok {
			var parts []string
			if strings.Contains(strJ, "|") {
				parts = strings.Split(strJ, "|")
			} else {
				parts = strings.Fields(strJ)
			}
			if len(parts) >= 2 {
				if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					countJ = count
				}
			}
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *SharedFunctionService) sortArrayDataByCount(dataSlice []interface{}) []interface{} {
	sortedSlice := make([]interface{}, len(dataSlice))
	copy(sortedSlice, dataSlice)

	sort.Slice(sortedSlice, func(i, j int) bool {
		countI := 0
		countJ := 0

		if arrayI, ok := sortedSlice[i].([]interface{}); ok && len(arrayI) >= 2 {
			countI = s.parseIntFromInterface(arrayI[1])
		}

		if arrayJ, ok := sortedSlice[j].([]interface{}); ok && len(arrayJ) >= 2 {
			countJ = s.parseIntFromInterface(arrayJ[1])
		}

		return countI > countJ
	})

	return sortedSlice
}

func (s *SharedFunctionService) convertOrderedMapToRegularMap(value interface{}) interface{} {
	if nestedOM, ok := value.(*orderedmap.OrderedMap); ok {
		result := make(map[string]interface{})
		var countFields []string
		var nestedFields []string

		for _, key := range nestedOM.Keys() {
			keyStr := fmt.Sprintf("%v", key)
			if strings.HasSuffix(keyStr, "Count") {
				countFields = append(countFields, keyStr)
			} else {
				nestedFields = append(nestedFields, keyStr)
			}
		}

		for _, key := range countFields {
			nestedValue, _ := nestedOM.Get(key)
			result[key] = nestedValue
		}

		for _, key := range nestedFields {
			nestedValue, _ := nestedOM.Get(key)
			if nestedNestedOM, ok := nestedValue.(*orderedmap.OrderedMap); ok {
				sortedNested := s.convertNestedOrderedMapToSortedRegularMap(nestedNestedOM)
				result[key] = sortedNested
			} else {
				result[key] = s.convertOrderedMapToRegularMap(nestedValue)
			}
		}

		return result
	}

	return value
}

func (s *SharedFunctionService) convertNestedOrderedMapToSortedRegularMap(om *orderedmap.OrderedMap) interface{} {
	type KeyValue struct {
		Key   string
		Value interface{}
		Count int
	}

	var keyValues []KeyValue
	for _, key := range om.Keys() {
		value, _ := om.Get(key)
		keyStr := fmt.Sprintf("%v", key)

		count := 0
		if nestedOM, ok := value.(*orderedmap.OrderedMap); ok {
			for _, nestedKey := range nestedOM.Keys() {
				nestedKeyStr := fmt.Sprintf("%v", nestedKey)
				if strings.HasSuffix(nestedKeyStr, "Count") {
					if countValue, exists := nestedOM.Get(nestedKey); exists {
						if countInt, ok := countValue.(int); ok {
							count = countInt
							break
						}
					}
				}
			}
		}

		keyValues = append(keyValues, KeyValue{
			Key:   keyStr,
			Value: value,
			Count: count,
		})
	}

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Count > keyValues[j].Count
	})

	orderedKeys := make([]string, len(keyValues))
	values := make(map[string]interface{})

	for i, kv := range keyValues {
		orderedKeys[i] = kv.Key
		values[kv.Key] = s.convertOrderedMapToRegularMap(kv.Value)
	}

	return OrderedJSONMap{
		Keys:   orderedKeys,
		Values: values,
	}
}

func (s *SharedFunctionService) buildNestedAggregationQuery(parentField string, nestedFields []string, pagination models.PaginationDto, filterFields models.FilterDataDto) (string, error) {
	parentLimit := pagination.Limit
	if parentLimit == 0 {
		parentLimit = 20
	}
	parentOffset := pagination.Offset
	nestedLimit := 5

	fieldMapping := map[string]string{
		"country":  "ee.edition_country as country",
		"city":     "ee.edition_city_name as city",
		"month":    s.buildMultiDayMonthSelect(),
		"date":     s.buildMultiDayDateSelect(),
		"category": "c.name as category",
		"tag":      "t.name as tag",
	}

	var cteClauses []string
	var previousCTE string

	today := time.Now().Format("2006-01-02")
	editionFilterConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		"edition_type = 'current_edition'",
		fmt.Sprintf("end_date >= '%s'", today),
	}

	queryResult, err := s.buildClickHouseQuery(filterFields)
	if err != nil {
		return "", err
	}

	if queryResult.NeedsVisitorJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_visitors AS (SELECT event_id FROM testing_db.event_visitors_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.VisitorWhereConditions, " AND ")))
		previousCTE = "filtered_visitors"
	}
	if queryResult.NeedsSpeakerJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_speakers AS (SELECT event_id FROM testing_db.event_speaker_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.SpeakerWhereConditions, " AND ")))
		previousCTE = "filtered_speakers"
	}
	if queryResult.NeedsExhibitorJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_exhibitors AS (SELECT event_id FROM testing_db.event_exhibitor_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.ExhibitorWhereConditions, " AND ")))
		previousCTE = "filtered_exhibitors"
	}
	if queryResult.NeedsSponsorJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_sponsors AS (SELECT event_id FROM testing_db.event_sponsors_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.SponsorWhereConditions, " AND ")))
		previousCTE = "filtered_sponsors"
	}
	if queryResult.NeedsCategoryJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_categories AS (SELECT event FROM testing_db.event_category_ch WHERE %s GROUP BY event)", strings.Join(queryResult.CategoryWhereConditions, " AND ")))
		previousCTE = "filtered_categories"
	}
	if queryResult.NeedsTypeJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_types AS (SELECT event_id FROM testing_db.event_type_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.TypeWhereConditions, " AND ")))
		previousCTE = "filtered_types"
	}
	if queryResult.NeedsEventRankingJoin {
		preEventFilterConditions := []string{
			s.buildPublishedCondition(filterFields),
			s.buildStatusCondition(filterFields),
			"edition_type = 'current_edition'",
		}

		hasUserEndDateFilter := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != ""
		if !hasUserEndDateFilter {
			preEventFilterConditions = append(preEventFilterConditions, fmt.Sprintf("end_date >= '%s'", today))
		}

		preEventFilterWhereClause := strings.Join(preEventFilterConditions, " AND ")

		var preEventFilterCTE string
		if previousCTE != "" {
			var selectColumn string
			if previousCTE == "filtered_categories" {
				selectColumn = "event"
			} else {
				selectColumn = "event_id"
			}
			preEventFilterCTE = fmt.Sprintf(`pre_event_filter AS (
			SELECT event_id, edition_id
			FROM testing_db.event_edition_ch AS ee
			WHERE event_id IN (SELECT %s FROM %s)
			AND %s
			GROUP BY event_id, edition_id
			ORDER BY event_id ASC
		)`, selectColumn, previousCTE, preEventFilterWhereClause)
		} else {
			preEventFilterCTE = fmt.Sprintf(`pre_event_filter AS (
			SELECT event_id, edition_id
			FROM testing_db.event_edition_ch AS ee
			WHERE %s
			GROUP BY event_id, edition_id
			ORDER BY event_id ASC
		)`, preEventFilterWhereClause)
		}

		cteClauses = append(cteClauses, preEventFilterCTE)
		currentMonth := time.Now().Month()
		currentMonthCondition := fmt.Sprintf("MONTH(created) = %d", currentMonth)

		eventRankingConditions := []string{currentMonthCondition}

		hasCountryFilter := len(filterFields.ParsedCountry) > 0
		hasCategoryFilter := len(filterFields.ParsedCategory) > 0

		if hasCountryFilter {
			countries := make([]string, len(filterFields.ParsedCountry))
			for i, country := range filterFields.ParsedCountry {
				countries[i] = fmt.Sprintf("'%s'", country)
			}
			eventRankingConditions = append(eventRankingConditions, fmt.Sprintf("country IN (%s)", strings.Join(countries, ",")))
		}

		if hasCategoryFilter {
			categories := make([]string, len(filterFields.ParsedCategory))
			for i, category := range filterFields.ParsedCategory {
				categories[i] = fmt.Sprintf("'%s'", category)
			}
			eventRankingConditions = append(eventRankingConditions, fmt.Sprintf("category_name IN (%s)", strings.Join(categories, ",")))
		}

		if !hasCountryFilter && !hasCategoryFilter {
			eventRankingConditions = append(eventRankingConditions, "((country = '' AND category_name = ''))")
		}

		if len(queryResult.EventRankingWhereConditions) > 0 {
			eventRankingConditions = append(eventRankingConditions, queryResult.EventRankingWhereConditions...)
		}

		eventRankingLimit := filterFields.ParsedEventRanking[0]

		filteredEventRankingCTE := fmt.Sprintf(`filtered_event_ranking AS (
			SELECT event_id
			FROM testing_db.event_ranking_ch
			WHERE event_id IN (SELECT event_id FROM pre_event_filter)
			AND %s
			GROUP BY event_id LIMIT %s
		)`, strings.Join(eventRankingConditions, " AND "), eventRankingLimit)

		cteClauses = append(cteClauses, filteredEventRankingCTE)
	}

	if queryResult.needsDesignationJoin {
		cteClauses = append(cteClauses, fmt.Sprintf("filtered_designations AS (SELECT event_id FROM testing_db.event_designation_ch WHERE %s GROUP BY event_id)", strings.Join(queryResult.JobCompositeWhereConditions, " AND ")))
	}

	hasUserEndDateFilter := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != ""

	if hasUserEndDateFilter {
		var filteredConditions []string
		for _, condition := range editionFilterConditions {
			if !strings.Contains(condition, fmt.Sprintf("end_date >= '%s'", today)) {
				filteredConditions = append(filteredConditions, condition)
			}
		}
		editionFilterConditions = filteredConditions
	}

	if queryResult.WhereClause != "" && strings.TrimSpace(queryResult.WhereClause) != "" {
		correctedWhereClause := queryResult.WhereClause
		correctedWhereClause = regexp.MustCompile(`\be\.`).ReplaceAllString(correctedWhereClause, "ee.")
		correctedWhereClause = strings.TrimPrefix(strings.TrimPrefix(correctedWhereClause, "AND "), "and ")
		editionFilterConditions = append(editionFilterConditions, correctedWhereClause)
	}

	if queryResult.SearchClause != "" && strings.TrimSpace(queryResult.SearchClause) != "" {
		correctedSearchClause := queryResult.SearchClause
		correctedSearchClause = regexp.MustCompile(`\be\.`).ReplaceAllString(correctedSearchClause, "ee.")
		correctedSearchClause = strings.TrimPrefix(strings.TrimPrefix(correctedSearchClause, "AND "), "and ")
		editionFilterConditions = append(editionFilterConditions, correctedSearchClause)
	}
	if len(nestedFields) == 0 {
		return s.buildHierarchicalNestedQuery(parentField, nestedFields, parentLimit, parentOffset, nestedLimit, cteClauses, fieldMapping, editionFilterConditions)
	}
	return s.buildHierarchicalNestedQuery(parentField, nestedFields, parentLimit, parentOffset, nestedLimit, cteClauses, fieldMapping, editionFilterConditions)
}

func (s *SharedFunctionService) buildHierarchicalNestedQuery(parentField string, nestedFields []string, parentLimit int, parentOffset int, nestedLimit int, cteClauses []string, fieldMapping map[string]string, editionFilterConditions []string) (string, error) {
	query := "WITH "
	allFields := append([]string{parentField}, nestedFields...)

	if len(cteClauses) > 0 {
		query += strings.Join(cteClauses, ",\n") + ",\n"
	}

	baseSelect := s.buildFieldSelect(allFields, fieldMapping)
	baseFrom := s.buildFieldFrom(allFields, cteClauses)
	baseWhere := s.buildFieldWhere(allFields, editionFilterConditions)

	needsMultiDayExpansion := s.needsMultiDayExpansion(allFields)

	if len(nestedFields) == 0 {
		singleFieldSelect := s.buildSingleFieldSelect(parentField, fieldMapping)

		if needsMultiDayExpansion && (parentField == "date" || parentField == "month") {
			multiDaySelect := s.buildMultiDayFieldSelect([]string{parentField}, fieldMapping)
			query += fmt.Sprintf(`base_data AS (
		SELECT
			%s,
			count(*) as %sCount
		FROM (
			SELECT 
				%s,
				ee.event_id,
				ee.edition_id
			%s
			%s
		)
		GROUP BY %s
		ORDER BY %sCount DESC
		LIMIT %d OFFSET %d
	)`, parentField, parentField, multiDaySelect, baseFrom, baseWhere, parentField, parentField, parentLimit, parentOffset)
		} else {
			query += fmt.Sprintf(`base_data AS (
		SELECT
			%s,
			count(*) as %sCount
		%s
		%s
		GROUP BY %s
		ORDER BY %sCount DESC
		LIMIT %d OFFSET %d
	)`, singleFieldSelect, parentField, baseFrom, baseWhere, parentField, parentField, parentLimit, parentOffset)
		}
	} else {
		if needsMultiDayExpansion {
			multiDaySelect := s.buildMultiDayFieldSelect(allFields, fieldMapping)
			var outerFields []string
			for _, field := range allFields {
				outerFields = append(outerFields, field)
			}
			outerSelect := strings.Join(outerFields, ",\n\t\t")

			query += fmt.Sprintf(`base_data AS (
		SELECT
			%s
		FROM (
			SELECT 
				%s,
				ee.event_id,
				ee.edition_id
			%s
			%s
		)
	)`, outerSelect, multiDaySelect, baseFrom, baseWhere)
		} else {
			query += fmt.Sprintf(`base_data AS (
		SELECT
			%s
		%s
		%s
	)`, baseSelect, baseFrom, baseWhere)
		}

		hierarchy := s.buildHierarchyStructure(parentField, nestedFields, parentLimit, parentOffset, nestedLimit)
		query += "\n" + hierarchy
	}

	query += fmt.Sprintf(`
SELECT
    %s,
    %sCount`, parentField, parentField)

	if len(nestedFields) > 0 {
		query += fmt.Sprintf(`,
    %sData`, nestedFields[0])
	}

	var finalCteName string
	if len(nestedFields) == 0 {
		finalCteName = "base_data"
	} else {
		finalCteName = "hierarchical_nested"
	}
	query += fmt.Sprintf(`
FROM %s`, finalCteName)

	return query, nil
}

func (s *SharedFunctionService) buildFieldSelect(fields []string, fieldMapping map[string]string) string {
	selects := []string{"ee.event_id", "ee.edition_id"}

	for _, field := range fields {
		switch field {
		case "category":
			selects = append(selects, "c.name as category")
		case "tag":
			selects = append(selects, "t.name as tag")
		default:
			if dbField, exists := fieldMapping[field]; exists {
				selects = append(selects, dbField)
			}
		}
	}

	return strings.Join(selects, ",\n\t\t")
}

func (s *SharedFunctionService) buildSingleFieldSelect(field string, fieldMapping map[string]string) string {
	switch field {
	case "category":
		return "c.name as category"
	case "tag":
		return "t.name as tag"
	default:
		if dbField, exists := fieldMapping[field]; exists {
			return dbField
		}
		return ""
	}
}

func (s *SharedFunctionService) buildFieldFrom(fields []string, cteClauses []string) string {
	from := "FROM testing_db.event_edition_ch ee"

	hasCategory := s.contains(fields, "category")
	hasTag := s.contains(fields, "tag")

	if hasCategory && hasTag {
		from += " INNER JOIN testing_db.event_category_ch c ON ee.event_id = c.event AND c.is_group = 1"
		from += " INNER JOIN testing_db.event_category_ch t ON ee.event_id = t.event AND t.is_group = 0"
	} else if hasCategory {
		from += " INNER JOIN testing_db.event_category_ch c ON ee.event_id = c.event"
	} else if hasTag {
		from += " INNER JOIN testing_db.event_category_ch t ON ee.event_id = t.event"
	}

	if s.containsCTE(cteClauses, "filtered_visitors") {
		from += " INNER JOIN filtered_visitors fv ON ee.event_id = fv.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_speakers") {
		from += " INNER JOIN filtered_speakers fs ON ee.event_id = fs.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_exhibitors") {
		from += " INNER JOIN filtered_exhibitors fe ON ee.event_id = fe.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_sponsors") {
		from += " INNER JOIN filtered_sponsors fsp ON ee.event_id = fsp.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_categories") {
		from += " INNER JOIN filtered_categories fc ON ee.event_id = fc.event"
	}
	if s.containsCTE(cteClauses, "filtered_types") {
		from += " INNER JOIN filtered_types ft ON ee.event_id = ft.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_event_ranking") {
		from += " INNER JOIN filtered_event_ranking fer ON ee.event_id = fer.event_id"
	}

	return from
}

func (s *SharedFunctionService) buildFieldWhere(fields []string, editionFilterConditions []string) string {
	conditionsWithAliases := make([]string, len(editionFilterConditions))
	for i, condition := range editionFilterConditions {
		condition = strings.ReplaceAll(condition, "published", "ee.published")
		condition = strings.ReplaceAll(condition, "status", "ee.status")
		condition = strings.ReplaceAll(condition, "edition_type", "ee.edition_type")
		condition = strings.ReplaceAll(condition, "end_date", "ee.end_date")
		condition = strings.ReplaceAll(condition, "start_date", "ee.start_date")
		condition = strings.ReplaceAll(condition, "event_avgRating", "ee.event_avgRating")
		condition = strings.ReplaceAll(condition, "event_frequency", "ee.event_frequency")
		condition = strings.ReplaceAll(condition, "ee.ee.", "ee.")
		conditionsWithAliases[i] = condition
	}

	where := fmt.Sprintf("WHERE %s", strings.Join(conditionsWithAliases, "\n      AND "))

	hasCategory := s.contains(fields, "category")
	hasTag := s.contains(fields, "tag")

	if hasCategory && !hasTag {
		where += "\n      AND c.is_group = 1"
	}
	if hasTag && !hasCategory {
		where += "\n      AND t.is_group = 0"
	}

	return where
}

func (s *SharedFunctionService) contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (s *SharedFunctionService) containsCTE(cteClauses []string, cteName string) bool {
	for _, cte := range cteClauses {
		if strings.Contains(cte, cteName) {
			return true
		}
	}
	return false
}

func (s *SharedFunctionService) buildHierarchyStructure(parentField string, nestedFields []string, parentLimit int, parentOffset int, nestedLimit int) string {
	if len(nestedFields) == 0 {
		return ""
	} else if len(nestedFields) == 1 {
		return fmt.Sprintf(`,
			hierarchical_nested AS (
				SELECT 
					%s,
					sum(%sCount) AS %sCount,
					groupArray(arrayStringConcat(array(%s, toString(%sCount)), '|')) AS %sData
				FROM (
					SELECT 
						%s,
						%s,
						count(*) AS %sCount
					FROM base_data
					GROUP BY %s, %s
					ORDER BY %sCount DESC
					LIMIT %d BY %s
				)
				GROUP BY %s
				ORDER BY %sCount DESC
				LIMIT %d OFFSET %d
			)`,
			parentField, nestedFields[0], parentField, nestedFields[0], nestedFields[0], nestedFields[0],
			parentField, nestedFields[0], nestedFields[0], parentField, nestedFields[0], nestedFields[0], nestedLimit, parentField,
			parentField, parentField, parentLimit, parentOffset)
	} else if len(nestedFields) == 2 {
		return fmt.Sprintf(`,
			hierarchical_nested AS (
				SELECT 
					%s,
					sum(%sCount) AS %sCount,
					groupArray(arrayStringConcat(array(%s, toString(%sCount), arrayStringConcat(%sData, ' ')), '|||')) AS %sData
				FROM (
					SELECT 
						%s,
						%s,
						sum(%sCount) AS %sCount,
						groupArray(arrayStringConcat(array(%s, toString(%sCount)), '|')) AS %sData
					FROM (
						SELECT 
							%s,
							%s,
							%s,
							count(*) AS %sCount
						FROM base_data
						GROUP BY %s, %s, %s
						ORDER BY %sCount DESC
						LIMIT %d BY %s, %s
					)
					GROUP BY %s, %s
					ORDER BY %sCount DESC
					LIMIT %d BY %s
				)
				GROUP BY %s
				ORDER BY %sCount DESC
				LIMIT %d OFFSET %d
			)`,
			parentField, nestedFields[0], parentField, nestedFields[0], nestedFields[0], nestedFields[1], nestedFields[0],
			parentField, nestedFields[0], nestedFields[1], nestedFields[0], nestedFields[1], nestedFields[1], nestedFields[1],
			parentField, nestedFields[0], nestedFields[1], nestedFields[1], parentField, nestedFields[0], nestedFields[1], nestedFields[1], nestedLimit, parentField, nestedFields[0],
			parentField, nestedFields[0], nestedFields[0], nestedLimit, parentField,
			parentField, parentField, parentLimit, parentOffset)
	} else if len(nestedFields) == 3 {
		return fmt.Sprintf(`,
			hierarchical_nested AS (
				SELECT 
					%s,
					sum(%sCount) AS %sCount,
					groupArray(arrayStringConcat(array(%s, toString(%sCount), arrayStringConcat(%sData, ' ')), '|||||')) AS %sData
				FROM (
					SELECT 
						%s,
						%s,
						sum(%sCount) AS %sCount,
						groupArray(arrayStringConcat(array(%s, toString(%sCount), arrayStringConcat(%sData, ' ')), '|||')) AS %sData
					FROM (
						SELECT
							%s,
							%s,
							%s,
							sum(%sCount) AS %sCount,
							groupArray(arrayStringConcat(array(%s, toString(%sCount)), '|')) AS %sData
						FROM (
							SELECT
								%s,
								%s,
								%s,
								%s,
								count(*) AS %sCount
							FROM base_data
							GROUP BY %s, %s, %s, %s
							ORDER BY %sCount DESC
							LIMIT %d BY %s, %s, %s
						)
						GROUP BY %s, %s, %s
						ORDER BY %sCount DESC
						LIMIT %d BY %s, %s
					)
					GROUP BY %s, %s
					ORDER BY %sCount DESC
					LIMIT %d BY %s
				)
				GROUP BY %s
				ORDER BY %sCount DESC
				LIMIT %d OFFSET %d
			)`,
			parentField, nestedFields[0], parentField, nestedFields[0], nestedFields[0], nestedFields[1], nestedFields[0],
			parentField, nestedFields[0], nestedFields[1], nestedFields[0], nestedFields[1], nestedFields[1], nestedFields[2], nestedFields[1],
			parentField, nestedFields[0], nestedFields[1], nestedFields[2], nestedFields[1], nestedFields[2], nestedFields[2], nestedFields[2],
			parentField, nestedFields[0], nestedFields[1], nestedFields[2], nestedFields[2], parentField, nestedFields[0], nestedFields[1], nestedFields[2], nestedFields[2], nestedLimit, parentField, nestedFields[0], nestedFields[1],
			parentField, nestedFields[0], nestedFields[1], nestedFields[1], nestedLimit, parentField, nestedFields[0],
			parentField, nestedFields[0], nestedFields[0], nestedLimit, parentField,
			parentField, parentField, parentLimit, parentOffset)
	} else {
		log.Printf("WARNING: Aggregation with %d nested fields is not supported. Maximum supported is 3 nested fields (4 total fields).", len(nestedFields))
	}

	return ""
}

func (s *SharedFunctionService) detectAggregationFields(sampleItem map[string]interface{}) []string {
	possibleFields := []string{"country", "city", "month", "date", "category", "tag", "type", "status", "edition_type"}
	var detectedFields []string

	for _, field := range possibleFields {
		if _, exists := sampleItem[field]; exists {
			detectedFields = append(detectedFields, field)
		}
	}

	for _, field := range possibleFields {
		nestedDataKey := fmt.Sprintf("%sData", field)
		if _, exists := sampleItem[nestedDataKey]; exists {
			found := false
			for _, detected := range detectedFields {
				if detected == field {
					found = true
					break
				}
			}
			if !found {
				detectedFields = append(detectedFields, field)
			}
		}
	}

	fieldOrder := []string{"country", "city", "month", "date", "category", "tag", "type", "status", "edition_type"}
	for i := 0; i < len(detectedFields)-1; i++ {
		for j := i + 1; j < len(detectedFields); j++ {
			indexA := s.indexOf(fieldOrder, detectedFields[i])
			indexB := s.indexOf(fieldOrder, detectedFields[j])
			if indexA > indexB {
				detectedFields[i], detectedFields[j] = detectedFields[j], detectedFields[i]
			}
		}
	}

	return detectedFields
}

func (s *SharedFunctionService) indexOf(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1
}

func (s *SharedFunctionService) getCountFromItem(item map[string]interface{}, field string) int {
	countKey := fmt.Sprintf("%sCount", field)
	if count, exists := item[countKey]; exists {
		return s.parseIntFromInterface(count)
	}

	if count, exists := item["count"]; exists {
		return s.parseIntFromInterface(count)
	}

	return 0
}

func (s *SharedFunctionService) formatCurrency(value float64) string {
	if value == 0 {
		return "$0"
	}

	str := fmt.Sprintf("%.2f", value)

	parts := strings.Split(str, ".")
	integerPart := parts[0]
	decimalPart := parts[1]

	var result strings.Builder
	for i, char := range integerPart {
		if i > 0 && (len(integerPart)-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(char)
	}

	return "$" + result.String() + "." + decimalPart
}

func (s *SharedFunctionService) GetCountryDataByISO(iso string) map[string]interface{} {
	for _, country := range CountryData {
		if countryID, ok := country["id"].(string); ok && countryID == iso {
			return country
		}
	}
	return nil
}
