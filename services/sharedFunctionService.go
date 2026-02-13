package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"reflect"
	"regexp"
	"search-event-go/config"
	"search-event-go/models"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type SharedFunctionService struct {
	db                   *gorm.DB
	clickhouseService    *ClickHouseService
	transformDataService *TransformDataService
	cfg                  *config.Config
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

func NewSharedFunctionService(db *gorm.DB, clickhouseService *ClickHouseService, cfg *config.Config) *SharedFunctionService {
	return &SharedFunctionService{
		db:                db,
		clickhouseService: clickhouseService,
		cfg:               cfg,
	}
}

// func (s *SharedFunctionService) matchPhraseConverter(fieldName string, searchTerm string) string {
// 	escapedTerm := strings.TrimSpace(searchTerm)
// 	if escapedTerm == "" {
// 		return ""
// 	}
//
// 	words := strings.Fields(strings.ToLower(escapedTerm))
//
// 	if len(words) == 0 {
// 		return ""
// 	}
// 	escapeSqlValue := func(word string) string {
// 		return strings.ReplaceAll(word, "'", "''")
// 	}
//
// 	escapeRegex := func(word string) string {
// 		word = strings.ReplaceAll(word, "\\", "\\\\")
// 		word = strings.ReplaceAll(word, ".", "\\.")
// 		word = strings.ReplaceAll(word, "+", "\\+")
// 		word = strings.ReplaceAll(word, "*", "\\*")
// 		word = strings.ReplaceAll(word, "?", "\\?")
// 		word = strings.ReplaceAll(word, "^", "\\^")
// 		word = strings.ReplaceAll(word, "$", "\\$")
// 		word = strings.ReplaceAll(word, "[", "\\[")
// 		word = strings.ReplaceAll(word, "]", "\\]")
// 		word = strings.ReplaceAll(word, "{", "\\{")
// 		word = strings.ReplaceAll(word, "}", "\\}")
// 		word = strings.ReplaceAll(word, "(", "\\(")
// 		word = strings.ReplaceAll(word, ")", "\\)")
// 		word = strings.ReplaceAll(word, "|", "\\|")
// 		return word
// 	}
//
// 	if len(words) == 1 {
// 		word := escapeSqlValue(words[0])
// 		return fmt.Sprintf("hasToken(lower(%s), '%s')", fieldName, word)
// 	}
// 	var conditions []string
// 	for _, word := range words {
// 		escapedWord := escapeSqlValue(word)
// 		conditions = append(conditions, fmt.Sprintf("hasToken(lower(%s), '%s')", fieldName, escapedWord))
// 	}
// 	escapedWords := make([]string, len(words))
// 	for i, word := range words {
// 		escapedWords[i] = escapeRegex(word)
// 	}
// 	regexPattern := `\b` + strings.Join(escapedWords, `\s+`) + `\b`
// 	escapedRegex := strings.ReplaceAll(regexPattern, "'", "''")
// 	conditions = append(conditions, fmt.Sprintf("match(lower(%s), '%s')", fieldName, escapedRegex))
// 	return fmt.Sprintf("(%s)", strings.Join(conditions, " AND "))
// }

func (s *SharedFunctionService) matchPhraseConverter(fieldName string, searchTerm string) string {
	escapedTerm := strings.TrimSpace(searchTerm)
	if escapedTerm == "" {
		return ""
	}

	escapeSqlValue := func(word string) string {
		return strings.ReplaceAll(word, "'", "''")
	}

	extractTokens := func(text string) []string {
		text = strings.ToLower(text)
		var tokens []string
		var currentToken strings.Builder

		for _, char := range text {
			if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
				currentToken.WriteRune(char)
			} else {
				if currentToken.Len() > 0 {
					token := currentToken.String()
					if token != "" {
						tokens = append(tokens, token)
					}
					currentToken.Reset()
				}
			}
		}
		if currentToken.Len() > 0 {
			token := currentToken.String()
			if token != "" {
				tokens = append(tokens, token)
			}
		}

		return tokens
	}

	tokens := extractTokens(escapedTerm)

	if len(tokens) == 0 {
		return ""
	}

	var conditions []string
	for _, token := range tokens {
		escapedToken := escapeSqlValue(token)
		conditions = append(conditions, fmt.Sprintf("hasToken(lower(%s), '%s')", fieldName, escapedToken))
	}

	return fmt.Sprintf("(%s)", strings.Join(conditions, " AND "))
}

func escapeILikePattern(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func (s *SharedFunctionService) speakerUserNameILikeCondition(searchTerm string) string {
	phrase := " " + strings.TrimSpace(searchTerm) + " "
	if phrase == "  " {
		return ""
	}
	escaped := escapeILikePattern(phrase)
	pattern := "%" + escaped + "%"
	columns := []string{"user_name", "speaker_title", "speaker_profile", "sourceUserName"}
	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s ILIKE '%s'", col, pattern))
	}
	return fmt.Sprintf("(%s)", strings.Join(conditions, " OR "))
}

func (s *SharedFunctionService) speakerUserCompanyILikeCondition(searchTerm string) string {
	phrase := " " + strings.TrimSpace(searchTerm) + " "
	if phrase == "  " {
		return ""
	}
	escaped := escapeILikePattern(phrase)
	pattern := "%" + escaped + "%"
	columns := []string{"user_company", "sourceCompanyName", "speaker_title", "speaker_profile"}
	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s ILIKE '%s'", col, pattern))
	}
	return fmt.Sprintf("(%s)", strings.Join(conditions, " OR "))
}

func (s *SharedFunctionService) commonUserNameILikeCondition(searchTerm string) string {
	phrase := " " + strings.TrimSpace(searchTerm) + " "
	if phrase == "  " {
		return ""
	}
	escaped := escapeILikePattern(phrase)
	return fmt.Sprintf("(user_name ILIKE '%%%s%%')", escaped)
}

func (s *SharedFunctionService) commonUserCompanyILikeCondition(searchTerm string) string {
	phrase := " " + strings.TrimSpace(searchTerm) + " "
	if phrase == "  " {
		return ""
	}
	escaped := escapeILikePattern(phrase)
	return fmt.Sprintf("(user_company ILIKE '%%%s%%')", escaped)
}

func (s *SharedFunctionService) buildUserNameUserCompanyCondition(filterFields models.FilterDataDto, useILikeLogic bool, useCommonColumnsOnly bool) string {
	hasUserNameFilter := len(filterFields.ParsedUserName) > 0
	hasUserCompanyNameFilter := len(filterFields.ParsedUserCompanyName) > 0
	if !hasUserNameFilter && !hasUserCompanyNameFilter {
		return ""
	}

	nameConditionFunc := func(term string) string {
		if useILikeLogic {
			if useCommonColumnsOnly {
				return s.commonUserNameILikeCondition(term)
			}
			return s.speakerUserNameILikeCondition(term)
		}
		return s.matchPhraseConverter("user_name", term)
	}
	companyConditionFunc := func(term string) string {
		if useILikeLogic {
			if useCommonColumnsOnly {
				return s.commonUserCompanyILikeCondition(term)
			}
			return s.speakerUserCompanyILikeCondition(term)
		}
		return s.matchPhraseConverter("user_company", term)
	}

	var finalCondition string
	if hasUserNameFilter && hasUserCompanyNameFilter {
		var allConditions []string
		minCount := len(filterFields.ParsedUserName)
		if len(filterFields.ParsedUserCompanyName) < minCount {
			minCount = len(filterFields.ParsedUserCompanyName)
		}
		for i := 0; i < minCount; i++ {
			nameCondition := nameConditionFunc(filterFields.ParsedUserName[i])
			companyCondition := companyConditionFunc(filterFields.ParsedUserCompanyName[i])
			if nameCondition != "" && companyCondition != "" {
				allConditions = append(allConditions, fmt.Sprintf("(%s AND %s)", nameCondition, companyCondition))
			}
		}
		for i := minCount; i < len(filterFields.ParsedUserName); i++ {
			if c := nameConditionFunc(filterFields.ParsedUserName[i]); c != "" {
				allConditions = append(allConditions, c)
			}
		}
		for i := minCount; i < len(filterFields.ParsedUserCompanyName); i++ {
			if c := companyConditionFunc(filterFields.ParsedUserCompanyName[i]); c != "" {
				allConditions = append(allConditions, c)
			}
		}
		if len(allConditions) > 0 {
			finalCondition = fmt.Sprintf("(%s)", strings.Join(allConditions, " OR "))
		}
	} else {
		var parts []string
		if hasUserNameFilter {
			var nameConditions []string
			for _, userName := range filterFields.ParsedUserName {
				if c := nameConditionFunc(userName); c != "" {
					nameConditions = append(nameConditions, c)
				}
			}
			if len(nameConditions) > 0 {
				parts = append(parts, fmt.Sprintf("(%s)", strings.Join(nameConditions, " OR ")))
			}
		}
		if hasUserCompanyNameFilter {
			var companyConditions []string
			for _, companyName := range filterFields.ParsedUserCompanyName {
				if c := companyConditionFunc(companyName); c != "" {
					companyConditions = append(companyConditions, c)
				}
			}
			if len(companyConditions) > 0 {
				parts = append(parts, fmt.Sprintf("(%s)", strings.Join(companyConditions, " OR ")))
			}
		}
		if len(parts) > 0 {
			finalCondition = strings.Join(parts, " AND ")
		}
	}
	return finalCondition
}

func (s *SharedFunctionService) matchWebsiteConverter(fieldName string, searchTerm string) string {
	escapedTerm := strings.TrimSpace(searchTerm)
	if escapedTerm == "" {
		return ""
	}

	escapeSqlValue := func(value string) string {
		value = strings.ReplaceAll(value, "'", "''")
		value = strings.ReplaceAll(value, "%", "\\%")
		value = strings.ReplaceAll(value, "_", "\\_")
		return value
	}

	escapedValue := escapeSqlValue(escapedTerm)
	return fmt.Sprintf("lower(%s) LIKE '%%%s%%'", fieldName, strings.ToLower(escapedValue))
}

func (s *SharedFunctionService) extractDomain(website string) string {
	website = strings.TrimSpace(website)
	if website == "" {
		return ""
	}

	website = strings.ToLower(website)
	website = strings.TrimPrefix(website, "https://")
	website = strings.TrimPrefix(website, "http://")

	website = strings.TrimPrefix(website, "www.")

	if idx := strings.IndexAny(website, "/?#"); idx != -1 {
		website = website[:idx]
	}

	if idx := strings.Index(website, ":"); idx != -1 {
		website = website[:idx]
	}

	return strings.TrimSpace(website)
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

	byPassAccess := s.ByPassAccess(userId)

	if byPassAccess {
		var allFilters []models.APIFilter
		err := s.db.Where("api_id = ? AND is_active = ?", apiId, true).
			Select("filter_name").
			Find(&allFilters).Error
		if err != nil {
			return nil, err
		}

		var allFilterNames []string
		for _, filter := range allFilters {
			allFilterNames = append(allFilterNames, filter.FilterName)
		}
		result.AllowedFilters = allFilterNames

		var allParameters []models.APIParameter
		err = s.db.Where("api_id = ? AND is_active = ? AND parameter_type = ?", apiId, true, "ADVANCED").
			Select("parameter_name").
			Find(&allParameters).Error
		if err != nil {
			return nil, err
		}

		for _, param := range allParameters {
			result.AllowedAdvancedParameters = append(result.AllowedAdvancedParameters, param.ParameterName)
		}

		log.Printf("User %s has unlimited access - skipping quota and granting access to all filters and parameters", userId)
		return &result, nil
	}

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

		var basicFilterNames []string
		for _, filter := range basicFilters {
			basicFilterNames = append(basicFilterNames, filter.FilterName)
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

func (s *SharedFunctionService) ByPassAccess(userId string) bool {
	if s.cfg == nil || s.cfg.UnlimitedAccessUserIDs == "" {
		return false
	}
	userIDs := strings.Split(s.cfg.UnlimitedAccessUserIDs, ",")
	for _, id := range userIDs {
		if strings.TrimSpace(id) == userId {
			return true
		}
	}
	return false
}

type QuotaExceededError struct {
	Message string
}

func (e *QuotaExceededError) Error() string {
	return e.Message
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

		var fieldName string

		if sort.Field == "duration" {
			if needsAnyJoin {
				fieldName = "(ee.end_date - ee.start_date)"
			} else {
				fieldName = "(end_date - start_date)"
			}
		} else {
			fieldName = sort.Field
			if needsAnyJoin {
				fieldName = fmt.Sprintf("ee.%s", fieldName)
			}
		}

		orderByPart := fmt.Sprintf("%s %s", fieldName, strings.ToUpper(sort.Order))
		orderByParts = append(orderByParts, orderByPart)
	}

	if len(orderByParts) > 0 {
		return fmt.Sprintf("ORDER BY %s", strings.Join(orderByParts, ", ")), nil
	}

	return "", nil
}

func (s *SharedFunctionService) determineQueryType(filterFields models.FilterDataDto) (string, error) {
	log.Printf("filterFields: %v", filterFields.View)
	if strings.Contains(filterFields.View, "count") {
		log.Printf("Query type determined: COUNT")
		return "COUNT", nil
	}

	if strings.Contains(filterFields.View, "calendar") {
		log.Printf("Query type determined: CALENDAR")
		return "CALENDAR", nil
	}

	if strings.Contains(filterFields.View, "trends") {
		log.Printf("Query type determined: TRENDS")
		return "TRENDS", nil
	}

	isAggregationView := strings.Contains(filterFields.View, "agg")
	isMapView := strings.Contains(filterFields.View, "map")
	isPromoteView := strings.Contains(filterFields.View, "promote")

	log.Printf("isAggregationView: %v, isMapView: %v, isPromoteView: %v, View: '%s'", isAggregationView, isMapView, isPromoteView, filterFields.View)

	if isAggregationView {
		log.Printf("Query type determined: AGGREGATION")
		return "AGGREGATION", nil
	}

	if isMapView {
		log.Printf("Query type determined: MAP")
		return "MAP", nil
	}

	if isPromoteView {
		log.Printf("Query type determined: PROMOTE")
		return "LIST", nil
	}

	log.Printf("Query type determined: LIST")
	return "LIST", nil
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
	NeedsRegionsJoin              bool
	NeedsUserIdUnionCTE           bool
	NeedsCompanyIdUnionCTE        bool
	VisitorJoinClause             string
	SpeakerJoinClause             string
	VisitorWhereConditions        []string
	SpeakerWhereConditions        []string
	UserIdWhereConditions         []string
	CompanyIdWhereConditions      []string
	ExhibitorWhereConditions      []string
	SponsorWhereConditions        []string
	OrganizerWhereConditions      []string
	CategoryWhereConditions       []string
	TypeWhereConditions           []string
	EventRankingWhereConditions   []string
	JobCompositeWhereConditions   []string
	AudienceSpreadWhereConditions []string
	RegionsWhereConditions        []string
	LocationIdsWhereConditions    []string
	CountryIdsWhereConditions     []string
	StateIdsWhereConditions       []string
	CityIdsWhereConditions        []string
	VenueIdsWhereConditions       []string
	HasRegionsFilter              bool
	HasCountryFilter              bool
	NeedsLocationIdsJoin          bool
	NeedsCountryIdsJoin           bool
	NeedsStateIdsJoin             bool
	NeedsCityIdsJoin              bool
	NeedsVenueIdsJoin             bool
}

func (s *SharedFunctionService) buildClickHouseQuery(filterFields models.FilterDataDto) (*ClickHouseQueryResult, error) {
	result := &ClickHouseQueryResult{
		VisitorWhereConditions:        make([]string, 0),
		SpeakerWhereConditions:        make([]string, 0),
		ExhibitorWhereConditions:      make([]string, 0),
		SponsorWhereConditions:        make([]string, 0),
		OrganizerWhereConditions:      make([]string, 0),
		CategoryWhereConditions:       make([]string, 0),
		TypeWhereConditions:           make([]string, 0),
		EventRankingWhereConditions:   make([]string, 0),
		JobCompositeWhereConditions:   make([]string, 0),
		AudienceSpreadWhereConditions: make([]string, 0),
		RegionsWhereConditions:        make([]string, 0),
		LocationIdsWhereConditions:    make([]string, 0),
		CountryIdsWhereConditions:     make([]string, 0),
		StateIdsWhereConditions:       make([]string, 0),
		CityIdsWhereConditions:        make([]string, 0),
		VenueIdsWhereConditions:       make([]string, 0),
		UserIdWhereConditions:         make([]string, 0),
		CompanyIdWhereConditions:      make([]string, 0),
		HasRegionsFilter:              false,
		HasCountryFilter:              false,
		NeedsLocationIdsJoin:          false,
		NeedsCountryIdsJoin:           false,
		NeedsStateIdsJoin:             false,
		NeedsCityIdsJoin:              false,
		NeedsVenueIdsJoin:             false,
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
			var likeConditions []string
			for _, val := range values {
				cleanVal := strings.Trim(val, "'")
				escapedVal := strings.ReplaceAll(strings.ToLower(cleanVal), "'", "''")
				escapedVal = strings.ReplaceAll(escapedVal, "%", "\\%")
				escapedVal = strings.ReplaceAll(escapedVal, "_", "\\_")
				likeConditions = append(likeConditions, fmt.Sprintf("lower(%s) LIKE '%%%s%%'", dbField, escapedVal))
			}
			*conditions = append(*conditions, fmt.Sprintf("(%s)", strings.Join(likeConditions, " OR ")))
		case "user_company":
			var likeConditions []string
			for _, val := range values {
				cleanVal := strings.Trim(val, "'")
				escapedVal := strings.ReplaceAll(strings.ToLower(cleanVal), "'", "''")
				escapedVal = strings.ReplaceAll(escapedVal, "%", "\\%")
				escapedVal = strings.ReplaceAll(escapedVal, "_", "\\_")
				likeConditions = append(likeConditions, fmt.Sprintf("lower(%s) LIKE '%s%%'", dbField, escapedVal))
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

	getEntityTypes := func() (hasVisitor bool, hasSpeaker bool, hasExhibitor bool, hasSponsor bool, hasOrganizer bool) {
		entities := filterFields.ParsedAdvancedSearchBy

		for _, entity := range entities {
			switch entity {
			case "visitor":
				hasVisitor = true
			case "speaker":
				hasSpeaker = true
			case "exhibitor":
				hasExhibitor = true
			case "sponsor":
				hasSponsor = true
			case "organizer":
				hasOrganizer = true
			}
		}
		return
	}

	hasVisitor, hasSpeaker, _, _, _ := getEntityTypes()

	if len(filterFields.ParsedUserId) > 0 {
		userIds := make([]string, 0, len(filterFields.ParsedUserId))
		for _, userId := range filterFields.ParsedUserId {
			userIds = append(userIds, escapeSqlValue(userId))
		}
		userIdCondition := fmt.Sprintf("user_id IN (%s)", strings.Join(userIds, ","))

		hasVisitor, hasSpeaker, _, _, _ = getEntityTypes()

		if hasVisitor && hasSpeaker {
			result.NeedsUserIdUnionCTE = true
			result.UserIdWhereConditions = append(result.UserIdWhereConditions, userIdCondition)
		} else {
			if hasVisitor {
				result.NeedsVisitorJoin = true
				result.VisitorWhereConditions = append(result.VisitorWhereConditions, userIdCondition)
			}
			if hasSpeaker {
				result.NeedsSpeakerJoin = true
				result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, userIdCondition)
			}
		}
	}

	hasUserNameFilter := len(filterFields.ParsedUserName) > 0 && len(filterFields.ParsedAdvancedSearchBy) > 0
	hasUserCompanyNameFilter := len(filterFields.ParsedUserCompanyName) > 0 && len(filterFields.ParsedAdvancedSearchBy) > 0

	if hasUserNameFilter || hasUserCompanyNameFilter {
		hasVisitor, hasSpeaker, _, _, _ = getEntityTypes()

		if finalCondition := s.buildUserNameUserCompanyCondition(filterFields, hasSpeaker, hasVisitor); finalCondition != "" {
			if hasVisitor && hasSpeaker {
				result.NeedsVisitorJoin = true
				result.NeedsSpeakerJoin = true
				visitorCondition := s.buildUserNameUserCompanyCondition(filterFields, false, true)
				speakerCondition := s.buildUserNameUserCompanyCondition(filterFields, true, false)
				if len(result.UserIdWhereConditions) > 0 {
					userIdPart := strings.Join(result.UserIdWhereConditions, " AND ")
					result.VisitorWhereConditions = append(result.VisitorWhereConditions, userIdPart, visitorCondition)
					result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, userIdPart, speakerCondition)
					result.UserIdWhereConditions = []string{}
				} else {
					result.VisitorWhereConditions = append(result.VisitorWhereConditions, visitorCondition)
					result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, speakerCondition)
				}
			} else {
				useILikeLogic := hasSpeaker
				useCommonColumnsOnly := hasVisitor
				finalCondition := s.buildUserNameUserCompanyCondition(filterFields, useILikeLogic, useCommonColumnsOnly)
				if hasVisitor {
					result.NeedsVisitorJoin = true
					if len(result.VisitorWhereConditions) > 0 {
						combinedCondition := fmt.Sprintf("(%s) AND %s", strings.Join(result.VisitorWhereConditions, " AND "), finalCondition)
						result.VisitorWhereConditions = []string{combinedCondition}
					} else {
						result.VisitorWhereConditions = append(result.VisitorWhereConditions, finalCondition)
					}
				}
				if hasSpeaker {
					result.NeedsSpeakerJoin = true
					if len(result.SpeakerWhereConditions) > 0 {
						combinedCondition := fmt.Sprintf("(%s) AND %s", strings.Join(result.SpeakerWhereConditions, " AND "), finalCondition)
						result.SpeakerWhereConditions = []string{combinedCondition}
					} else {
						result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, finalCondition)
					}
				}
			}
		}
	}

	if len(filterFields.ParsedCompanyId) > 0 {
		companyIds := make([]string, 0, len(filterFields.ParsedCompanyId))
		for _, companyId := range filterFields.ParsedCompanyId {
			companyIds = append(companyIds, escapeSqlValue(companyId))
		}
		companyIdCondition := fmt.Sprintf("company_id IN (%s)", strings.Join(companyIds, ","))
		userCompanyIdCondition := fmt.Sprintf("user_company_id IN (%s)", strings.Join(companyIds, ","))
		result.CompanyIdWhereConditions = append(result.CompanyIdWhereConditions, companyIdCondition)

		hasVisitor, hasSpeaker, hasExhibitor, hasSponsor, hasOrganizer := getEntityTypes()

		// Count entity types
		companyEntityCount := 0
		if hasExhibitor {
			companyEntityCount++
		}
		if hasSponsor {
			companyEntityCount++
		}
		if hasOrganizer {
			companyEntityCount++
		}

		userEntityCount := 0
		if hasVisitor {
			userEntityCount++
		}
		if hasSpeaker {
			userEntityCount++
		}

		// Determine if we need UNION CTE
		needsUnionCTE := false
		if companyEntityCount > 1 {
			needsUnionCTE = true
		}
		if userEntityCount > 1 {
			needsUnionCTE = true
		}
		if companyEntityCount > 0 && userEntityCount > 0 {
			needsUnionCTE = true
		}

		if needsUnionCTE {
			result.NeedsCompanyIdUnionCTE = true
		} else {
			// Single entity type - use direct joins
			if hasVisitor {
				result.NeedsVisitorJoin = true
				result.VisitorWhereConditions = append(result.VisitorWhereConditions, userCompanyIdCondition)
			}
			if hasSpeaker {
				result.NeedsSpeakerJoin = true
				result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, userCompanyIdCondition)
			}
			if hasExhibitor {
				result.NeedsExhibitorJoin = true
				result.ExhibitorWhereConditions = append(result.ExhibitorWhereConditions, companyIdCondition)
			}
			if hasSponsor {
				result.NeedsSponsorJoin = true
				result.SponsorWhereConditions = append(result.SponsorWhereConditions, companyIdCondition)
			}
			if hasOrganizer {
				result.OrganizerWhereConditions = append(result.OrganizerWhereConditions, fmt.Sprintf("company_id IN (%s)", strings.Join(companyIds, ",")))
			}
		}
	}

	if len(filterFields.ParsedCompanyName) > 0 && len(filterFields.ParsedAdvancedSearchBy) > 0 {
		hasVisitor, hasSpeaker, hasExhibitor, hasSponsor, hasOrganizer := getEntityTypes()

		isCompanyEntity := false
		if len(filterFields.ParsedSearchByEntity) > 0 {
			searchByEntityLower := strings.ToLower(strings.TrimSpace(filterFields.ParsedSearchByEntity[0]))
			isCompanyEntity = searchByEntityLower == "company"
		}

		var companyNameConditions []string
		for _, companyName := range filterFields.ParsedCompanyName {
			companyCondition := s.matchPhraseConverter("user_company", companyName)
			if companyCondition != "" {
				companyNameConditions = append(companyNameConditions, companyCondition)
			}
		}

		if len(companyNameConditions) > 0 {
			finalCompanyNameCondition := fmt.Sprintf("(%s)", strings.Join(companyNameConditions, " OR "))
			if !isCompanyEntity && (hasVisitor || hasSpeaker) {
				if hasVisitor && hasSpeaker {
					result.NeedsUserIdUnionCTE = true
					if len(result.UserIdWhereConditions) > 0 {
						combinedCondition := fmt.Sprintf("(%s) AND %s", strings.Join(result.UserIdWhereConditions, " AND "), finalCompanyNameCondition)
						result.UserIdWhereConditions = []string{combinedCondition}
					} else {
						result.UserIdWhereConditions = append(result.UserIdWhereConditions, finalCompanyNameCondition)
					}
				} else {
					if hasVisitor {
						result.NeedsVisitorJoin = true
						if len(result.VisitorWhereConditions) > 0 {
							combinedCondition := fmt.Sprintf("(%s) AND %s", strings.Join(result.VisitorWhereConditions, " AND "), finalCompanyNameCondition)
							result.VisitorWhereConditions = []string{combinedCondition}
						} else {
							result.VisitorWhereConditions = append(result.VisitorWhereConditions, finalCompanyNameCondition)
						}
					}
					if hasSpeaker {
						result.NeedsSpeakerJoin = true
						if len(result.SpeakerWhereConditions) > 0 {
							combinedCondition := fmt.Sprintf("(%s) AND %s", strings.Join(result.SpeakerWhereConditions, " AND "), finalCompanyNameCondition)
							result.SpeakerWhereConditions = []string{combinedCondition}
						} else {
							result.SpeakerWhereConditions = append(result.SpeakerWhereConditions, finalCompanyNameCondition)
						}
					}
				}
			}

			if hasExhibitor {
				result.NeedsExhibitorJoin = true
				var exhibitorConditions []string
				for _, companyName := range filterFields.ParsedCompanyName {
					exhibitorCondition := s.matchPhraseConverter("company_id_name", companyName)
					if exhibitorCondition != "" {
						exhibitorConditions = append(exhibitorConditions, exhibitorCondition)
					}
				}
				if len(exhibitorConditions) > 0 {
					result.ExhibitorWhereConditions = append(result.ExhibitorWhereConditions, fmt.Sprintf("(%s)", strings.Join(exhibitorConditions, " OR ")))
				}
			}
			if hasSponsor {
				result.NeedsSponsorJoin = true
				var sponsorConditions []string
				for _, companyName := range filterFields.ParsedCompanyName {
					sponsorCondition := s.matchPhraseConverter("company_id_name", companyName)
					if sponsorCondition != "" {
						sponsorConditions = append(sponsorConditions, sponsorCondition)
					}
				}
				if len(sponsorConditions) > 0 {
					result.SponsorWhereConditions = append(result.SponsorWhereConditions, fmt.Sprintf("(%s)", strings.Join(sponsorConditions, " OR ")))
				}
			}
			if hasOrganizer {
				var organizerConditions []string
				for _, companyName := range filterFields.ParsedCompanyName {
					organizerCondition := s.matchPhraseConverter("company_name", companyName)
					if organizerCondition != "" {
						organizerConditions = append(organizerConditions, organizerCondition)
					}
				}
				if len(organizerConditions) > 0 {
					result.OrganizerWhereConditions = append(result.OrganizerWhereConditions, fmt.Sprintf("(%s)", strings.Join(organizerConditions, " OR ")))
				}
			}
		}
	}

	if len(filterFields.ParsedCompanyWebsite) > 0 && len(filterFields.ParsedAdvancedSearchBy) > 0 {
		_, _, hasExhibitor, hasSponsor, hasOrganizer := getEntityTypes()

		if hasExhibitor {
			result.NeedsExhibitorJoin = true
			var exhibitorConditions []string
			for _, companyWebsite := range filterFields.ParsedCompanyWebsite {
				exhibitorCondition := s.matchWebsiteConverter("company_website", companyWebsite)
				if exhibitorCondition != "" {
					exhibitorConditions = append(exhibitorConditions, exhibitorCondition)
				}
			}
			if len(exhibitorConditions) > 0 {
				result.ExhibitorWhereConditions = append(result.ExhibitorWhereConditions, fmt.Sprintf("(%s)", strings.Join(exhibitorConditions, " OR ")))
			}
		}
		if hasSponsor {
			result.NeedsSponsorJoin = true
			var sponsorConditions []string
			for _, companyWebsite := range filterFields.ParsedCompanyWebsite {
				sponsorCondition := s.matchWebsiteConverter("company_website", companyWebsite)
				if sponsorCondition != "" {
					sponsorConditions = append(sponsorConditions, sponsorCondition)
				}
			}
			if len(sponsorConditions) > 0 {
				result.SponsorWhereConditions = append(result.SponsorWhereConditions, fmt.Sprintf("(%s)", strings.Join(sponsorConditions, " OR ")))
			}
		}
		if hasOrganizer {
			var organizerConditions []string
			for _, companyWebsite := range filterFields.ParsedCompanyWebsite {
				organizerCondition := s.matchWebsiteConverter("company_website", companyWebsite)
				if organizerCondition != "" {
					organizerConditions = append(organizerConditions, organizerCondition)
				}
			}
			if len(organizerConditions) > 0 {
				result.OrganizerWhereConditions = append(result.OrganizerWhereConditions, fmt.Sprintf("(%s)", strings.Join(organizerConditions, " OR ")))
			}
		}
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
		result.TypeWhereConditions = append(result.TypeWhereConditions, fmt.Sprintf("etc.name IN (%s)", strings.Join(types, ",")))
	}

	if len(filterFields.ParsedEventTypes) > 0 {
		result.NeedsTypeJoin = true
		eventTypes := make([]string, len(filterFields.ParsedEventTypes))
		for i, et := range filterFields.ParsedEventTypes {
			eventTypes[i] = escapeSqlValue(et)
		}
		result.TypeWhereConditions = append(result.TypeWhereConditions, fmt.Sprintf("etc.eventtype_uuid IN (%s)", strings.Join(eventTypes, ",")))
	}

	if len(filterFields.ParsedEventRanking) > 0 {
		result.NeedsEventRankingJoin = true
		eventRankingValue := filterFields.ParsedEventRanking[0]
		result.EventRankingWhereConditions = append(result.EventRankingWhereConditions, fmt.Sprintf("event_rank <= %s", eventRankingValue))
	}

	if filterFields.ParsedJobCompositeFilter != nil {
		result.needsDesignationJoin = true
		var whereConditions []string

		if len(filterFields.ParsedJobCompositeFilter.DesignationIds) > 0 {
			designationUUIDs := make([]string, 0, len(filterFields.ParsedJobCompositeFilter.DesignationIds))
			for _, id := range filterFields.ParsedJobCompositeFilter.DesignationIds {
				trimmed := strings.TrimSpace(id)
				if trimmed != "" {
					designationUUIDs = append(designationUUIDs, escapeSqlValue(trimmed))
				}
			}
			if len(designationUUIDs) > 0 {
				whereConditions = append(whereConditions, fmt.Sprintf("designation_uuid IN (%s)", strings.Join(designationUUIDs, ",")))
			}
		}

		if filterFields.ParsedJobCompositeFilter.Conditions != nil {
			var conditionsWhere []string

			if len(filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds) > 0 {
				departmentUUIDs := make([]string, 0, len(filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds))
				for _, id := range filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds {
					trimmed := strings.TrimSpace(id)
					if trimmed != "" {
						departmentUUIDs = append(departmentUUIDs, escapeSqlValue(trimmed))
					}
				}
				if len(departmentUUIDs) > 0 {
					conditionsWhere = append(conditionsWhere, fmt.Sprintf("designation_uuid IN (%s)", strings.Join(departmentUUIDs, ",")))
				}
			}

			if len(filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds) > 0 {
				seniorityUUIDs := make([]string, 0, len(filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds))
				for _, id := range filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds {
					trimmed := strings.TrimSpace(id)
					if trimmed != "" {
						seniorityUUIDs = append(seniorityUUIDs, escapeSqlValue(trimmed))
					}
				}
				if len(seniorityUUIDs) > 0 {
					conditionsWhere = append(conditionsWhere, fmt.Sprintf("designation_uuid IN (%s)", strings.Join(seniorityUUIDs, ",")))
				}
			}

			if len(conditionsWhere) > 0 {
				conditionsLogic := filterFields.ParsedJobCompositeFilter.Conditions.Logic
				if conditionsLogic != "" && (conditionsLogic == "AND" || conditionsLogic == "OR") {
					if len(conditionsWhere) == 1 {
						whereConditions = append(whereConditions, conditionsWhere[0])
					} else {
						combinedCondition := fmt.Sprintf("(%s)", strings.Join(conditionsWhere, fmt.Sprintf(" %s ", conditionsLogic)))
						whereConditions = append(whereConditions, combinedCondition)
					}
				} else {
					if len(conditionsWhere) == 1 {
						whereConditions = append(whereConditions, conditionsWhere[0])
					} else {
						combinedCondition := strings.Join(conditionsWhere, " AND ")
						whereConditions = append(whereConditions, combinedCondition)
					}
				}
			}
		}

		if len(whereConditions) > 0 {
			mainLogic := filterFields.ParsedJobCompositeFilter.Logic
			if mainLogic == "" {
				mainLogic = "AND"
			}

			var finalCondition string
			if len(whereConditions) == 1 {
				finalCondition = whereConditions[0]
			} else {
				finalCondition = fmt.Sprintf("(%s)", strings.Join(whereConditions, fmt.Sprintf(" %s ", mainLogic)))
			}

			result.JobCompositeWhereConditions = append(result.JobCompositeWhereConditions, fmt.Sprintf("%s AND total_visitors >= 5", finalCondition))
		}
	} else if len(filterFields.ParsedJobComposite) > 0 {
		// Fallback: Handle old simple string format (backward compatibility)
		result.needsDesignationJoin = true
		jobComposites := make([]string, len(filterFields.ParsedJobComposite))
		for i, jobComposite := range filterFields.ParsedJobComposite {
			jobComposites[i] = escapeSqlValue(jobComposite)
		}
		result.JobCompositeWhereConditions = append(result.JobCompositeWhereConditions, fmt.Sprintf("display_name IN (%s) AND total_visitors >= 5", strings.Join(jobComposites, ",")))
	}

	if len(filterFields.ParsedAudienceSpread) > 0 {
		result.needsAudienceSpreadJoin = true
		var isoCodes []string
		var locationUUIDs []string

		for _, audienceSpread := range filterFields.ParsedAudienceSpread {
			trimmed := strings.TrimSpace(audienceSpread)
			if len(trimmed) > 2 {
				locationUUIDs = append(locationUUIDs, trimmed)
			} else {
				isoCodes = append(isoCodes, trimmed)
			}
		}

		if len(locationUUIDs) > 0 {
			uuidISOs, err := s.getISOFromLocationUUIDs(locationUUIDs)
			if err != nil {
				log.Printf("Error fetching ISO codes from location UUIDs: %v", err)
				return nil, fmt.Errorf("error fetching ISO codes from location UUIDs: %w", err)
			}
			isoCodes = append(isoCodes, uuidISOs...)
		}

		isoSet := make(map[string]bool)
		for _, iso := range isoCodes {
			if iso != "" {
				isoSet[iso] = true
			}
		}

		if len(isoSet) > 0 {
			var jsonConditions []string
			for iso := range isoSet {
				escapedISO := escapeSqlValue(iso)
				jsonConditions = append(jsonConditions, fmt.Sprintf("arrayExists(x -> x.cntry_id = %s AND x.total_count >= 5, user_by_cntry)", escapedISO))
			}
			result.AudienceSpreadWhereConditions = append(result.AudienceSpreadWhereConditions, fmt.Sprintf("(%s)", strings.Join(jsonConditions, " AND ")))
		}
	}

	if len(filterFields.ParsedRegions) > 0 {
		result.NeedsRegionsJoin = true
		result.HasRegionsFilter = true
		regions := make([]string, len(filterFields.ParsedRegions))
		for i, region := range filterFields.ParsedRegions {
			regions[i] = escapeSqlValue(region)
		}
		result.RegionsWhereConditions = append(result.RegionsWhereConditions, fmt.Sprintf("regions IS NOT NULL AND length(regions) > 0 AND regions[1] IN (%s)", strings.Join(regions, ",")))
	}

	if len(filterFields.ParsedCountry) > 0 {
		result.HasCountryFilter = true
	}

	if len(filterFields.ParsedLocationIds) > 0 {
		result.NeedsLocationIdsJoin = true
		result.LocationIdsWhereConditions = append(result.LocationIdsWhereConditions, fmt.Sprintf("id_uuid IN (%s)", strings.Join(filterFields.ParsedLocationIds, ",")))
	}

	if len(filterFields.ParsedCountryIds) > 0 {
		result.NeedsCountryIdsJoin = true
		result.CountryIdsWhereConditions = append(result.CountryIdsWhereConditions, fmt.Sprintf("location_type = 'COUNTRY' AND id_uuid IN (%s)", strings.Join(filterFields.ParsedCountryIds, ",")))
	}

	if len(filterFields.ParsedStateIds) > 0 {
		result.NeedsStateIdsJoin = true
		result.StateIdsWhereConditions = append(result.StateIdsWhereConditions, fmt.Sprintf("location_type = 'STATE' AND id_uuid IN (%s)", strings.Join(filterFields.ParsedStateIds, ",")))
	}

	if len(filterFields.ParsedCityIds) > 0 {
		result.NeedsCityIdsJoin = true
		result.CityIdsWhereConditions = append(result.CityIdsWhereConditions, fmt.Sprintf("location_type = 'CITY' AND id_uuid IN (%s)", strings.Join(filterFields.ParsedCityIds, ",")))
	}

	if len(filterFields.ParsedVenueIds) > 0 {
		result.NeedsVenueIdsJoin = true
		result.VenueIdsWhereConditions = append(result.VenueIdsWhereConditions, fmt.Sprintf("location_type = 'VENUE' AND id_uuid IN (%s)", strings.Join(filterFields.ParsedVenueIds, ",")))
	}

	if len(filterFields.ParsedCategoryIds) > 0 {
		result.NeedsCategoryJoin = true
		result.CategoryWhereConditions = append(result.CategoryWhereConditions, fmt.Sprintf("category_uuid IN (%s)", strings.Join(filterFields.ParsedCategoryIds, ",")))
	}

	result.NeedsAnyJoin = result.NeedsVisitorJoin || result.NeedsSpeakerJoin || result.NeedsExhibitorJoin || result.NeedsSponsorJoin || result.NeedsCategoryJoin || result.NeedsTypeJoin || result.NeedsEventRankingJoin || result.needsDesignationJoin || result.needsAudienceSpreadJoin || result.NeedsRegionsJoin || result.NeedsLocationIdsJoin || result.NeedsCountryIdsJoin || result.NeedsStateIdsJoin || result.NeedsCityIdsJoin || result.NeedsVenueIdsJoin

	s.addRangeFilters("following", "event_followers", &whereConditions, filterFields, false)
	s.addRangeFilters("speaker", "event_speaker", &whereConditions, filterFields, false)
	s.addRangeFilters("exhibitors", "event_exhibitor", &whereConditions, filterFields, false)
	s.addRangeFilters("editions", "event_editions", &whereConditions, filterFields, false)
	s.addRangeFilters("start", "start_date", &whereConditions, filterFields, true)
	s.addRangeFilters("end", "end_date", &whereConditions, filterFields, true)

	// When nil, we want all events for past/active counts, but still use createdAt in SELECT for "new" count
	if filterFields.ParsedGetNew != nil && *filterFields.ParsedGetNew {
		s.addRangeFilters("createdAt", "event_created", &whereConditions, filterFields, true)
	}
	s.addRangeFilters("inboundScore", "inboundScore", &whereConditions, filterFields, false)
	s.addRangeFilters("internationalScore", "internationalScore", &whereConditions, filterFields, false)
	s.addRangeFilters("trustScore", "repeatSentimentChangePercentage", &whereConditions, filterFields, false)
	s.addRangeFilters("impactScore", "impactScore", &whereConditions, filterFields, false)
	s.addRangeFilters("economicImpact", "event_economic_value", &whereConditions, filterFields, false)

	s.addEstimatedExhibitorsFilter(&whereConditions, filterFields)

	if !result.HasRegionsFilter {
		s.addInFilter("country", "edition_country", &whereConditions, filterFields)
	}
	s.addInFilter("venue", "venue_name", &whereConditions, filterFields)
	s.addInFilter("company", "company_name", &whereConditions, filterFields)
	s.addInFilter("companyCountry", "company_country", &whereConditions, filterFields)
	s.addInFilter("companyCity", "company_city_name", &whereConditions, filterFields)
	s.addInFilter("companyDomain", "company_domain", &whereConditions, filterFields)
	s.addInFilter("companyState", "company_state", &whereConditions, filterFields)

	if len(filterFields.ParsedEventIds) > 0 {
		whereConditions = append(whereConditions, fmt.Sprintf("ee.event_id IN (SELECT event_id FROM testing_db.allevent_ch WHERE event_uuid IN (%s))", strings.Join(filterFields.ParsedEventIds, ",")))
	}

	if len(filterFields.ParsedNotEventIds) > 0 {
		whereConditions = append(whereConditions, fmt.Sprintf("ee.event_id NOT IN (SELECT event_id FROM testing_db.allevent_ch WHERE event_uuid IN (%s))", strings.Join(filterFields.ParsedNotEventIds, ",")))
	}

	if len(filterFields.ParsedSourceEventIds) > 0 {
		whereConditions = append(whereConditions, fmt.Sprintf("ee.event_id IN (%s)", strings.Join(filterFields.ParsedSourceEventIds, ",")))
	}

	if len(filterFields.ParsedVisibility) > 0 {
		escapedVisibilities := make([]string, len(filterFields.ParsedVisibility))
		for i, visibility := range filterFields.ParsedVisibility {
			escapedVisibilities[i] = escapeSqlValue(visibility)
		}
		whereConditions = append(whereConditions, fmt.Sprintf("ee.edition_functionality IN (%s)", strings.Join(escapedVisibilities, ",")))
	}
	if len(filterFields.ParsedEstimatedVisitors) > 0 {
		escapedValues := make([]string, len(filterFields.ParsedEstimatedVisitors))
		for i, ev := range filterFields.ParsedEstimatedVisitors {
			escapedValues[i] = escapeSqlValue(ev)
		}
		whereConditions = append(whereConditions, fmt.Sprintf("ee.event_estimatedVisitors IN (%s)", strings.Join(escapedValues, ",")))
	}

	if filterFields.EventEstimate {
		whereConditions = append(whereConditions, "ee.event_economic_value IS NOT NULL")
	}

	s.addActiveDateFilters(&whereConditions, filterFields)

	result.DistanceOrderClause = s.addGeographicFilters(&whereConditions, filterFields, func(field string) string {
		return fmt.Sprintf("ee.%s", field)
	})

	if filterFields.ParsedViewBound != nil && filterFields.ParsedViewBound.BoundType == models.BoundTypePoint {
		var geoCoords models.GeoCoordinates
		if err := json.Unmarshal(filterFields.ParsedViewBound.Coordinates, &geoCoords); err == nil {
			unit := filterFields.ParsedViewBound.Unit
			if unit == "" {
				unit = "km"
			}

			radiusStr := fmt.Sprintf("%.2f", *geoCoords.Radius)
			latStr := fmt.Sprintf("%.9f", geoCoords.Latitude)
			lonStr := fmt.Sprintf("%.9f", geoCoords.Longitude)
			_, _, radiusInMeters := s.transformDataService.parseCoordinates(latStr, lonStr, radiusStr, unit)

			latField := "COALESCE(ee.venue_lat, ee.edition_city_lat)"
			lonField := "COALESCE(ee.venue_long, ee.edition_city_long)"
			coordinateExistsCondition := "(ee.venue_lat IS NOT NULL AND ee.venue_long IS NOT NULL) OR (ee.edition_city_lat IS NOT NULL AND ee.edition_city_long IS NOT NULL)"
			distanceCondition := fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f",
				geoCoords.Latitude, geoCoords.Longitude, latField, lonField, radiusInMeters)
			whereConditions = append(whereConditions, fmt.Sprintf("(%s) AND %s", coordinateExistsCondition, distanceCondition))

			if filterFields.EventDistanceOrder != "" {
				orderDirection := "ASC"
				if filterFields.EventDistanceOrder == "farthest" {
					orderDirection = "DESC"
				}
				log.Printf("viewBound distance sorting: EventDistanceOrder=%s, orderDirection=%s", filterFields.EventDistanceOrder, orderDirection)
				result.DistanceOrderClause = fmt.Sprintf("ORDER BY greatCircleDistance(%f, %f, %s, %s) %s",
					geoCoords.Latitude, geoCoords.Longitude, latField, lonField, orderDirection)
			}
		}
	}

	if len(filterFields.ParsedViewBounds) > 0 {
		for _, viewBound := range filterFields.ParsedViewBounds {
			if viewBound == nil {
				continue
			}

			switch viewBound.BoundType {
			case models.BoundTypePoint:
				var geoCoords models.GeoCoordinates
				if err := json.Unmarshal(viewBound.Coordinates, &geoCoords); err == nil {
					unit := viewBound.Unit
					if unit == "" {
						unit = "km"
					}

					radiusStr := fmt.Sprintf("%.2f", *geoCoords.Radius)
					latStr := fmt.Sprintf("%.9f", geoCoords.Latitude)
					lonStr := fmt.Sprintf("%.9f", geoCoords.Longitude)
					_, _, radiusInMeters := s.transformDataService.parseCoordinates(latStr, lonStr, radiusStr, unit)

					latField := "COALESCE(ee.venue_lat, ee.edition_city_lat)"
					lonField := "COALESCE(ee.venue_long, ee.edition_city_long)"
					coordinateExistsCondition := "(ee.venue_lat IS NOT NULL AND ee.venue_long IS NOT NULL) OR (ee.edition_city_lat IS NOT NULL AND ee.edition_city_long IS NOT NULL)"
					distanceCondition := fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f",
						geoCoords.Latitude, geoCoords.Longitude, latField, lonField, radiusInMeters)
					whereConditions = append(whereConditions, fmt.Sprintf("(%s) AND %s", coordinateExistsCondition, distanceCondition))
				}
				// case models.BoundTypeBox:
				// 	var boxCoords []float64
				// 	if err := json.Unmarshal(viewBound.Coordinates, &boxCoords); err == nil && len(boxCoords) == 4 {
				// 		minLng, minLat, maxLng, maxLat := boxCoords[0], boxCoords[1], boxCoords[2], boxCoords[3]

				// 		var latField, lonField string
				// 		if viewBound.ToEvent {
				// 			latField = "ee.edition_city_lat"
				// 			lonField = "ee.edition_city_long"
				// 			whereConditions = append(whereConditions, "ee.edition_city_lat IS NOT NULL AND ee.edition_city_long IS NOT NULL")
				// 		} else {
				// 			latField = "ee.venue_lat"
				// 			lonField = "ee.venue_long"
				// 		}

				// 		whereConditions = append(whereConditions, fmt.Sprintf("%s BETWEEN %f AND %f AND %s BETWEEN %f AND %f",
				// 			latField, minLat, maxLat, lonField, minLng, maxLng))
				// 	}
			}
		}
	}

	if len(filterFields.CreatedAt) > 0 {
		if filterFields.ParsedGetNew == nil || (filterFields.ParsedGetNew != nil && *filterFields.ParsedGetNew == true) {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.event_created >= '%s'", filterFields.CreatedAt))
		}
	}

	if len(filterFields.ParsedDates) > 0 {
		forecasted := filterFields.Forecasted
		tableAlias := "ee"
		var dateRangeConditions []string

		for _, dateRange := range filterFields.ParsedDates {
			start := dateRange[0]
			end := dateRange[1]

			var rangeConditions []string

			if start != nil && *start != "" {
				escapedStart := strings.ReplaceAll(*start, "'", "''")
				if end != nil && *end != "" {
					escapedEnd := strings.ReplaceAll(*end, "'", "''")
					switch forecasted {
					case "only":
						rangeConditions = append(rangeConditions, fmt.Sprintf("%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'", tableAlias, escapedStart, tableAlias, escapedEnd))
					case "included":
						rangeConditions = append(rangeConditions, fmt.Sprintf("((%s.end_date >= '%s' AND %s.start_date <= '%s') OR (%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'))",
							tableAlias, escapedStart, tableAlias, escapedEnd,
							tableAlias, escapedStart, tableAlias, escapedEnd))
					default:
						rangeConditions = append(rangeConditions, fmt.Sprintf("%s.end_date >= '%s' AND %s.start_date <= '%s'", tableAlias, escapedStart, tableAlias, escapedEnd))
					}
				} else {
					switch forecasted {
					case "only":
						rangeConditions = append(rangeConditions, fmt.Sprintf("%s.end_date >= '%s' AND %s", tableAlias, escapedStart, s.buildForecastedNotNullCondition(tableAlias)))
					case "included":
						rangeConditions = append(rangeConditions, fmt.Sprintf("((%s.end_date >= '%s') OR (%s.futureExpexctedEndDate >= '%s'))", tableAlias, escapedStart, tableAlias, escapedStart))
					default:
						rangeConditions = append(rangeConditions, fmt.Sprintf("%s.end_date >= '%s'", tableAlias, escapedStart))
					}
				}
			} else if end != nil && *end != "" {
				escapedEnd := strings.ReplaceAll(*end, "'", "''")
				switch forecasted {
				case "only":
					rangeConditions = append(rangeConditions, fmt.Sprintf("%s.end_date <= '%s' AND %s", tableAlias, escapedEnd, s.buildForecastedNotNullCondition(tableAlias)))
				case "included":
					rangeConditions = append(rangeConditions, fmt.Sprintf("((%s.end_date <= '%s') OR (%s.futureExpexctedEndDate <= '%s'))", tableAlias, escapedEnd, tableAlias, escapedEnd))
				default:
					rangeConditions = append(rangeConditions, fmt.Sprintf("%s.end_date <= '%s'", tableAlias, escapedEnd))
				}
			}

			if len(rangeConditions) > 0 {
				dateRangeConditions = append(dateRangeConditions, fmt.Sprintf("(%s)", strings.Join(rangeConditions, " AND ")))
			}
		}

		if len(dateRangeConditions) > 0 {
			whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(dateRangeConditions, " AND ")))
		}
	}

	if filterFields.ParsedPastBetween != nil {
		forecasted := filterFields.Forecasted
		tableAlias := "ee"
		start := filterFields.ParsedPastBetween.Start
		end := filterFields.ParsedPastBetween.End
		escapedStart := strings.ReplaceAll(start, "'", "''")
		escapedEnd := strings.ReplaceAll(end, "'", "''")

		switch forecasted {
		case "only":
			whereConditions = append(whereConditions, fmt.Sprintf("%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'",
				tableAlias, escapedStart, tableAlias, escapedEnd))
		case "included":
			whereConditions = append(whereConditions, fmt.Sprintf("((%s.end_date >= '%s' AND %s.start_date <= '%s') OR (%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'))",
				tableAlias, escapedStart, tableAlias, escapedEnd,
				tableAlias, escapedStart, tableAlias, escapedEnd))
		default:
			whereConditions = append(whereConditions, fmt.Sprintf("%s.end_date >= '%s' AND %s.start_date <= '%s'",
				tableAlias, escapedStart, tableAlias, escapedEnd))
		}
	}

	if filterFields.ParsedActiveBetween != nil {
		forecasted := filterFields.Forecasted
		tableAlias := "ee"
		start := filterFields.ParsedActiveBetween.Start
		end := filterFields.ParsedActiveBetween.End
		escapedStart := strings.ReplaceAll(start, "'", "''")
		escapedEnd := strings.ReplaceAll(end, "'", "''")

		switch forecasted {
		case "only":
			whereConditions = append(whereConditions, fmt.Sprintf("%s.futureExpexctedStartDate <= '%s' AND %s.futureExpexctedEndDate >= '%s'",
				tableAlias, escapedEnd, tableAlias, escapedStart))
		case "included":
			whereConditions = append(whereConditions, fmt.Sprintf("((%s.start_date <= '%s' AND %s.end_date >= '%s') OR (%s.futureExpexctedStartDate <= '%s' AND %s.futureExpexctedEndDate >= '%s'))",
				tableAlias, escapedEnd, tableAlias, escapedStart,
				tableAlias, escapedEnd, tableAlias, escapedStart))
		default:
			whereConditions = append(whereConditions, fmt.Sprintf("%s.start_date <= '%s' AND %s.end_date >= '%s'",
				tableAlias, escapedEnd, tableAlias, escapedStart))
		}
	}

	if len(filterFields.ParsedCity) > 0 {
		escapedCities := make([]string, len(filterFields.ParsedCity))
		for i, city := range filterFields.ParsedCity {
			escapedCities[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(city, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("ee.edition_city_name IN (%s)", strings.Join(escapedCities, ",")))
	}

	if len(filterFields.ParsedState) > 0 {
		escapedStates := make([]string, len(filterFields.ParsedState))
		for i, state := range filterFields.ParsedState {
			escapedStates[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(state, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("ee.edition_city_state IN (%s)", strings.Join(escapedStates, ",")))
	}

	if len(filterFields.ParsedPrice) > 0 {
		escapedPrices := make([]string, 0, len(filterFields.ParsedPrice))
		hasFreeAndPaid := false
		for _, price := range filterFields.ParsedPrice {
			escapedPrices = append(escapedPrices, fmt.Sprintf("'%s'", strings.ReplaceAll(price, "'", "''")))
			if price == "free_and_paid" {
				hasFreeAndPaid = true
			}
		}
		if !hasFreeAndPaid {
			escapedPrices = append(escapedPrices, "'free_and_paid'")
		}

		var priceCondition string
		if len(escapedPrices) == 1 {
			priceCondition = fmt.Sprintf("ee.event_pricing = %s", escapedPrices[0])
		} else {
			priceCondition = fmt.Sprintf("ee.event_pricing IN (%s)", strings.Join(escapedPrices, ","))
		}
		whereConditions = append(whereConditions, priceCondition)
	}

	if len(filterFields.ParsedFrequency) > 0 {
		escapedFrequencies := make([]string, 0, len(filterFields.ParsedFrequency))
		for _, freq := range filterFields.ParsedFrequency {
			escapedFrequencies = append(escapedFrequencies, fmt.Sprintf("'%s'", strings.ReplaceAll(freq, "'", "''")))
		}
		if len(escapedFrequencies) == 1 {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.event_frequency = %s", escapedFrequencies[0]))
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.event_frequency IN (%s)", strings.Join(escapedFrequencies, ",")))
		}
	}

	if len(filterFields.ParsedAvgRating) > 0 {
		var ratingConditions []string
		for _, ratingRange := range filterFields.ParsedAvgRating {
			var rangeConditions []string
			if ratingRange.Start >= 0 {
				rangeConditions = append(rangeConditions, fmt.Sprintf("ee.event_avgRating >= %g", ratingRange.Start))
			}
			if ratingRange.End > 0 {
				rangeConditions = append(rangeConditions, fmt.Sprintf("ee.event_avgRating <= %g", ratingRange.End))
			}
			if len(rangeConditions) > 0 {
				ratingConditions = append(ratingConditions, fmt.Sprintf("(%s)", strings.Join(rangeConditions, " AND ")))
			}
		}
		if len(ratingConditions) > 0 {
			whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(ratingConditions, " OR ")))
		}
	}

	if len(filterFields.ParsedMode) > 0 {
		log.Printf("Parsed mode: %v", filterFields.ParsedMode)
		escapedModes := make([]string, 0, len(filterFields.ParsedMode))
		for _, mode := range filterFields.ParsedMode {
			escapedModes = append(escapedModes, fmt.Sprintf("'%s'", strings.ReplaceAll(mode, "'", "''")))
		}

		// Check if all three modes (ONLINE, OFFLINE, HYBRID) are present
		hasOnline := false
		hasOffline := false
		hasHybrid := false
		for _, mode := range filterFields.ParsedMode {
			switch mode {
			case "ONLINE":
				hasOnline = true
			case "OFFLINE":
				hasOffline = true
			case "HYBRID":
				hasHybrid = true
			}
		}
		allModesPresent := hasOnline && hasOffline && hasHybrid

		if len(escapedModes) == 1 {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.event_format = %s", escapedModes[0]))
		} else {
			condition := fmt.Sprintf("ee.event_format IN (%s)", strings.Join(escapedModes, ","))
			if allModesPresent {
				condition = fmt.Sprintf("(%s OR ee.event_format IS NULL)", condition)
			}
			whereConditions = append(whereConditions, condition)
		}
	}

	if filterFields.ParsedIsBranded != nil {
		if *filterFields.ParsedIsBranded {
			whereConditions = append(whereConditions, "ee.eventBrandId IS NOT NULL")
		} else {
			whereConditions = append(whereConditions, "ee.eventBrandId IS NULL")
		}
	}

	if filterFields.ParsedIsSeries != nil {
		if *filterFields.ParsedIsSeries {
			whereConditions = append(whereConditions, "ee.eventSeriesId IS NOT NULL")
		} else {
			whereConditions = append(whereConditions, "ee.eventSeriesId IS NULL")
		}
	}

	if len(filterFields.ParsedMaturity) > 0 {
		escapedMaturities := make([]string, 0, len(filterFields.ParsedMaturity))
		for _, maturity := range filterFields.ParsedMaturity {
			escapedMaturities = append(escapedMaturities, fmt.Sprintf("'%s'", strings.ReplaceAll(maturity, "'", "''")))
		}
		if len(escapedMaturities) == 1 {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.maturity = %s", escapedMaturities[0]))
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.maturity IN (%s)", strings.Join(escapedMaturities, ",")))
		}
	}

	if filterFields.ParsedAudienceZone != nil {
		audienceZones := filterFields.ParsedAudienceZone
		for _, audienceZone := range audienceZones {
			whereConditions = append(whereConditions, fmt.Sprintf("ee.audienceZone = '%s'", strings.ReplaceAll(audienceZone, "'", "''")))
		}
	}

	if len(filterFields.ParsedEventAudience) > 0 {
		audienceValues := make([]string, len(filterFields.ParsedEventAudience))
		for i, val := range filterFields.ParsedEventAudience {
			audienceValues[i] = fmt.Sprintf("%d", val)
		}
		whereConditions = append(whereConditions, fmt.Sprintf("ee.editions_audiance_type IN (%s)", strings.Join(audienceValues, ",")))
	}

	result.SearchClause = s.buildSearchClause(filterFields)

	if result.HasRegionsFilter {
		countryCondition := "ee.edition_country IN (SELECT iso FROM filtered_regions)"

		if result.HasCountryFilter && len(filterFields.ParsedCountry) > 0 {
			countries := make([]string, len(filterFields.ParsedCountry))
			for i, country := range filterFields.ParsedCountry {
				countries[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(country, "'", "''"))
			}
			countryCondition = fmt.Sprintf("(ee.edition_country IN (SELECT iso FROM filtered_regions) OR ee.edition_country IN (%s))", strings.Join(countries, ","))
		}
		whereConditions = append(whereConditions, countryCondition)
	}

	if result.NeedsLocationIdsJoin {
		var locationConditions []string

		locationConditions = append(locationConditions, "ee.edition_country IN (SELECT iso FROM filtered_locations WHERE location_type = 'COUNTRY' AND iso IS NOT NULL)")

		locationConditions = append(locationConditions, "ee.edition_city IN (SELECT id FROM filtered_locations WHERE location_type = 'CITY' AND id IS NOT NULL)")

		locationConditions = append(locationConditions, "ee.edition_city_state_id IN (SELECT id FROM filtered_locations WHERE location_type = 'STATE' AND id IS NOT NULL)")

		locationConditions = append(locationConditions, "ee.venue_id IN (SELECT id FROM filtered_locations WHERE location_type = 'VENUE' AND id IS NOT NULL)")

		whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(locationConditions, " OR ")))
	}

	if result.NeedsCountryIdsJoin {
		whereConditions = append(whereConditions, "ee.edition_country IN (SELECT iso FROM filtered_country_ids WHERE iso IS NOT NULL)")
	}

	if result.NeedsStateIdsJoin {
		whereConditions = append(whereConditions, "ee.edition_city_state_id IN (SELECT id FROM filtered_state_ids WHERE id IS NOT NULL)")
	}

	if result.NeedsCityIdsJoin {
		whereConditions = append(whereConditions, "ee.edition_city IN (SELECT id FROM filtered_city_ids WHERE id IS NOT NULL)")
	}

	if result.NeedsVenueIdsJoin {
		whereConditions = append(whereConditions, "ee.venue_id IN (SELECT id FROM filtered_venue_ids WHERE id IS NOT NULL)")
	}

	result.WhereClause = strings.Join(whereConditions, " AND ")

	return result, nil
}

type CTEAndJoinResult struct {
	CTEClauses     []string
	JoinConditions []string
	JoinClausesStr string // JOIN clauses
}

func GetEventTypePublishedConditionForJoin(eventTypeIDs []string, tableAlias string) string {
	if len(eventTypeIDs) == 0 {
		return tableAlias + ".published = 1"
	}
	groups := models.GetEventTypeGroupsFromIDs(eventTypeIDs)
	hasHoliday := groups["unattended"]
	hasOther := groups["business"] || groups["social"]
	prefix := tableAlias + ".published"
	if hasHoliday && !hasOther {
		return prefix + " = 4"
	}
	if !hasHoliday && hasOther {
		return prefix + " IN (1, 2)"
	}
	if hasHoliday && hasOther {
		return prefix + " IN (1, 4)"
	}
	return tableAlias + ".published = 1"
}

func (s *SharedFunctionService) GetEventRankingScopeResolved(filterFields models.FilterDataDto) (countryIsos []string, categoryNames []string, err error) {
	countryUUIDs := filterFields.ParsedCountryIds
	if len(countryUUIDs) == 0 && len(filterFields.ParsedLocationIds) > 0 {
		countryUUIDs = filterFields.ParsedLocationIds
	}
	if len(countryUUIDs) > 0 {
		countryIsos, err = s.getISOFromLocationUUIDs(countryUUIDs)
		if err != nil {
			log.Printf("Error converting country UUIDs to ISO codes: %v", err)
			return nil, nil, err
		}
	}
	if len(filterFields.ParsedCategoryIds) > 0 {
		categoryNames, err = s.getCategoryNamesFromUUIDs(filterFields.ParsedCategoryIds)
		if err != nil {
			log.Printf("Error converting category UUIDs to names: %v", err)
			return nil, nil, err
		}
	}
	return countryIsos, categoryNames, nil
}

func (s *SharedFunctionService) BuildEventRankingScopeConditions(filterFields models.FilterDataDto, resolvedCountryIsos []string, resolvedCategoryNames []string) (string, error) {
	countryUUIDs := filterFields.ParsedCountryIds
	if len(countryUUIDs) == 0 && len(filterFields.ParsedLocationIds) > 0 {
		countryUUIDs = filterFields.ParsedLocationIds
	}
	hasCountryFilter := len(countryUUIDs) > 0
	hasCategoryFilter := len(filterFields.ParsedCategoryIds) > 0

	var conditions []string
	if hasCountryFilter && len(resolvedCountryIsos) > 0 {
		countries := make([]string, len(resolvedCountryIsos))
		for i, iso := range resolvedCountryIsos {
			escaped := strings.ReplaceAll(iso, "'", "''")
			countries[i] = fmt.Sprintf("'%s'", escaped)
		}
		conditions = append(conditions, fmt.Sprintf("country IN (%s)", strings.Join(countries, ",")))
	}
	if hasCategoryFilter && len(resolvedCategoryNames) > 0 {
		categories := make([]string, len(resolvedCategoryNames))
		for i, category := range resolvedCategoryNames {
			escaped := strings.ReplaceAll(category, "'", "''")
			categories[i] = fmt.Sprintf("'%s'", escaped)
		}
		conditions = append(conditions, fmt.Sprintf("category_name IN (%s)", strings.Join(categories, ",")))
	}
	if !hasCountryFilter && !hasCategoryFilter {
		conditions = append(conditions, "((country = '' AND category_name = ''))")
	}
	if hasCountryFilter && !hasCategoryFilter {
		conditions = append(conditions, "category_name = ''")
	}
	if hasCategoryFilter && !hasCountryFilter {
		conditions = append(conditions, "country = ''")
	}
	return strings.Join(conditions, " AND "), nil
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
	needsRegionsJoin bool,
	needsLocationIdsJoin bool,
	needsCountryIdsJoin bool,
	needsStateIdsJoin bool,
	needsCityIdsJoin bool,
	needsVenueIdsJoin bool,
	needsUserIdUnionCTE bool,
	visitorWhereConditions []string,
	speakerWhereConditions []string,
	exhibitorWhereConditions []string,
	sponsorWhereConditions []string,
	organizerWhereConditions []string,
	categoryWhereConditions []string,
	typeWhereConditions []string,
	eventRankingWhereConditions []string,
	jobCompositeWhereConditions []string,
	audienceSpreadWhereConditions []string,
	regionsWhereConditions []string,
	locationIdsWhereConditions []string,
	countryIdsWhereConditions []string,
	stateIdsWhereConditions []string,
	cityIdsWhereConditions []string,
	venueIdsWhereConditions []string,
	userIdWhereConditions []string,
	needsCompanyIdUnionCTE bool,
	companyIdWhereConditions []string,
	mainWhereClause string,
	mainSearchClause string,
	resolvedCountryIsos []string,
	resolvedCategoryNames []string,
	filterFields models.FilterDataDto,
) CTEAndJoinResult {
	result := CTEAndJoinResult{
		CTEClauses:     make([]string, 0),
		JoinConditions: make([]string, 0),
		JoinClausesStr: "",
	}

	previousCTE := ""

	addJoinClause := func(joinClause string) {
		if result.JoinClausesStr == "" {
			result.JoinClausesStr = joinClause
		} else {
			result.JoinClausesStr += "\n\t\t" + joinClause
		}
	}

	if needsRegionsJoin && len(regionsWhereConditions) > 0 {
		regionsWhereClause := strings.Join(regionsWhereConditions, " AND ")
		regionsCTE := fmt.Sprintf(`filtered_regions AS (
			SELECT iso
			FROM testing_db.location_ch
			WHERE %s
		)`, regionsWhereClause)
		result.CTEClauses = append(result.CTEClauses, regionsCTE)
	}

	if needsLocationIdsJoin && len(locationIdsWhereConditions) > 0 {
		locationIdsWhereClause := strings.Join(locationIdsWhereConditions, " AND ")

		locationIdsCTE := fmt.Sprintf(`filtered_locations AS (
			SELECT location_type, iso, id
			FROM testing_db.location_ch
			WHERE %s
		)`, locationIdsWhereClause)
		result.CTEClauses = append(result.CTEClauses, locationIdsCTE)
	}

	if needsCountryIdsJoin && len(countryIdsWhereConditions) > 0 {
		countryIdsWhereClause := strings.Join(countryIdsWhereConditions, " AND ")

		countryIdsCTE := fmt.Sprintf(`filtered_country_ids AS (
			SELECT iso
			FROM testing_db.location_ch
			WHERE %s
		)`, countryIdsWhereClause)
		result.CTEClauses = append(result.CTEClauses, countryIdsCTE)
	}

	if needsStateIdsJoin && len(stateIdsWhereConditions) > 0 {
		stateIdsWhereClause := strings.Join(stateIdsWhereConditions, " AND ")

		stateIdsCTE := fmt.Sprintf(`filtered_state_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, stateIdsWhereClause)
		result.CTEClauses = append(result.CTEClauses, stateIdsCTE)
	}

	if needsCityIdsJoin && len(cityIdsWhereConditions) > 0 {
		cityIdsWhereClause := strings.Join(cityIdsWhereConditions, " AND ")

		cityIdsCTE := fmt.Sprintf(`filtered_city_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, cityIdsWhereClause)
		result.CTEClauses = append(result.CTEClauses, cityIdsCTE)
	}

	if needsVenueIdsJoin && len(venueIdsWhereConditions) > 0 {
		venueIdsWhereClause := strings.Join(venueIdsWhereConditions, " AND ")

		venueIdsCTE := fmt.Sprintf(`filtered_venue_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, venueIdsWhereClause)
		result.CTEClauses = append(result.CTEClauses, venueIdsCTE)
	}

	var unifiedUnionParts []string
	visitorAddedToUnion := false
	speakerAddedToUnion := false

	visitorHasCompanyNameCondition := false
	speakerHasCompanyNameCondition := false

	if needsVisitorJoin && len(visitorWhereConditions) > 0 {
		for _, condition := range visitorWhereConditions {
			if strings.Contains(condition, "user_company") {
				visitorHasCompanyNameCondition = true
				break
			}
		}
	}
	if needsSpeakerJoin && len(speakerWhereConditions) > 0 {
		for _, condition := range speakerWhereConditions {
			if strings.Contains(condition, "user_company") {
				speakerHasCompanyNameCondition = true
				break
			}
		}
	}

	if needsUserIdUnionCTE && len(userIdWhereConditions) > 0 {
		for _, condition := range userIdWhereConditions {
			if strings.Contains(condition, "user_company") {
				visitorHasCompanyNameCondition = true
				speakerHasCompanyNameCondition = true
				break
			}
		}
	}

	const publishedConditionCh = " AND published IN (1, 2)"
	if needsUserIdUnionCTE && len(userIdWhereConditions) > 0 && !visitorHasCompanyNameCondition {
		userIdCondition := strings.Join(userIdWhereConditions, " AND ")
		visitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_visitors_ch
			WHERE %s%s`, userIdCondition, publishedConditionCh)
		unifiedUnionParts = append(unifiedUnionParts, visitorPart)
	}

	if needsUserIdUnionCTE && len(userIdWhereConditions) > 0 && !speakerHasCompanyNameCondition {
		userIdCondition := strings.Join(userIdWhereConditions, " AND ")
		speakerPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_speaker_ch
			WHERE %s%s`, userIdCondition, publishedConditionCh)
		unifiedUnionParts = append(unifiedUnionParts, speakerPart)
	}

	if needsExhibitorJoin && len(exhibitorWhereConditions) > 0 {
		exhibitorWhereClause := strings.Join(exhibitorWhereConditions, " OR ")
		exhibitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_exhibitor_ch
			WHERE %s%s`, exhibitorWhereClause, publishedConditionCh)
		unifiedUnionParts = append(unifiedUnionParts, exhibitorPart)
	}

	if needsSponsorJoin && len(sponsorWhereConditions) > 0 {
		sponsorWhereClause := strings.Join(sponsorWhereConditions, " OR ")
		sponsorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_sponsors_ch
			WHERE %s%s`, sponsorWhereClause, publishedConditionCh)
		unifiedUnionParts = append(unifiedUnionParts, sponsorPart)
	}

	if len(organizerWhereConditions) > 0 {
		organizerWhereClause := strings.Join(organizerWhereConditions, " OR ")
		editionTypeCondition := s.buildEditionTypeCondition(filterFields, "")
		organizerPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.allevent_ch
			WHERE %s AND %s`, organizerWhereClause, editionTypeCondition)
		unifiedUnionParts = append(unifiedUnionParts, organizerPart)
	}

	if needsVisitorJoin && len(visitorWhereConditions) > 0 {
		hasCompanyNameCondition := false
		for _, condition := range visitorWhereConditions {
			if strings.Contains(condition, "user_company") {
				hasCompanyNameCondition = true
				break
			}
		}
		if hasCompanyNameCondition {
			visitorWhereClause := strings.Join(visitorWhereConditions, " AND ")
			visitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
				FROM testing_db.event_visitors_ch
				WHERE %s%s`, visitorWhereClause, publishedConditionCh)
			unifiedUnionParts = append(unifiedUnionParts, visitorPart)
			visitorAddedToUnion = true
		}
	}

	if needsSpeakerJoin && len(speakerWhereConditions) > 0 {
		hasCompanyNameCondition := false
		for _, condition := range speakerWhereConditions {
			if strings.Contains(condition, "user_company") {
				hasCompanyNameCondition = true
				break
			}
		}
		if hasCompanyNameCondition {
			speakerWhereClause := strings.Join(speakerWhereConditions, " AND ")
			speakerPart := fmt.Sprintf(`SELECT DISTINCT event_id
				FROM testing_db.event_speaker_ch
				WHERE %s%s`, speakerWhereClause, publishedConditionCh)
			unifiedUnionParts = append(unifiedUnionParts, speakerPart)
			speakerAddedToUnion = true
		}
	}

	if needsUserIdUnionCTE && len(userIdWhereConditions) > 0 {
		hasCompanyNameCondition := false
		for _, condition := range userIdWhereConditions {
			if strings.Contains(condition, "user_company") {
				hasCompanyNameCondition = true
				break
			}
		}
		if hasCompanyNameCondition {
			userIdCondition := strings.Join(userIdWhereConditions, " AND ")

			// Add visitor if not already added
			if !visitorAddedToUnion {
				visitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
					FROM testing_db.event_visitors_ch
					WHERE %s%s`, userIdCondition, publishedConditionCh)
				unifiedUnionParts = append(unifiedUnionParts, visitorPart)
				visitorAddedToUnion = true
			}

			// Add speaker if not already added
			if !speakerAddedToUnion {
				speakerPart := fmt.Sprintf(`SELECT DISTINCT event_id
					FROM testing_db.event_speaker_ch
					WHERE %s%s`, userIdCondition, publishedConditionCh)
				unifiedUnionParts = append(unifiedUnionParts, speakerPart)
				speakerAddedToUnion = true
			}
		}
	}

	if len(unifiedUnionParts) > 0 {
		cteName := "filtered_company_events_by_name"
		selectColumn := "event_id"

		if !needsExhibitorJoin && !needsSponsorJoin && len(organizerWhereConditions) == 0 && !visitorAddedToUnion && !speakerAddedToUnion {
			cteName = "filtered_user_events"
			selectColumn = "event_id"
		}

		unionCTE := fmt.Sprintf(`%s AS (
			SELECT DISTINCT %s
			FROM (
				%s
			)
		)`, cteName, selectColumn, strings.Join(unifiedUnionParts, "\n\t\t\t\tUNION ALL\n\t\t\t\t"))

		result.CTEClauses = append(result.CTEClauses, unionCTE)
		previousCTE = cteName
	}

	if needsCompanyIdUnionCTE && len(companyIdWhereConditions) > 0 {
		companyIdCondition := strings.Join(companyIdWhereConditions, " AND ")

		// Extract companyIds from the condition for user_company_id queries
		companyIds := make([]string, 0)
		if len(filterFields.ParsedCompanyId) > 0 {
			for _, companyId := range filterFields.ParsedCompanyId {
				escaped := strings.ReplaceAll(companyId, "'", "''")
				companyIds = append(companyIds, fmt.Sprintf("'%s'", escaped))
			}
		}
		userCompanyIdCondition := fmt.Sprintf("user_company_id IN (%s)", strings.Join(companyIds, ","))

		hasVisitor, hasSpeaker, hasExhibitor, hasSponsor, hasOrganizer := func() (bool, bool, bool, bool, bool) {
			entities := filterFields.ParsedAdvancedSearchBy
			hasVisitor := false
			hasSpeaker := false
			hasExhibitor := false
			hasSponsor := false
			hasOrganizer := false
			for _, entity := range entities {
				switch entity {
				case "visitor":
					hasVisitor = true
				case "speaker":
					hasSpeaker = true
				case "exhibitor":
					hasExhibitor = true
				case "sponsor":
					hasSponsor = true
				case "organizer":
					hasOrganizer = true
				}
			}
			return hasVisitor, hasSpeaker, hasExhibitor, hasSponsor, hasOrganizer
		}()

		var unionParts []string

		const publishedConditionCh = " AND published IN (1, 2)"
		if hasVisitor {
			visitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_visitors_ch
			WHERE %s%s`, userCompanyIdCondition, publishedConditionCh)
			unionParts = append(unionParts, visitorPart)
		}

		if hasSpeaker {
			speakerPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_speaker_ch
			WHERE %s%s`, userCompanyIdCondition, publishedConditionCh)
			unionParts = append(unionParts, speakerPart)
		}

		if hasExhibitor {
			exhibitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_exhibitor_ch
			WHERE %s%s`, companyIdCondition, publishedConditionCh)
			unionParts = append(unionParts, exhibitorPart)
		}

		if hasSponsor {
			sponsorPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.event_sponsors_ch
			WHERE %s%s`, companyIdCondition, publishedConditionCh)
			unionParts = append(unionParts, sponsorPart)
		}

		if hasOrganizer {
			editionTypeCondition := s.buildEditionTypeCondition(filterFields, "")
			organizerPart := fmt.Sprintf(`SELECT DISTINCT event_id
			FROM testing_db.allevent_ch
			WHERE %s AND %s`, companyIdCondition, editionTypeCondition)
			unionParts = append(unionParts, organizerPart)
		}

		if len(unionParts) > 0 {
			unionCTE := fmt.Sprintf(`filtered_company_events AS (
			SELECT DISTINCT event_id
			FROM (
				%s
			)
		)`, strings.Join(unionParts, "\n\t\t\t\tUNION ALL\n\t\t\t\t"))

			result.CTEClauses = append(result.CTEClauses, unionCTE)
			previousCTE = "filtered_company_events"
		}
	}

	if needsVisitorJoin && !visitorAddedToUnion {
		visitorWhereClause := ""
		if len(visitorWhereConditions) > 0 {
			visitorWhereClause = fmt.Sprintf("WHERE %s AND published IN (1, 2)", strings.Join(visitorWhereConditions, " AND "))
		} else {
			visitorWhereClause = "WHERE published IN (1, 2)"
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

	if needsSpeakerJoin && !speakerAddedToUnion {
		speakerWhereClause := ""
		if len(speakerWhereConditions) > 0 {
			speakerWhereClause = fmt.Sprintf("WHERE %s AND published IN (1, 2)", strings.Join(speakerWhereConditions, " AND "))
		} else {
			speakerWhereClause = "WHERE published IN (1, 2)"
		}

		speakerQuery := fmt.Sprintf(`filtered_speakers AS (
			SELECT event_id
			FROM testing_db.event_speaker_ch
			%s`, speakerWhereClause)

		if previousCTE != "" {
			var joinColumn string
			switch previousCTE {
			case "filtered_user_events":
				joinColumn = "event_id"
			case "filtered_company_events":
				joinColumn = "event_id"
			case "filtered_company_events_by_name":
				joinColumn = "event_id"
			default:
				joinColumn = "event_id"
			}

			speakerQuery = fmt.Sprintf(`filtered_speakers AS (
				SELECT event_id
				FROM testing_db.event_speaker_ch
				WHERE %s IN (SELECT %s FROM %s)`, joinColumn, joinColumn, previousCTE)
			if len(speakerWhereConditions) > 0 {
				speakerQuery += fmt.Sprintf(`
				AND %s`, strings.Join(speakerWhereConditions, " AND "))
			}
			speakerQuery += `
				AND published IN (1, 2)`
		}

		speakerQuery += `
			GROUP BY event_id
		)`

		result.CTEClauses = append(result.CTEClauses, speakerQuery)
		previousCTE = "filtered_speakers"
	}

	companyAddedToUnified := (needsExhibitorJoin && len(exhibitorWhereConditions) > 0) ||
		(needsSponsorJoin && len(sponsorWhereConditions) > 0)

	if !companyAddedToUnified && (needsExhibitorJoin || needsSponsorJoin) {
		shouldUseCompanyUnion := needsExhibitorJoin && needsSponsorJoin &&
			len(exhibitorWhereConditions) > 0 && len(sponsorWhereConditions) > 0 &&
			(previousCTE == "" || previousCTE == "filtered_user_events" || previousCTE == "filtered_company_events" || previousCTE == "filtered_company_events_by_name")

		if shouldUseCompanyUnion {
			var unionParts []string
			var joinColumn string
			var joinCondition string

			if previousCTE != "" {
				switch previousCTE {
				case "filtered_user_events":
					joinColumn = "event_id"
				case "filtered_company_events_by_name", "filtered_company_events":
					joinColumn = "event_id"
				default:
					joinColumn = "event_id"
				}
				joinCondition = fmt.Sprintf("%s IN (SELECT %s FROM %s) AND ", joinColumn, joinColumn, previousCTE)
			}

			exhibitorWhereClause := strings.Join(exhibitorWhereConditions, " AND ")
			exhibitorPart := fmt.Sprintf(`SELECT DISTINCT event_id
				FROM testing_db.event_exhibitor_ch
				WHERE %s%s AND published IN (1, 2)`, joinCondition, exhibitorWhereClause)
			unionParts = append(unionParts, exhibitorPart)

			sponsorWhereClause := strings.Join(sponsorWhereConditions, " AND ")
			sponsorPart := fmt.Sprintf(`SELECT DISTINCT event_id
				FROM testing_db.event_sponsors_ch
				WHERE %s%s AND published IN (1, 2)`, joinCondition, sponsorWhereClause)
			unionParts = append(unionParts, sponsorPart)

			unionCTE := fmt.Sprintf(`filtered_company_events_by_name AS (
				SELECT DISTINCT event_id
				FROM (
					%s
				)
			)`, strings.Join(unionParts, "\n\t\t\t\tUNION ALL\n\t\t\t\t"))

			result.CTEClauses = append(result.CTEClauses, unionCTE)
			previousCTE = "filtered_company_events_by_name"
		} else {
			if needsExhibitorJoin {
				exhibitorWhereClause := ""
				if len(exhibitorWhereConditions) > 0 {
					exhibitorWhereClause = fmt.Sprintf("WHERE %s AND published IN (1, 2)", strings.Join(exhibitorWhereConditions, " AND "))
				} else {
					exhibitorWhereClause = "WHERE published IN (1, 2)"
				}

				exhibitorQuery := fmt.Sprintf(`filtered_exhibitors AS (
					SELECT event_id
					FROM testing_db.event_exhibitor_ch
					%s`, exhibitorWhereClause)

				if previousCTE != "" {
					var joinColumn string
					switch previousCTE {
					case "filtered_user_events":
						joinColumn = "event_id"
					case "filtered_company_events":
						joinColumn = "event_id"
					default:
						joinColumn = "event_id"
					}

					exhibitorQuery = fmt.Sprintf(`filtered_exhibitors AS (
						SELECT event_id
						FROM testing_db.event_exhibitor_ch
						WHERE %s IN (SELECT %s FROM %s)`, joinColumn, joinColumn, previousCTE)
					if len(exhibitorWhereConditions) > 0 {
						exhibitorQuery += fmt.Sprintf(`
						AND %s`, strings.Join(exhibitorWhereConditions, " AND "))
					}
					exhibitorQuery += `
						AND published IN (1, 2)`
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
					sponsorWhereClause = fmt.Sprintf("WHERE %s AND published IN (1, 2)", strings.Join(sponsorWhereConditions, " AND "))
				} else {
					sponsorWhereClause = "WHERE published IN (1, 2)"
				}

				sponsorQuery := fmt.Sprintf(`filtered_sponsors AS (
					SELECT event_id
					FROM testing_db.event_sponsors_ch
					%s`, sponsorWhereClause)

				if previousCTE != "" {
					var joinColumn string
					switch previousCTE {
					case "filtered_user_events":
						joinColumn = "event_id"
					case "filtered_company_events":
						joinColumn = "event_id"
					default:
						joinColumn = "event_id"
					}

					sponsorQuery = fmt.Sprintf(`filtered_sponsors AS (
						SELECT event_id
						FROM testing_db.event_sponsors_ch
						WHERE %s IN (SELECT %s FROM %s)`, joinColumn, joinColumn, previousCTE)
					if len(sponsorWhereConditions) > 0 {
						sponsorQuery += fmt.Sprintf(`
						AND %s`, strings.Join(sponsorWhereConditions, " AND "))
					}
					sponsorQuery += `
						AND published IN (1, 2)`
				}

				sponsorQuery += `
					GROUP BY event_id
				)`

				result.CTEClauses = append(result.CTEClauses, sponsorQuery)
				previousCTE = "filtered_sponsors"
			}
		}
	}

	if needsTypeJoin {
		// OLD LOGIC: Creating filtered_types CTE - COMMENTED OUT
		// We now use direct JOIN instead of CTE for better performance
		// typeWhereClause := ""
		// if len(typeWhereConditions) > 0 {
		// 	typeWhereClause = fmt.Sprintf("WHERE %s", strings.Join(typeWhereConditions, " AND "))
		// }
		//
		// typeQuery := fmt.Sprintf(`filtered_types AS (
		// 	SELECT event_id
		// 	FROM testing_db.event_type_ch
		// 	%s`, typeWhereClause)
		//
		// if previousCTE != "" {
		// 	var selectColumn string
		// 	if previousCTE == "filtered_categories" {
		// 		selectColumn = "event"
		// 	} else {
		// 		selectColumn = "event_id"
		// 	}
		// 	typeQuery = fmt.Sprintf(`filtered_types AS (
		// 		SELECT event_id
		// 		FROM testing_db.event_type_ch
		// 		WHERE event_id IN (SELECT %s FROM %s)`, selectColumn, previousCTE)
		// 	if len(typeWhereConditions) > 0 {
		// 		typeQuery += fmt.Sprintf(`
		// 		AND %s`, strings.Join(typeWhereConditions, " AND "))
		// 	}
		// }
		//
		// typeQuery += `
		// 	GROUP BY event_id
		// 	-- ORDER BY event_id
		// )`
		//
		// result.CTEClauses = append(result.CTEClauses, typeQuery)
		// previousCTE = "filtered_types"

		eventTypePublishedCond := GetEventTypePublishedConditionForJoin(filterFields.ParsedEventTypes, "etc")
		typeJoinOnClause := "etc.event_id = ee.event_id and " + eventTypePublishedCond
		if len(typeWhereConditions) > 0 {
			typeJoinOnClause += fmt.Sprintf("\n\t\t\tAND %s", strings.Join(typeWhereConditions, " AND "))
		}
		typeJoinClause := fmt.Sprintf("INNER JOIN testing_db.event_type_ch etc\n\t\t\tON %s", typeJoinOnClause)
		addJoinClause(typeJoinClause)
	}

	if needsCategoryJoin {
		categoryJoinOnClause := "ec.event = ee.event_id"
		if len(categoryWhereConditions) > 0 {
			prefixedConditions := make([]string, len(categoryWhereConditions))
			for i, condition := range categoryWhereConditions {
				prefixedCondition := condition
				categoryFields := []string{"name", "is_group", "category_uuid"}
				for _, field := range categoryFields {
					fieldPattern := fmt.Sprintf("\\b%s\\b", field)
					re := regexp.MustCompile(fieldPattern)
					prefixedCondition = re.ReplaceAllString(prefixedCondition, fmt.Sprintf("ec.%s", field))
				}
				prefixedConditions[i] = prefixedCondition
			}
			categoryJoinOnClause += fmt.Sprintf("\n\t\t\tAND %s", strings.Join(prefixedConditions, " AND "))
		}
		categoryJoinClause := fmt.Sprintf("INNER JOIN testing_db.event_category_ch ec\n\t\t\tON %s", categoryJoinOnClause)
		addJoinClause(categoryJoinClause)
	}

	if needsEventRankingJoin {
		today := time.Now().Format("2006-01-02")

		preEventFilterConditions := []string{
			s.buildPublishedCondition(filterFields),
			s.buildStatusCondition(filterFields),
			s.buildEditionTypeCondition(filterFields, "ee"),
		}
		hasUserEndDateFilter := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
			filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != ""
		if !hasUserEndDateFilter {
			preEventFilterConditions = append(preEventFilterConditions, fmt.Sprintf("end_date >= '%s'", today))
		}
		if strings.TrimSpace(mainWhereClause) != "" {
			preEventFilterConditions = append(preEventFilterConditions, mainWhereClause)
		}
		if strings.TrimSpace(mainSearchClause) != "" {
			preEventFilterConditions = append(preEventFilterConditions, mainSearchClause)
		}

		preEventFilterWhereClause := strings.Join(preEventFilterConditions, " AND ")
		preEventFilterFrom := "FROM testing_db.allevent_ch AS ee"
		if result.JoinClausesStr != "" {
			preEventFilterFrom += "\n\t\t" + result.JoinClausesStr
		}

		if previousCTE != "" {
			var selectColumn string
			if previousCTE == "filtered_categories" {
				selectColumn = "event"
			} else {
				selectColumn = "event_id"
			}
			preEventFilterCTE := fmt.Sprintf(`pre_event_filter AS (
				SELECT ee.event_id as event_id, ee.edition_id, ee.event_score
				%s
				WHERE ee.event_id IN (SELECT %s FROM %s)
				AND %s
				GROUP BY ee.event_id, ee.edition_id, ee.event_score
				ORDER BY ee.event_score DESC
			)`, preEventFilterFrom, selectColumn, previousCTE, preEventFilterWhereClause)
			result.CTEClauses = append(result.CTEClauses, preEventFilterCTE)
		} else {
			preEventFilterCTE := fmt.Sprintf(`pre_event_filter AS (
				SELECT ee.event_id as event_id, ee.edition_id, ee.event_score
				%s
				WHERE %s
				GROUP BY ee.event_id, ee.edition_id, ee.event_score
				ORDER BY ee.event_score DESC
			)`, preEventFilterFrom, preEventFilterWhereClause)
			result.CTEClauses = append(result.CTEClauses, preEventFilterCTE)
		}

		currentMonth := time.Now().Month()
		currentMonthCondition := fmt.Sprintf("MONTH(created) = %d", currentMonth)

		eventRankingConditions := []string{currentMonthCondition}
		scopeConditions, err := s.BuildEventRankingScopeConditions(filterFields, resolvedCountryIsos, resolvedCategoryNames)
		if err != nil {
			scopeConditions = "((country = '' AND category_name = ''))"
		}
		if scopeConditions != "" {
			eventRankingConditions = append(eventRankingConditions, scopeConditions)
		}
		if len(eventRankingWhereConditions) > 0 {
			eventRankingConditions = append(eventRankingConditions, eventRankingWhereConditions...)
		}
		eventRankingLimit := filterFields.ParsedEventRanking[0]

		eventRankingQuery := fmt.Sprintf(`filtered_event_ranking AS (
			SELECT er.event_id
			FROM testing_db.event_ranking_ch AS er
			INNER JOIN (SELECT event_id FROM pre_event_filter) AS pef ON er.event_id = pef.event_id
			WHERE %s
			GROUP BY er.event_id LIMIT %s
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
		var joinColumn string
		switch previousCTE {
		case "filtered_categories":
			selectColumn = "event"
			joinColumn = "ee.event_id"
		case "filtered_company_events":
			selectColumn = "event_id"
			joinColumn = "ee.event_id"
		case "filtered_company_events_by_name":
			selectColumn = "event_id"
			joinColumn = "ee.event_id"
		case "filtered_user_events":
			selectColumn = "event_id"
			joinColumn = "ee.event_id"
		default:
			selectColumn = "event_id"
			joinColumn = "ee.event_id"
		}
		result.JoinConditions = append(result.JoinConditions, fmt.Sprintf("%s IN (SELECT %s FROM %s)", joinColumn, selectColumn, previousCTE))
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
		"impact_score":      "impactScore",
		"event_score":       "score",
		"event_name":        "name",
		"event_updated":     "updated",
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
			"impactScore":         "impactScore",
			"score":               "event_score",
			"updated":             "event_updated",
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
	var ranges []string

	if len(filterFields.ParsedEstimatedExhibitors) > 0 {
		ranges = filterFields.ParsedEstimatedExhibitors
	} else if filterFields.EstimatedExhibitors != "" {
		ranges = strings.Split(filterFields.EstimatedExhibitors, ",")
		for i, r := range ranges {
			ranges[i] = strings.TrimSpace(r)
		}
	} else {
		return
	}

	if len(ranges) == 0 {
		return
	}

	var rangeConditions []string

	for _, rangeStr := range ranges {
		if rangeStr == "" {
			continue
		}

		var gte, lte int

		switch rangeStr {
		case "0-100":
			gte, lte = 0, 100
		case "100-500":
			gte, lte = 100, 500
		case "500-1000":
			gte, lte = 500, 1000
		case "1000":
			gte, lte = 1000, 1000000
		default:
			continue
		}

		rangeConditions = append(rangeConditions, fmt.Sprintf("(ee.exhibitors_mean >= %d AND ee.exhibitors_mean <= %d)", gte, lte))
	}

	if len(rangeConditions) == 0 {
		return
	}

	// Combine all range conditions with OR and add NULL check
	condition := fmt.Sprintf("ee.exhibitors_mean IS NOT NULL AND (%s)", strings.Join(rangeConditions, " OR "))
	*whereConditions = append(*whereConditions, condition)
}

func (s *SharedFunctionService) addInFilter(filterKey string, dbField string, whereConditions *[]string, filterFields models.FilterDataDto) {
	values := s.transformDataService.getParsedArrayValue(filterFields, filterKey)
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

func (s *SharedFunctionService) buildDefaultDateCondition(forecasted string, tableAlias string, today string) string {
	switch forecasted {
	case "only":
		return fmt.Sprintf("%s.futureExpexctedEndDate >= '%s'", tableAlias, today)
	case "included":
		return fmt.Sprintf("(%s.end_date >= '%s' OR %s.futureExpexctedEndDate >= '%s')", tableAlias, today, tableAlias, today)
	default:
		return fmt.Sprintf("%s.end_date >= '%s'", tableAlias, today)
	}
}

func (s *SharedFunctionService) buildPreFilterSelectDates(preFilterSelect string, forecasted string) string {
	switch forecasted {
	case "only":
		return preFilterSelect + ", e.futureExpexctedStartDate, e.futureExpexctedEndDate"
	case "included":
		return preFilterSelect + ", e.start_date, e.end_date, e.futureExpexctedStartDate, e.futureExpexctedEndDate"
	default:
		return preFilterSelect + ", e.start_date, e.end_date"
	}
}

func (s *SharedFunctionService) getDateFieldName(forecasted string, fieldType string, tableAlias string) string {
	if forecasted == "only" {
		if fieldType == "start" {
			return fmt.Sprintf("%s.futureExpexctedStartDate", tableAlias)
		}
		return fmt.Sprintf("%s.futureExpexctedEndDate", tableAlias)
	}
	if fieldType == "start" {
		return fmt.Sprintf("%s.start_date", tableAlias)
	}
	return fmt.Sprintf("%s.end_date", tableAlias)
}

func (s *SharedFunctionService) buildForecastedNotNullCondition(tableAlias string) string {
	return fmt.Sprintf("(%s.futureExpexctedStartDate IS NOT NULL AND %s.futureExpexctedEndDate IS NOT NULL)", tableAlias, tableAlias)
}

func (s *SharedFunctionService) getEndDateFieldForPastActive(forecasted string, filterFields models.FilterDataDto, tableAlias string) string {
	if forecasted == "only" && s.isStandaloneActiveDateFilter(filterFields) {
		return fmt.Sprintf("%s.end_date", tableAlias)
	}
	return s.getDateFieldName(forecasted, "end", tableAlias)
}

func (s *SharedFunctionService) buildTrendsDateCondition(forecasted string, tableAlias string, conditionType string, startDate string, endDate string) string {
	switch conditionType {
	case "preFilterRange":
		switch forecasted {
		case "only":
			if endDate == "" || endDate == startDate {
				return fmt.Sprintf("%s.futureExpexctedEndDate >= '%s'", tableAlias, startDate)
			}
			return fmt.Sprintf("%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'", tableAlias, startDate, tableAlias, endDate)
		case "included":
			if endDate == "" || endDate == startDate {
				return fmt.Sprintf("((%s.end_date >= '%s') OR (%s.futureExpexctedEndDate >= '%s'))", tableAlias, startDate, tableAlias, startDate)
			}
			return fmt.Sprintf("((%s.end_date >= '%s' AND %s.start_date <= '%s') OR (%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'))",
				tableAlias, startDate, tableAlias, endDate,
				tableAlias, startDate, tableAlias, endDate)
		default:
			if endDate == "" || endDate == startDate {
				return fmt.Sprintf("%s.end_date >= '%s'", tableAlias, startDate)
			}
			return fmt.Sprintf("%s.end_date >= '%s' AND %s.start_date <= '%s'", tableAlias, startDate, tableAlias, endDate)
		}
	case "dateJoin":
		switch forecasted {
		case "only":
			return fmt.Sprintf("%s.futureExpexctedStartDate <= ds.date AND %s.futureExpexctedEndDate >= ds.date", tableAlias, tableAlias)
		case "included":
			return fmt.Sprintf("((%s.start_date <= ds.date AND %s.end_date >= ds.date) OR (%s.futureExpexctedStartDate <= ds.date AND %s.futureExpexctedEndDate >= ds.date))",
				tableAlias, tableAlias, tableAlias, tableAlias)
		default:
			return fmt.Sprintf("%s.start_date <= ds.date AND %s.end_date >= ds.date", tableAlias, tableAlias)
		}
	case "finalDatesJoin":
		switch forecasted {
		case "only":
			return fmt.Sprintf("%s.futureExpexctedStartDate <= fd.end_date AND %s.futureExpexctedEndDate >= fd.start_date", tableAlias, tableAlias)
		case "included":
			return fmt.Sprintf("((%s.start_date <= fd.end_date AND %s.end_date >= fd.start_date) OR (%s.futureExpexctedStartDate <= fd.end_date AND %s.futureExpexctedEndDate >= fd.start_date))",
				tableAlias, tableAlias, tableAlias, tableAlias)
		default:
			return fmt.Sprintf("%s.start_date <= fd.end_date AND %s.end_date >= fd.start_date", tableAlias, tableAlias)
		}
	default:
		return ""
	}
}

func (s *SharedFunctionService) addActiveDateFilters(whereConditions *[]string, filterFields models.FilterDataDto) {
	forecasted := filterFields.Forecasted
	tableAlias := "ee"

	activeGteValue := s.getFilterValue(filterFields, "ActiveGte")
	activeGtValue := s.getFilterValue(filterFields, "ActiveGt")
	activeLteValue := s.getFilterValue(filterFields, "ActiveLte")
	activeLtValue := s.getFilterValue(filterFields, "ActiveLt")

	hasActiveGte := activeGteValue != ""
	hasActiveLte := activeLteValue != ""

	if hasActiveGte && hasActiveLte {
		switch forecasted {
		case "only":
			*whereConditions = append(*whereConditions, fmt.Sprintf("%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'",
				tableAlias, activeGteValue, tableAlias, activeLteValue))
		case "included":
			*whereConditions = append(*whereConditions, fmt.Sprintf("((%s.end_date >= '%s' AND %s.start_date <= '%s') OR (%s.futureExpexctedEndDate >= '%s' AND %s.futureExpexctedStartDate <= '%s'))",
				tableAlias, activeGteValue, tableAlias, activeLteValue,
				tableAlias, activeGteValue, tableAlias, activeLteValue))
		default:
			*whereConditions = append(*whereConditions, fmt.Sprintf("%s.end_date >= '%s' AND %s.start_date <= '%s'",
				tableAlias, activeGteValue, tableAlias, activeLteValue))
		}
		return
	}

	if hasActiveGte {
		if forecasted == "included" {
			originalField := s.getDateFieldName("", "end", tableAlias)
			futureField := s.getDateFieldName("only", "end", tableAlias)
			originalCondition := fmt.Sprintf("%s >= '%s'", originalField, activeGteValue)
			futureCondition := fmt.Sprintf("%s >= '%s'", futureField, activeGteValue)
			*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
		} else {
			switch forecasted {
			case "only":
				*whereConditions = append(*whereConditions, fmt.Sprintf("%s.end_date >= '%s' AND %s", tableAlias, activeGteValue, s.buildForecastedNotNullCondition(tableAlias)))
			default:
				*whereConditions = append(*whereConditions, fmt.Sprintf("ee.end_date >= '%s'", activeGteValue))
			}
		}
	}

	if activeGtValue != "" {
		if forecasted == "included" {
			originalField := s.getDateFieldName("", "end", tableAlias)
			futureField := s.getDateFieldName("only", "end", tableAlias)
			originalCondition := fmt.Sprintf("%s > '%s'", originalField, activeGtValue)
			futureCondition := fmt.Sprintf("%s > '%s'", futureField, activeGtValue)
			*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
		} else {
			switch forecasted {
			case "only":
				*whereConditions = append(*whereConditions, fmt.Sprintf("%s.end_date > '%s' AND %s", tableAlias, activeGtValue, s.buildForecastedNotNullCondition(tableAlias)))
			default:
				*whereConditions = append(*whereConditions, fmt.Sprintf("ee.end_date > '%s'", activeGtValue))
			}
		}
	}

	if activeLteValue != "" {
		if hasActiveGte {
			if forecasted == "included" {
				originalField := s.getDateFieldName("", "start", tableAlias)
				futureField := s.getDateFieldName("only", "start", tableAlias)
				originalCondition := fmt.Sprintf("%s <= '%s'", originalField, activeLteValue)
				futureCondition := fmt.Sprintf("%s <= '%s'", futureField, activeLteValue)
				*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
			} else {
				switch forecasted {
				case "only":
					futureField := s.getDateFieldName("only", "start", tableAlias)
					*whereConditions = append(*whereConditions, fmt.Sprintf("%s <= '%s'", futureField, activeLteValue))
				default:
					*whereConditions = append(*whereConditions, fmt.Sprintf("ee.start_date <= '%s'", activeLteValue))
				}
			}
		} else {
			if forecasted == "included" {
				originalField := s.getDateFieldName("", "end", tableAlias)
				futureField := s.getDateFieldName("only", "end", tableAlias)
				originalCondition := fmt.Sprintf("%s < '%s'", originalField, activeLteValue)
				futureCondition := fmt.Sprintf("%s < '%s'", futureField, activeLteValue)
				*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
			} else {
				switch forecasted {
				case "only":
					*whereConditions = append(*whereConditions, fmt.Sprintf("%s.end_date < '%s' AND %s", tableAlias, activeLteValue, s.buildForecastedNotNullCondition(tableAlias)))
				default:
					*whereConditions = append(*whereConditions, fmt.Sprintf("ee.end_date < '%s'", activeLteValue))
				}
			}
		}
	}

	if activeLtValue != "" {
		if hasActiveGte {
			if forecasted == "included" {
				originalField := s.getDateFieldName("", "start", tableAlias)
				futureField := s.getDateFieldName("only", "start", tableAlias)
				originalCondition := fmt.Sprintf("%s < '%s'", originalField, activeLtValue)
				futureCondition := fmt.Sprintf("%s < '%s'", futureField, activeLtValue)
				*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
			} else {
				switch forecasted {
				case "only":
					futureField := s.getDateFieldName("only", "start", tableAlias)
					*whereConditions = append(*whereConditions, fmt.Sprintf("%s < '%s'", futureField, activeLtValue))
				default:
					*whereConditions = append(*whereConditions, fmt.Sprintf("ee.start_date < '%s'", activeLtValue))
				}
			}
		} else {
			if forecasted == "included" {
				originalField := s.getDateFieldName("", "end", tableAlias)
				futureField := s.getDateFieldName("only", "end", tableAlias)
				originalCondition := fmt.Sprintf("%s < '%s'", originalField, activeLtValue)
				futureCondition := fmt.Sprintf("%s < '%s'", futureField, activeLtValue)
				*whereConditions = append(*whereConditions, fmt.Sprintf("((%s) OR (%s))", originalCondition, futureCondition))
			} else {
				switch forecasted {
				case "only":
					*whereConditions = append(*whereConditions, fmt.Sprintf("%s.end_date < '%s' AND %s", tableAlias, activeLtValue, s.buildForecastedNotNullCondition(tableAlias)))
				default:
					*whereConditions = append(*whereConditions, fmt.Sprintf("ee.end_date < '%s'", activeLtValue))
				}
			}
		}
	}
}

func (s *SharedFunctionService) addGeographicFilters(whereConditions *[]string, filterFields models.FilterDataDto, addTableAlias func(string) string) string {
	distanceOrderClause := ""

	if filterFields.Lat != "" && filterFields.Lon != "" && filterFields.Radius != "" && filterFields.Unit != "" {
		lat, lon, radiusInMeters := s.transformDataService.parseCoordinates(filterFields.Lat, filterFields.Lon, filterFields.Radius, filterFields.Unit)
		*whereConditions = append(*whereConditions, fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f", lat, lon, addTableAlias("edition_city_lat"), addTableAlias("edition_city_long"), radiusInMeters))
		if filterFields.EventDistanceOrder != "" {
			orderDirection := "ASC"
			if filterFields.EventDistanceOrder == "farthest" {
				orderDirection = "DESC"
			}
			distanceOrderClause = fmt.Sprintf("ORDER BY greatCircleDistance(%f, %f, lat, lon) %s", lat, lon, orderDirection)
		}
	}

	if filterFields.VenueLatitude != "" && filterFields.VenueLongitude != "" && filterFields.Radius != "" && filterFields.Unit != "" {
		lat, lon, radiusInMeters := s.transformDataService.parseCoordinates(filterFields.VenueLatitude, filterFields.VenueLongitude, filterFields.Radius, filterFields.Unit)
		*whereConditions = append(*whereConditions, fmt.Sprintf("greatCircleDistance(%f, %f, %s, %s) <= %f", lat, lon, addTableAlias("venue_lat"), addTableAlias("venue_long"), radiusInMeters))
		if distanceOrderClause == "" && filterFields.EventDistanceOrder != "" {
			orderDirection := "ASC"
			if filterFields.EventDistanceOrder == "farthest" {
				orderDirection = "DESC"
			}
			distanceOrderClause = fmt.Sprintf("ORDER BY greatCircleDistance(%f, %f, venueLat, venueLon) %s", lat, lon, orderDirection)
		}
	}

	return distanceOrderClause
}

func (s *SharedFunctionService) buildStatusCondition(filterFields models.FilterDataDto) string {
	if len(filterFields.ParsedStatus) == 0 {
		return "ee.status != 'U'"
	}
	statuses := make([]string, len(filterFields.ParsedStatus))
	for i, status := range filterFields.ParsedStatus {
		statuses[i] = fmt.Sprintf("'%s'", status)
	}

	return fmt.Sprintf("ee.status IN (%s)", strings.Join(statuses, ","))
}

func (s *SharedFunctionService) buildPublishedCondition(filterFields models.FilterDataDto) string {
	if filterFields.GroupBy != "" && filterFields.GroupBy == "eventTypeGroup" {
		return "ee.published in (1, 2, 4)"
	}
	// changes made due to GEO/GTM requests
	if len(filterFields.ParsedPublished) == 0 {
		return "ee.published IN ('1', '2')"
	}
	if len(filterFields.ParsedPublished) == 1 {
		return fmt.Sprintf("ee.published = '%s'", filterFields.ParsedPublished[0])
	}
	publishedValues := make([]string, len(filterFields.ParsedPublished))
	for i, published := range filterFields.ParsedPublished {
		publishedValues[i] = fmt.Sprintf("'%s'", published)
	}
	return fmt.Sprintf("ee.published IN (%s)", strings.Join(publishedValues, ","))
}

func (s *SharedFunctionService) buildEditionTypeCondition(filterFields models.FilterDataDto, tableAlias string) string {
	prefix := ""
	if tableAlias != "" {
		prefix = tableAlias + "."
	}
	if len(filterFields.ParsedEditionType) == 0 {
		return fmt.Sprintf("%sedition_type = 'current_edition'", prefix)
	}
	if len(filterFields.ParsedEditionType) == 1 {
		return fmt.Sprintf("%sedition_type = '%s'", prefix, filterFields.ParsedEditionType[0])
	}
	editionTypeValues := make([]string, len(filterFields.ParsedEditionType))
	for i, editionType := range filterFields.ParsedEditionType {
		editionTypeValues[i] = fmt.Sprintf("'%s'", editionType)
	}
	return fmt.Sprintf("%sedition_type IN (%s)", prefix, strings.Join(editionTypeValues, ","))
}

func (s *SharedFunctionService) buildListDataCountQuery(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	eventFilterSelectStr string,
	eventFilterGroupByStr string,
	hasEndDateFilters bool,
	filterFields models.FilterDataDto,
) string {
	today := time.Now().Format("2006-01-02")

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinClauses := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClauses = cteAndJoinResult.JoinClausesStr
	}

	whereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "ee"),
	}

	if !hasEndDateFilters {
		dateCondition := s.buildDefaultDateCondition(filterFields.Forecasted, "ee", today)
		whereConditions = append(whereConditions, dateCondition)
	}

	if queryResult.WhereClause != "" {
		whereConditions = append(whereConditions, queryResult.WhereClause)
	}

	if queryResult.SearchClause != "" {
		whereConditions = append(whereConditions, queryResult.SearchClause)
	}

	if len(cteAndJoinResult.JoinConditions) > 0 {
		whereConditions = append(whereConditions, cteAndJoinResult.JoinConditions...)
	}

	whereClause := strings.Join(whereConditions, "\n\t\t\tAND ")

	countQuery := fmt.Sprintf(`
		WITH %sevent_filter AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			%s
			WHERE %s
			GROUP BY %s
		),
		event_data AS (
			SELECT edition_id
			FROM testing_db.allevent_ch AS ee
			WHERE ee.edition_id in (SELECT edition_id from event_filter)
			GROUP BY edition_id
		)
		SELECT count(*) as total_count
		FROM event_data
	`,
		cteClausesStr,
		eventFilterSelectStr,
		func() string {
			if joinClauses != "" {
				return "\t\t" + joinClauses
			}
			return ""
		}(),
		whereClause,
		eventFilterGroupByStr)

	return countQuery
}

func (s *SharedFunctionService) getCountOnly(filterFields models.FilterDataDto) (int, error) {
	queryResult, err := s.buildClickHouseQuery(filterFields)
	if err != nil {
		log.Printf("Error building ClickHouse query: %v", err)
		return 0, err
	}

	eventFilterSelectFields := []string{"ee.event_id as event_id", "ee.edition_id as edition_id"}
	eventFilterGroupByFields := []string{"ee.event_id", "ee.edition_id"}

	if queryResult.DistanceOrderClause != "" && strings.Contains(queryResult.DistanceOrderClause, "greatCircleDistance") {
		if strings.Contains(queryResult.DistanceOrderClause, "lat") && strings.Contains(queryResult.DistanceOrderClause, "lon") {
			eventFilterSelectFields = append(eventFilterSelectFields, "ee.edition_city_lat as lat")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "lat")
			eventFilterSelectFields = append(eventFilterSelectFields, "ee.edition_city_long as lon")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "lon")
		}
		if strings.Contains(queryResult.DistanceOrderClause, "venueLat") && strings.Contains(queryResult.DistanceOrderClause, "venueLon") {
			eventFilterSelectFields = append(eventFilterSelectFields, "ee.venue_lat as venueLat")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "venueLat")
			eventFilterSelectFields = append(eventFilterSelectFields, "ee.venue_long as venueLon")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "venueLon")
		}
	}

	eventFilterSelectStr := strings.Join(eventFilterSelectFields, ", ")
	eventFilterGroupByStr := strings.Join(eventFilterGroupByFields, ", ")

	var resolvedCountryIsos, resolvedCategoryNames []string
	if queryResult.NeedsEventRankingJoin {
		resolvedCountryIsos, resolvedCategoryNames, _ = s.GetEventRankingScopeResolved(filterFields)
	}

	cteAndJoinResult := s.buildFilterCTEsAndJoins(
		queryResult.NeedsVisitorJoin,
		queryResult.NeedsSpeakerJoin,
		queryResult.NeedsExhibitorJoin,
		queryResult.NeedsSponsorJoin,
		queryResult.NeedsCategoryJoin,
		queryResult.NeedsTypeJoin,
		queryResult.NeedsEventRankingJoin,
		queryResult.needsDesignationJoin,
		queryResult.needsAudienceSpreadJoin,
		queryResult.NeedsRegionsJoin,
		queryResult.NeedsLocationIdsJoin,
		queryResult.NeedsCountryIdsJoin,
		queryResult.NeedsStateIdsJoin,
		queryResult.NeedsCityIdsJoin,
		queryResult.NeedsVenueIdsJoin,
		queryResult.NeedsUserIdUnionCTE,
		queryResult.VisitorWhereConditions,
		queryResult.SpeakerWhereConditions,
		queryResult.ExhibitorWhereConditions,
		queryResult.SponsorWhereConditions,
		queryResult.OrganizerWhereConditions,
		queryResult.CategoryWhereConditions,
		queryResult.TypeWhereConditions,
		queryResult.EventRankingWhereConditions,
		queryResult.JobCompositeWhereConditions,
		queryResult.AudienceSpreadWhereConditions,
		queryResult.RegionsWhereConditions,
		queryResult.LocationIdsWhereConditions,
		queryResult.CountryIdsWhereConditions,
		queryResult.StateIdsWhereConditions,
		queryResult.CityIdsWhereConditions,
		queryResult.VenueIdsWhereConditions,
		queryResult.UserIdWhereConditions,
		queryResult.NeedsCompanyIdUnionCTE,
		queryResult.CompanyIdWhereConditions,
		queryResult.WhereClause,
		queryResult.SearchClause,
		resolvedCountryIsos,
		resolvedCategoryNames,
		filterFields,
	)

	hasEndDateFilters := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
		filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != "" ||
		filterFields.CreatedAt != "" || len(filterFields.ParsedEventIds) > 0 || len(filterFields.ParsedNotEventIds) > 0 || len(filterFields.ParsedSourceEventIds) > 0 || len(filterFields.ParsedDates) > 0 || filterFields.ParsedPastBetween != nil || filterFields.ParsedActiveBetween != nil

	countQuery := s.buildListDataCountQuery(
		queryResult,
		&cteAndJoinResult,
		eventFilterSelectStr,
		eventFilterGroupByStr,
		hasEndDateFilters,
		filterFields,
	)

	log.Printf("Count query: %s", countQuery)

	countQueryTime := time.Now()
	countResult, err := s.clickhouseService.ExecuteQuery(context.Background(), countQuery)
	if err != nil {
		log.Printf("ClickHouse count query error: %v", err)
		return 0, err
	}
	defer countResult.Close()

	countQueryDuration := time.Since(countQueryTime)
	log.Printf("Count query time: %v", countQueryDuration)

	var totalCount uint64
	if countResult.Next() {
		if err := countResult.Scan(&totalCount); err != nil {
			log.Printf("Error scanning count result: %v", err)
			return 0, err
		}
	}

	return int(totalCount), nil
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
		queryKeywords := strings.Split(filterFields.Q, ",")
		var qConditions []string

		for _, keyword := range queryKeywords {
			cleanKeyword := strings.TrimSpace(keyword)
			cleanKeyword = strings.ReplaceAll(cleanKeyword, "\\", "\\\\")
			cleanKeyword = strings.ReplaceAll(cleanKeyword, "'", "''")
			if cleanKeyword != "" {
				qConditions = append(qConditions, fmt.Sprintf("ee.event_name ILIKE '%%%s%%'", cleanKeyword))
			}
		}

		if len(qConditions) > 0 {
			searchClause.WriteString(fmt.Sprintf("(%s)", strings.Join(qConditions, " OR ")))
		}
	}

	if filterFields.ParsedKeywords != nil {
		keywords := filterFields.ParsedKeywords

		if len(keywords.IncludeForQuery) > 0 {
			var includeConditions []string
			for _, keyword := range keywords.IncludeForQuery {
				keywordEscaped := strings.ToLower(strings.ReplaceAll(keyword, "'", "''"))
				parts := models.KeywordPartsForMatch(keywordEscaped)
				if len(parts) >= 2 {
					hasParts := make([]string, len(parts))
					for i, p := range parts {
						hasParts[i] = fmt.Sprintf("has(ee.keywords, '%s')", p)
					}
					includeConditions = append(includeConditions, fmt.Sprintf("(%s)", strings.Join(hasParts, " AND ")))
				} else {
					includeConditions = append(includeConditions, fmt.Sprintf("has(ee.keywords, '%s')", keywordEscaped))
				}
			}
			if searchClause.Len() > 0 {
				searchClause.WriteString(fmt.Sprintf(" AND (%s)", strings.Join(includeConditions, " OR ")))
			} else {
				searchClause.WriteString(fmt.Sprintf("(%s)", strings.Join(includeConditions, " OR ")))
			}
		}

		if len(keywords.ExcludeForQuery) > 0 {
			var excludeConditions []string
			for _, keyword := range keywords.ExcludeForQuery {
				keywordEscaped := strings.ToLower(strings.ReplaceAll(keyword, "'", "''"))
				parts := models.KeywordPartsForMatch(keywordEscaped)
				if len(parts) >= 2 {
					hasParts := make([]string, len(parts))
					for i, p := range parts {
						hasParts[i] = fmt.Sprintf("has(ee.keywords, '%s')", p)
					}
					excludeConditions = append(excludeConditions, fmt.Sprintf("NOT (%s)", strings.Join(hasParts, " AND ")))
				} else {
					excludeConditions = append(excludeConditions, fmt.Sprintf("NOT has(ee.keywords, '%s')", keywordEscaped))
				}
			}
			if searchClause.Len() > 0 {
				searchClause.WriteString(fmt.Sprintf(" AND (%s)", strings.Join(excludeConditions, " OR ")))
			} else {
				searchClause.WriteString(fmt.Sprintf("(%s)", strings.Join(excludeConditions, " OR ")))
			}
		}
	}

	return searchClause.String()
}

func (s *SharedFunctionService) isStandaloneActiveDateFilter(filterFields models.FilterDataDto) bool {
	if filterFields.Forecasted != "only" {
		return false
	}
	activeFilterKeys := []string{"ActiveGte", "ActiveLte", "ActiveGt", "ActiveLt"}
	setCount := 0
	for _, key := range activeFilterKeys {
		if s.getFilterValue(filterFields, key) != "" {
			setCount++
		}
	}
	return setCount == 1
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
		s.buildEditionTypeCondition(filterFields, "ee"),
		fmt.Sprintf("end_date >= '%s'", today),
	}

	queryResult, err := s.buildClickHouseQuery(filterFields)
	if err != nil {
		return "", err
	}

	if queryResult.NeedsRegionsJoin && len(queryResult.RegionsWhereConditions) > 0 {
		regionsWhereClause := strings.Join(queryResult.RegionsWhereConditions, " AND ")
		regionsCTE := fmt.Sprintf(`filtered_regions AS (
			SELECT iso
			FROM testing_db.location_ch
			WHERE %s
		)`, regionsWhereClause)
		cteClauses = append(cteClauses, regionsCTE)
	}

	if queryResult.NeedsLocationIdsJoin && len(queryResult.LocationIdsWhereConditions) > 0 {
		locationIdsWhereClause := strings.Join(queryResult.LocationIdsWhereConditions, " AND ")

		locationIdsCTE := fmt.Sprintf(`filtered_locations AS (
			SELECT location_type, iso, id
			FROM testing_db.location_ch
			WHERE %s
		)`, locationIdsWhereClause)
		cteClauses = append(cteClauses, locationIdsCTE)
	}

	if queryResult.NeedsCountryIdsJoin && len(queryResult.CountryIdsWhereConditions) > 0 {
		countryIdsWhereClause := strings.Join(queryResult.CountryIdsWhereConditions, " AND ")

		countryIdsCTE := fmt.Sprintf(`filtered_country_ids AS (
			SELECT iso
			FROM testing_db.location_ch
			WHERE %s
		)`, countryIdsWhereClause)
		cteClauses = append(cteClauses, countryIdsCTE)
	}

	if queryResult.NeedsStateIdsJoin && len(queryResult.StateIdsWhereConditions) > 0 {
		stateIdsWhereClause := strings.Join(queryResult.StateIdsWhereConditions, " AND ")

		stateIdsCTE := fmt.Sprintf(`filtered_state_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, stateIdsWhereClause)
		cteClauses = append(cteClauses, stateIdsCTE)
	}

	if queryResult.NeedsCityIdsJoin && len(queryResult.CityIdsWhereConditions) > 0 {
		cityIdsWhereClause := strings.Join(queryResult.CityIdsWhereConditions, " AND ")

		cityIdsCTE := fmt.Sprintf(`filtered_city_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, cityIdsWhereClause)
		cteClauses = append(cteClauses, cityIdsCTE)
	}

	if queryResult.NeedsVenueIdsJoin && len(queryResult.VenueIdsWhereConditions) > 0 {
		venueIdsWhereClause := strings.Join(queryResult.VenueIdsWhereConditions, " AND ")

		venueIdsCTE := fmt.Sprintf(`filtered_venue_ids AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE %s
		)`, venueIdsWhereClause)
		cteClauses = append(cteClauses, venueIdsCTE)
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
		resolvedCountryIsos, resolvedCategoryNames, _ := s.GetEventRankingScopeResolved(filterFields)

		preEventFilterConditions := []string{
			s.buildPublishedCondition(filterFields),
			s.buildStatusCondition(filterFields),
			s.buildEditionTypeCondition(filterFields, "ee"),
		}

		hasUserEndDateFilter := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
			filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != ""
		if !hasUserEndDateFilter {
			preEventFilterConditions = append(preEventFilterConditions, fmt.Sprintf("end_date >= '%s'", today))
		}
		if strings.TrimSpace(queryResult.WhereClause) != "" {
			preEventFilterConditions = append(preEventFilterConditions, queryResult.WhereClause)
		}
		if strings.TrimSpace(queryResult.SearchClause) != "" {
			preEventFilterConditions = append(preEventFilterConditions, queryResult.SearchClause)
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
				FROM testing_db.allevent_ch AS ee
				WHERE event_id IN (SELECT %s FROM %s)
				AND %s
				GROUP BY event_id, edition_id
				ORDER BY event_id ASC
			)`, selectColumn, previousCTE, preEventFilterWhereClause)
		} else {
			preEventFilterCTE = fmt.Sprintf(`pre_event_filter AS (
				SELECT event_id, edition_id
				FROM testing_db.allevent_ch AS ee
				WHERE %s
				GROUP BY event_id, edition_id
				ORDER BY event_id ASC
			)`, preEventFilterWhereClause)
		}

		cteClauses = append(cteClauses, preEventFilterCTE)
		currentMonth := time.Now().Month()
		currentMonthCondition := fmt.Sprintf("MONTH(created) = %d", currentMonth)

		eventRankingConditions := []string{currentMonthCondition}
		scopeConditionsHierarchical, err := s.BuildEventRankingScopeConditions(filterFields, resolvedCountryIsos, resolvedCategoryNames)
		if err != nil {
			scopeConditionsHierarchical = "((country = '' AND category_name = ''))"
		}
		if scopeConditionsHierarchical != "" {
			eventRankingConditions = append(eventRankingConditions, scopeConditionsHierarchical)
		}
		if len(queryResult.EventRankingWhereConditions) > 0 {
			eventRankingConditions = append(eventRankingConditions, queryResult.EventRankingWhereConditions...)
		}
		eventRankingLimit := filterFields.ParsedEventRanking[0]

		filteredEventRankingCTE := fmt.Sprintf(`filtered_event_ranking AS (
			SELECT er.event_id
			FROM testing_db.event_ranking_ch AS er
			INNER JOIN (SELECT event_id FROM pre_event_filter) AS pef ON er.event_id = pef.event_id
			WHERE %s
			GROUP BY er.event_id LIMIT %s
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
	from := "FROM testing_db.allevent_ch ee"

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

	if s.containsCTE(cteClauses, "filtered_user_events") {
		from += " INNER JOIN filtered_user_events fue ON ee.event_id = fue.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_company_events") {
		from += " INNER JOIN filtered_company_events fce ON ee.event_id = fce.event_id"
	}
	if s.containsCTE(cteClauses, "filtered_company_events_by_name") {
		from += " INNER JOIN filtered_company_events_by_name fcebn ON ee.event_id = fcebn.event_id"
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

func (s *SharedFunctionService) GetCountryDataByISO(iso string) map[string]interface{} {
	for _, country := range CountryData {
		if countryID, ok := country["id"].(string); ok && countryID == iso {
			return country
		}
	}
	return nil
}

// func (s *SharedFunctionService) getDesignationIdsByDepartment(ctx context.Context, designationUUIDs []string) ([]string, error) {
// 	if len(designationUUIDs) == 0 {
// 		return []string{}, nil
// 	}

// 	uuidList := make([]string, len(designationUUIDs))
// 	for i, uuid := range designationUUIDs {
// 		uuidList[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''"))
// 	}

// 	query := fmt.Sprintf(`
// 		SELECT DISTINCT designation_uuid
// 		FROM testing_db.event_designation_ch
// 		WHERE department IN (
// 			SELECT DISTINCT department
// 			FROM testing_db.event_designation_ch
// 			WHERE designation_uuid IN (%s)
// 		)`, strings.Join(uuidList, ","))

// 	log.Printf("Designation ID by department Query: %s", query)

// 	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to query designation IDs by department: %w", err)
// 	}
// 	defer rows.Close()

// 	var result []string
// 	for rows.Next() {
// 		var designationUUID string
// 		if err := rows.Scan(&designationUUID); err != nil {
// 			return nil, fmt.Errorf("failed to scan designation UUID: %w", err)
// 		}
// 		result = append(result, designationUUID)
// 	}

// 	return result, nil
// }

type DesignationWithDepartment struct {
	ID         string
	Department string
}

type DesignationWithRole struct {
	ID   string
	Role string
}

type DesignationWithName struct {
	ID   string
	Name string
}

func (s *SharedFunctionService) getDesignationIdsByDepartmentWithProps(ctx context.Context, designationUUIDs []string) ([]DesignationWithDepartment, error) {
	if len(designationUUIDs) == 0 {
		return []DesignationWithDepartment{}, nil
	}

	uuidList := make([]string, len(designationUUIDs))
	for i, uuid := range designationUUIDs {
		uuidList[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''"))
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT designation_uuid, department
		FROM testing_db.event_designation_ch
		WHERE department IN (
			SELECT DISTINCT department
			FROM testing_db.event_designation_ch
			WHERE designation_uuid IN (%s)
		)
		AND department != ''`, strings.Join(uuidList, ","))

	log.Printf("Designation ID by department with props Query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query designation IDs by department: %w", err)
	}
	defer rows.Close()

	var result []DesignationWithDepartment
	for rows.Next() {
		var designationUUID, department string
		if err := rows.Scan(&designationUUID, &department); err != nil {
			return nil, fmt.Errorf("failed to scan designation UUID and department: %w", err)
		}
		result = append(result, DesignationWithDepartment{
			ID:         designationUUID,
			Department: department,
		})
	}

	return result, nil
}

// func (s *SharedFunctionService) getSeniorityIdsByRole(ctx context.Context, seniorityUUIDs []string) ([]string, error) {
// 	if len(seniorityUUIDs) == 0 {
// 		return []string{}, nil
// 	}

// 	uuidList := make([]string, len(seniorityUUIDs))
// 	for i, uuid := range seniorityUUIDs {
// 		uuidList[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''"))
// 	}

// 	query := fmt.Sprintf(`
// 		SELECT DISTINCT designation_uuid
// 		FROM testing_db.event_designation_ch
// 		WHERE role IN (
// 			SELECT DISTINCT role
// 			FROM testing_db.event_designation_ch
// 			WHERE designation_uuid IN (%s)
// 		)`, strings.Join(uuidList, ","))

// 	log.Printf("Seniority ID by role Query: %s", query)

// 	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to query designation IDs by role: %w", err)
// 	}
// 	defer rows.Close()

// 	var result []string
// 	for rows.Next() {
// 		var designationUUID string
// 		if err := rows.Scan(&designationUUID); err != nil {
// 			return nil, fmt.Errorf("failed to scan designation UUID: %w", err)
// 		}
// 		result = append(result, designationUUID)
// 	}

// 	return result, nil
// }

func (s *SharedFunctionService) getSeniorityIdsByRoleWithProps(ctx context.Context, seniorityUUIDs []string) ([]DesignationWithRole, error) {
	if len(seniorityUUIDs) == 0 {
		return []DesignationWithRole{}, nil
	}

	uuidList := make([]string, len(seniorityUUIDs))
	for i, uuid := range seniorityUUIDs {
		uuidList[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''"))
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT designation_uuid, role
		FROM testing_db.event_designation_ch
		WHERE role IN (
			SELECT DISTINCT role
			FROM testing_db.event_designation_ch
			WHERE designation_uuid IN (%s)
		)
		AND role != ''`, strings.Join(uuidList, ","))

	log.Printf("Seniority ID by role with props Query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query designation IDs by role: %w", err)
	}
	defer rows.Close()

	var result []DesignationWithRole
	for rows.Next() {
		var designationUUID, role string
		if err := rows.Scan(&designationUUID, &role); err != nil {
			return nil, fmt.Errorf("failed to scan designation UUID and role: %w", err)
		}
		result = append(result, DesignationWithRole{
			ID:   designationUUID,
			Role: role,
		})
	}

	return result, nil
}

func (s *SharedFunctionService) getDesignationNamesByIds(ctx context.Context, designationUUIDs []string) ([]DesignationWithName, error) {
	if len(designationUUIDs) == 0 {
		return []DesignationWithName{}, nil
	}

	uuidList := make([]string, len(designationUUIDs))
	for i, uuid := range designationUUIDs {
		uuidList[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''"))
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT designation_uuid, display_name
		FROM testing_db.event_designation_ch
		WHERE designation_uuid IN (%s)
		AND display_name != ''`, strings.Join(uuidList, ","))

	log.Printf("Designation names by IDs Query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query designation names by IDs: %w", err)
	}
	defer rows.Close()

	var result []DesignationWithName
	for rows.Next() {
		var designationUUID, name string
		if err := rows.Scan(&designationUUID, &name); err != nil {
			return nil, fmt.Errorf("failed to scan designation UUID and name: %w", err)
		}
		result = append(result, DesignationWithName{
			ID:   designationUUID,
			Name: name,
		})
	}

	return result, nil
}

func deduplicateStrings(strs []string) []string {
	if len(strs) == 0 {
		return []string{}
	}

	seen := make(map[string]bool)
	result := make([]string, 0, len(strs))

	for _, str := range strs {
		trimmed := strings.TrimSpace(str)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			result = append(result, trimmed)
		}
	}

	return result
}

func findValidEventTypes(eventTypes []string, eventTypeGroup string) []string {
	eventTypeGroupLower := strings.ToLower(strings.TrimSpace(eventTypeGroup))
	if !validEventTypeGroups[eventTypeGroupLower] {
		return []string{}
	}

	validEventTypes := []string{}

	for _, eventType := range eventTypes {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}

		eventTypeSlug, exists := models.EventTypeById[eventType]
		if !exists {
			continue
		}

		et, exists := EventTypeGroups[eventTypeSlug]
		if !exists {
			continue
		}

		if strings.EqualFold(et.Group, eventTypeGroupLower) {
			validEventTypes = append(validEventTypes, eventType)
		}
	}

	return validEventTypes
}

func (s *SharedFunctionService) validateParameters(filterFields models.FilterDataDto) (models.FilterDataDto, error) {
	ctx := context.Background()

	// Handle top-level ParsedDesignationId
	// if len(filterFields.ParsedDesignationId) > 0 {
	// 	expandedIds, err := s.getDesignationIdsByDepartment(ctx, filterFields.ParsedDesignationId)
	// 	if err != nil {
	// 		return filterFields, fmt.Errorf("failed to expand designation IDs by department: %w", err)
	// 	}
	// 	filterFields.ParsedDesignationId = expandedIds
	// }

	// Handle top-level ParsedSeniorityId
	// if len(filterFields.ParsedSeniorityId) > 0 {
	// 	expandedIds, err := s.getSeniorityIdsByRole(ctx, filterFields.ParsedSeniorityId)
	// 	if err != nil {
	// 		return filterFields, fmt.Errorf("failed to expand seniority IDs by role: %w", err)
	// 	}
	// 	filterFields.ParsedSeniorityId = expandedIds
	// }

	// jobComposite filter
	if filterFields.ParsedJobCompositeFilter != nil {
		uniqueDesignationIds := make(map[string]bool)
		propertyIds := make([]string, 0)
		property := &models.JobCompositeProperty{}

		//conditions.DepartmentIds
		if filterFields.ParsedJobCompositeFilter.Conditions != nil {
			if len(filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds) > 0 {
				expanded, err := s.getDesignationIdsByDepartmentWithProps(ctx, filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds)
				if err != nil {
					return filterFields, fmt.Errorf("failed to expand department IDs in jobComposite conditions: %w", err)
				}

				departmentSet := make(map[string]bool)
				for _, item := range expanded {
					trimmedID := strings.TrimSpace(item.ID)
					if trimmedID != "" {
						uniqueDesignationIds[trimmedID] = true
						propertyIds = append(propertyIds, trimmedID)

						trimmedDept := strings.TrimSpace(item.Department)
						if trimmedDept != "" {
							departmentSet[trimmedDept] = true
						}
					}
				}

				// Collect unique departments
				property.Department = make([]string, 0, len(departmentSet))
				for dept := range departmentSet {
					property.Department = append(property.Department, dept)
				}
				filterFields.ParsedJobCompositeFilter.Conditions.DepartmentIds = nil
			}

			// conditions.SeniorityIds
			if len(filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds) > 0 {
				expanded, err := s.getSeniorityIdsByRoleWithProps(ctx, filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds)
				if err != nil {
					return filterFields, fmt.Errorf("failed to expand seniority IDs in jobComposite conditions: %w", err)
				}

				roleSet := make(map[string]bool)
				for _, item := range expanded {
					trimmedID := strings.TrimSpace(item.ID)
					if trimmedID != "" {
						uniqueDesignationIds[trimmedID] = true
						propertyIds = append(propertyIds, trimmedID)

						trimmedRole := strings.TrimSpace(item.Role)
						if trimmedRole != "" {
							roleSet[trimmedRole] = true
						}
					}
				}

				// Collect unique roles
				property.Role = make([]string, 0, len(roleSet))
				for role := range roleSet {
					property.Role = append(property.Role, role)
				}

				// Clear original condition array
				filterFields.ParsedJobCompositeFilter.Conditions.SeniorityIds = nil
			}
		}

		// Handle direct DesignationIds
		if len(filterFields.ParsedJobCompositeFilter.DesignationIds) > 0 {
			designationData, err := s.getDesignationNamesByIds(ctx, filterFields.ParsedJobCompositeFilter.DesignationIds)
			if err != nil {
				return filterFields, fmt.Errorf("failed to fetch designation names by IDs: %w", err)
			}

			nameSet := make(map[string]bool)
			for _, item := range designationData {
				trimmedID := strings.TrimSpace(item.ID)
				if trimmedID != "" {
					uniqueDesignationIds[trimmedID] = true
					propertyIds = append(propertyIds, trimmedID)

					trimmedName := strings.TrimSpace(item.Name)
					if trimmedName != "" {
						nameSet[trimmedName] = true
					}
				}
			}

			// Collect unique names
			property.Name = make([]string, 0, len(nameSet))
			for name := range nameSet {
				property.Name = append(property.Name, name)
			}
		}

		consolidatedIds := make([]string, 0, len(uniqueDesignationIds))
		for id := range uniqueDesignationIds {
			consolidatedIds = append(consolidatedIds, id)
		}
		filterFields.ParsedJobCompositeFilter.DesignationIds = consolidatedIds
		filterFields.ParsedJobCompositeFilter.PropertyIds = deduplicateStrings(propertyIds)
		hasPropertyValues := len(property.Department) > 0 || len(property.Role) > 0 || len(property.Name) > 0
		if hasPropertyValues {
			filterFields.ParsedJobCompositeFilter.Property = property
		}
	}

	return filterFields, nil
}

func (s *SharedFunctionService) getISOFromLocationUUIDs(locationUUIDs []string) ([]string, error) {
	if len(locationUUIDs) == 0 {
		return []string{}, nil
	}

	escapeSqlValue := func(value string) string {
		if value == "" {
			return ""
		}
		value = strings.Trim(value, "'")
		escaped := strings.ReplaceAll(value, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}

	uuidList := make([]string, 0, len(locationUUIDs))
	for _, uuid := range locationUUIDs {
		escapedUUID := escapeSqlValue(uuid)
		uuidList = append(uuidList, escapedUUID)
	}
	uuidListStr := strings.Join(uuidList, ",")

	query := fmt.Sprintf(`
		SELECT DISTINCT iso
		FROM testing_db.location_ch
		WHERE location_type = 'COUNTRY' 
		AND id_uuid IN (%s)
		AND iso IS NOT NULL
		AND iso != ''
	`, uuidListStr)

	log.Printf("Fetching ISO codes from location UUIDs query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("Error fetching ISO codes from location UUIDs: %v", err)
		return nil, err
	}
	defer rows.Close()

	var isoCodes []string
	for rows.Next() {
		var iso string
		if err := rows.Scan(&iso); err != nil {
			log.Printf("Error scanning ISO code row: %v", err)
			continue
		}
		if iso != "" {
			isoCodes = append(isoCodes, iso)
		}
	}

	log.Printf("Found %d ISO codes from %d location UUIDs", len(isoCodes), len(locationUUIDs))
	return isoCodes, nil
}

func (s *SharedFunctionService) getCategoryNamesFromUUIDs(categoryUUIDs []string) ([]string, error) {
	if len(categoryUUIDs) == 0 {
		return []string{}, nil
	}

	escapeSqlValue := func(value string) string {
		if value == "" {
			return ""
		}
		value = strings.Trim(value, "'")
		escaped := strings.ReplaceAll(value, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}

	uuidList := make([]string, 0, len(categoryUUIDs))
	for _, uuid := range categoryUUIDs {
		escapedUUID := escapeSqlValue(uuid)
		uuidList = append(uuidList, escapedUUID)
	}
	uuidListStr := strings.Join(uuidList, ",")

	query := fmt.Sprintf(`
		SELECT DISTINCT name
		FROM testing_db.event_category_ch
		WHERE category_uuid IN (%s)
		AND name IS NOT NULL
		AND name != ''
		AND is_group = 1
	`, uuidListStr)

	log.Printf("Fetching category names from category UUIDs query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("Error fetching category names from category UUIDs: %v", err)
		return nil, err
	}
	defer rows.Close()

	var categoryNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Printf("Error scanning category name row: %v", err)
			continue
		}
		if name != "" {
			categoryNames = append(categoryNames, name)
		}
	}

	log.Printf("Found %d category names from %d category UUIDs", len(categoryNames), len(categoryUUIDs))
	return categoryNames, nil
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	parts := strings.Split(s, "-")
	return len(parts) == 5 && len(parts[0]) == 8 && len(parts[1]) == 4 && len(parts[2]) == 4 && len(parts[3]) == 4 && len(parts[4]) == 12
}

type EventLocationData struct {
	VenueID            *uint32
	VenueCity          *uint32
	EditionCity        *uint32
	EditionCityStateID *uint32
	EditionCountry     *string
}

func (s *SharedFunctionService) FetchEventLocationData(eventIds []uint32, filterFields models.FilterDataDto) (map[uint32]*EventLocationData, error) {
	if len(eventIds) == 0 {
		return make(map[uint32]*EventLocationData), nil
	}

	var eventIdsStr []string
	for _, id := range eventIds {
		eventIdsStr = append(eventIdsStr, fmt.Sprintf("%d", id))
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")

	editionTypeCondition := s.buildEditionTypeCondition(filterFields, "")

	query := fmt.Sprintf(`
		SELECT 
			event_id,
			venue_id,
			venue_city,
			edition_city,
			edition_city_state_id,
			edition_country
		FROM testing_db.allevent_ch
		WHERE event_id IN (%s)
		AND %s
		GROUP BY event_id, venue_id, venue_city, edition_city, edition_city_state_id, edition_country
	`, eventIdsStrJoined, editionTypeCondition)

	log.Printf("Event location data query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("Error fetching event location data: %v", err)
		return nil, err
	}
	defer rows.Close()

	locationDataMap := make(map[uint32]*EventLocationData)

	for rows.Next() {
		var eventID uint32
		var venueID, venueCity, editionCity, editionCityStateID *uint32
		var editionCountry *string

		if err := rows.Scan(&eventID, &venueID, &venueCity, &editionCity, &editionCityStateID, &editionCountry); err != nil {
			log.Printf("Error scanning event location data row: %v", err)
			continue
		}

		locationDataMap[eventID] = &EventLocationData{
			VenueID:            venueID,
			VenueCity:          venueCity,
			EditionCity:        editionCity,
			EditionCityStateID: editionCityStateID,
			EditionCountry:     editionCountry,
		}
	}

	return locationDataMap, nil
}

func (s *SharedFunctionService) FetchLocations(locationDataMap map[uint32]*EventLocationData) (map[uint32]map[string]interface{}, error) {
	if len(locationDataMap) == 0 {
		return make(map[uint32]map[string]interface{}), nil
	}

	venueIDSet := make(map[uint32]bool)
	cityIDSet := make(map[uint32]bool)
	stateIDSet := make(map[uint32]bool)
	countryISOSet := make(map[string]bool)

	for _, data := range locationDataMap {
		if data.VenueID != nil {
			venueIDSet[*data.VenueID] = true
		}
		if data.EditionCity != nil {
			cityIDSet[*data.EditionCity] = true
		}
		if data.VenueCity != nil {
			cityIDSet[*data.VenueCity] = true
		}
		if data.EditionCityStateID != nil {
			stateIDSet[*data.EditionCityStateID] = true
		}
		if data.EditionCountry != nil && *data.EditionCountry != "" {
			countryISOSet[*data.EditionCountry] = true
		}
	}

	log.Printf("Location query stats - Venues: %d, Cities: %d, States: %d, Countries: %d, Total events: %d",
		len(venueIDSet), len(cityIDSet), len(stateIDSet), len(countryISOSet), len(locationDataMap))

	var whereConditions []string

	if len(venueIDSet) > 0 {
		var venueIDs []string
		for id := range venueIDSet {
			venueIDs = append(venueIDs, fmt.Sprintf("%d", id))
		}
		venueIDsStr := strings.Join(venueIDs, ",")
		whereConditions = append(whereConditions, fmt.Sprintf("(location_type = 'VENUE' AND id IN (%s))", venueIDsStr))
	}

	if len(cityIDSet) > 0 {
		var cityIDs []string
		for id := range cityIDSet {
			cityIDs = append(cityIDs, fmt.Sprintf("%d", id))
		}
		cityIDsStr := strings.Join(cityIDs, ",")
		whereConditions = append(whereConditions, fmt.Sprintf("(location_type = 'CITY' AND id IN (%s))", cityIDsStr))
	}

	if len(stateIDSet) > 0 {
		var stateIDs []string
		for id := range stateIDSet {
			stateIDs = append(stateIDs, fmt.Sprintf("%d", id))
		}
		stateIDsStr := strings.Join(stateIDs, ",")
		whereConditions = append(whereConditions, fmt.Sprintf("(location_type = 'STATE' AND id IN (%s))", stateIDsStr))
	}

	if len(countryISOSet) > 0 {
		var countryISOs []string
		for iso := range countryISOSet {
			countryISOs = append(countryISOs, fmt.Sprintf("'%s'", iso))
		}
		countryISOsStr := strings.Join(countryISOs, ",")
		whereConditions = append(whereConditions, fmt.Sprintf("(location_type = 'COUNTRY' AND iso IN (%s))", countryISOsStr))
	}

	if len(whereConditions) == 0 {
		return make(map[uint32]map[string]interface{}), nil
	}

	query := fmt.Sprintf(`
		SELECT 
			CASE 
				WHEN location_type = 'VENUE' THEN toUInt32(grouping_id_num)
				WHEN location_type = 'CITY' THEN toUInt32(grouping_id_num)
				WHEN location_type = 'STATE' THEN toUInt32(grouping_id_num)
				WHEN location_type = 'COUNTRY' THEN toUInt32(0)
				ELSE toUInt32(0)
			END AS lookup_id,
			location_type,
			toString(id_uuid) AS id,
			name,
			toString(city_uuid) AS cityId,
			city_name AS cityName,
			toString(state_uuid) AS stateId,
			state_name AS stateName,
			toString(country_uuid) AS countryId,
			country_name AS countryName,
			toString(latitude) AS latitude,
			toString(longitude) AS longitude,
			address,
			regions,
			toString(iso) AS countryIso
		FROM (
			SELECT 
				location_type,
				grouping_key,
				argMax(id_uuid, last_updated_at) AS id_uuid,
				argMax(name, last_updated_at) AS name,
				argMax(city_uuid, last_updated_at) AS city_uuid,
				argMax(city_name, last_updated_at) AS city_name,
				argMax(state_uuid, last_updated_at) AS state_uuid,
				argMax(state_name, last_updated_at) AS state_name,
				argMax(country_uuid, last_updated_at) AS country_uuid,
				argMax(country_name, last_updated_at) AS country_name,
				argMax(latitude, last_updated_at) AS latitude,
				argMax(longitude, last_updated_at) AS longitude,
				argMax(address, last_updated_at) AS address,
				argMax(regions, last_updated_at) AS regions,
				argMax(iso, last_updated_at) AS iso,
				argMax(grouping_id_num, last_updated_at) AS grouping_id_num
			FROM (
				SELECT 
					location_type,
					CASE 
						WHEN location_type = 'VENUE' THEN toString(id)
						WHEN location_type = 'CITY' THEN toString(id)
						WHEN location_type = 'STATE' THEN toString(id)
						WHEN location_type = 'COUNTRY' THEN toString(iso)
						ELSE ''
					END AS grouping_key,
					CASE 
						WHEN location_type = 'VENUE' THEN id
						WHEN location_type = 'CITY' THEN id
						WHEN location_type = 'STATE' THEN id
						WHEN location_type = 'COUNTRY' THEN 0
						ELSE 0
					END AS grouping_id_num,
					id_uuid,
					name,
					city_uuid,
					city_name,
					state_uuid,
					state_name,
					country_uuid,
					country_name,
					latitude,
					longitude,
					address,
					regions,
					iso,
					last_updated_at
				FROM testing_db.location_ch
				WHERE %s
			)
			GROUP BY location_type, grouping_key
		)
	`, strings.Join(whereConditions, " OR "))

	log.Printf("Locations query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("Error fetching locations: %v", err)
		return nil, err
	}
	defer rows.Close()

	locationLookup := make(map[string]map[string]interface{})
	rowCount := 0
	stateKeys := make([]string, 0)

	for rows.Next() {
		rowCount++
		var lookupID uint32
		var locationType, id, name, cityID, cityName, stateID, stateName, countryID, countryName, latitude, longitude, address, countryIso *string
		var regions []string

		if err := rows.Scan(&lookupID, &locationType, &id, &name, &cityID, &cityName, &stateID, &stateName, &countryID, &countryName, &latitude, &longitude, &address, &regions, &countryIso); err != nil {
			log.Printf("Error scanning location row: %v", err)
			continue
		}

		var lookupKey string
		if locationType != nil {
			if *locationType == "COUNTRY" && countryIso != nil {
				lookupKey = fmt.Sprintf("%s:%s", *locationType, *countryIso)
			} else {
				lookupKey = fmt.Sprintf("%s:%d", *locationType, lookupID)
			}
		}

		if lookupKey != "" {
			locationLookup[lookupKey] = s.buildLocationMap(id, name, cityID, cityName, stateID, stateName, countryID, countryName, latitude, longitude, address, locationType, countryIso, regions)
			if locationType != nil && *locationType == "STATE" {
				stateKeys = append(stateKeys, lookupKey)
			}
		}
	}

	locationsByEvent := make(map[uint32]map[string]map[string]interface{})

	for eventID, data := range locationDataMap {
		locations := make(map[string]map[string]interface{})

		if data.VenueID != nil {
			lookupKey := fmt.Sprintf("VENUE:%d", *data.VenueID)
			if loc, ok := locationLookup[lookupKey]; ok {
				locations["venue"] = loc
			}
		}

		if data.EditionCity != nil {
			lookupKey := fmt.Sprintf("CITY:%d", *data.EditionCity)
			if loc, ok := locationLookup[lookupKey]; ok {
				locations["city"] = loc
			}
		}

		if data.VenueCity != nil {
			lookupKey := fmt.Sprintf("CITY:%d", *data.VenueCity)
			if loc, ok := locationLookup[lookupKey]; ok {
				locations["venueCity"] = loc
			}
		}

		if data.EditionCityStateID != nil {
			lookupKey := fmt.Sprintf("STATE:%d", *data.EditionCityStateID)
			if loc, ok := locationLookup[lookupKey]; ok {
				locations["state"] = loc
			}
		}

		if data.EditionCountry != nil && *data.EditionCountry != "" {
			lookupKey := fmt.Sprintf("COUNTRY:%s", *data.EditionCountry)
			if loc, ok := locationLookup[lookupKey]; ok {
				locations["country"] = loc
			}
		}

		if len(locations) > 0 {
			locationsByEvent[eventID] = locations
		}
	}

	result := make(map[uint32]map[string]interface{})
	for eventID, locations := range locationsByEvent {
		transformed := s.transformLocationData(locations)
		result[eventID] = transformed
	}

	return result, nil
}

func (s *SharedFunctionService) buildLocationMap(id, name, cityID, cityName, stateID, stateName, countryID, countryName, latitude, longitude, address, locationType, countryIso *string, regions []string) map[string]interface{} {
	location := make(map[string]interface{})

	if id != nil {
		location["id"] = *id
	}
	if name != nil {
		location["name"] = *name
	}
	if locationType != nil {
		location["locationType"] = *locationType
	}
	if latitude != nil && *latitude != "null" && *latitude != "" {
		location["latitude"] = *latitude
	} else {
		location["latitude"] = nil
	}
	if longitude != nil && *longitude != "null" && *longitude != "" {
		location["longitude"] = *longitude
	} else {
		location["longitude"] = nil
	}
	if countryIso != nil && *countryIso != "null" && *countryIso != "" {
		location["countryIso"] = *countryIso
	} else {
		location["countryIso"] = nil
	}

	locType := ""
	if locationType != nil {
		locType = *locationType
	}

	switch locType {
	case "VENUE":
		if address != nil && *address != "null" && *address != "" {
			location["address"] = *address
		}
		if cityID != nil && *cityID != "null" && *cityID != "" {
			location["cityId"] = *cityID
		} else {
			location["cityId"] = nil
		}
		if cityName != nil && *cityName != "null" && *cityName != "" {
			location["cityName"] = *cityName
		} else {
			location["cityName"] = nil
		}
		if stateID != nil && *stateID != "null" && *stateID != "" {
			location["stateId"] = *stateID
		} else {
			location["stateId"] = nil
		}
		if stateName != nil && *stateName != "null" && *stateName != "" {
			location["stateName"] = *stateName
		} else {
			location["stateName"] = nil
		}
		if countryID != nil && *countryID != "null" && *countryID != "" {
			location["countryId"] = *countryID
		} else {
			location["countryId"] = nil
		}
		if countryName != nil && *countryName != "null" && *countryName != "" {
			location["countryName"] = *countryName
		} else {
			location["countryName"] = nil
		}
	case "CITY":
		if stateID != nil && *stateID != "null" && *stateID != "" {
			location["stateId"] = *stateID
		} else {
			location["stateId"] = nil
		}
		if stateName != nil && *stateName != "null" && *stateName != "" {
			location["stateName"] = *stateName
		} else {
			location["stateName"] = nil
		}
		if countryID != nil && *countryID != "null" && *countryID != "" {
			location["countryId"] = *countryID
		} else {
			location["countryId"] = nil
		}
		if countryName != nil && *countryName != "null" && *countryName != "" {
			location["countryName"] = *countryName
		} else {
			location["countryName"] = nil
		}
	case "STATE":
		if countryID != nil && *countryID != "null" && *countryID != "" {
			location["countryId"] = *countryID
		} else {
			location["countryId"] = nil
		}
		if countryName != nil && *countryName != "null" && *countryName != "" {
			location["countryName"] = *countryName
		} else {
			location["countryName"] = nil
		}
	case "COUNTRY":
		if len(regions) > 0 {
			location["region"] = regions
		} else {
			location["region"] = nil
		}
		if countryIso != nil && *countryIso != "null" && *countryIso != "" {
			location["iso"] = *countryIso
		} else {
			location["iso"] = nil
		}
	}

	displayNameParts := []string{}
	if name != nil && *name != "" {
		displayNameParts = append(displayNameParts, *name)
	}
	if cityName, ok := location["cityName"].(string); ok && cityName != "" {
		displayNameParts = append(displayNameParts, cityName)
	}
	if stateName, ok := location["stateName"].(string); ok && stateName != "" {
		displayNameParts = append(displayNameParts, stateName)
	}
	if countryName, ok := location["countryName"].(string); ok && countryName != "" {
		displayNameParts = append(displayNameParts, countryName)
	}
	if len(displayNameParts) > 0 {
		location["displayName"] = strings.Join(displayNameParts, ", ")
	} else {
		location["displayName"] = ""
	}

	return location
}

func (s *SharedFunctionService) transformLocationData(locations map[string]map[string]interface{}) map[string]interface{} {
	if venue, ok := locations["venue"]; ok {
		venueCopy := make(map[string]interface{})
		for k, v := range venue {
			venueCopy[k] = v
		}

		if state, ok := locations["state"]; ok {
			if stateID, ok := state["id"].(string); ok && stateID != "" {
				venueCopy["stateId"] = stateID
			}
		} else if city, ok := locations["city"]; ok {
			if stateID, ok := city["stateId"].(string); ok && stateID != "" {
				venueCopy["stateId"] = stateID
			}
		}

		var cityForEnrichment map[string]interface{}
		if venueCity, ok := locations["venueCity"]; ok {
			cityForEnrichment = venueCity
		} else if city, ok := locations["city"]; ok {
			cityForEnrichment = city
		}

		if cityForEnrichment != nil {
			if cityLat, ok := cityForEnrichment["latitude"].(string); ok && cityLat != "" {
				venueCopy["cityLatitude"] = cityLat
			} else {
				venueCopy["cityLatitude"] = nil
			}
			if cityLon, ok := cityForEnrichment["longitude"].(string); ok && cityLon != "" {
				venueCopy["cityLongitude"] = cityLon
			} else {
				venueCopy["cityLongitude"] = nil
			}
		} else {
			venueCopy["cityLatitude"] = nil
			venueCopy["cityLongitude"] = nil
		}

		if country, ok := locations["country"]; ok {
			if iso, ok := country["iso"].(string); ok && iso != "" {
				venueCopy["countryIso"] = iso
			}
			if region, ok := country["region"].([]string); ok && len(region) > 0 {
				venueCopy["region"] = region
			} else {
				venueCopy["region"] = nil
			}
		}
		return venueCopy
	}

	if city, ok := locations["city"]; ok {
		cityCopy := make(map[string]interface{})
		for k, v := range city {
			cityCopy[k] = v
		}

		if country, ok := locations["country"]; ok {
			if iso, ok := country["iso"].(string); ok && iso != "" {
				cityCopy["countryIso"] = iso
			}
			if region, ok := country["region"].([]string); ok && len(region) > 0 {
				cityCopy["region"] = region
			} else {
				cityCopy["region"] = nil
			}
		}
		return cityCopy
	}

	if state, ok := locations["state"]; ok {
		stateCopy := make(map[string]interface{})
		for k, v := range state {
			stateCopy[k] = v
		}

		if country, ok := locations["country"]; ok {
			if iso, ok := country["iso"].(string); ok && iso != "" {
				stateCopy["countryIso"] = iso
			}
			if region, ok := country["region"].([]string); ok && len(region) > 0 {
				stateCopy["region"] = region
			} else {
				stateCopy["region"] = nil
			}
		}
		return stateCopy
	}

	if country, ok := locations["country"]; ok {
		countryCopy := make(map[string]interface{})
		for k, v := range country {
			countryCopy[k] = v
		}
		return countryCopy
	}

	for _, location := range locations {
		locationCopy := make(map[string]interface{})
		for k, v := range location {
			locationCopy[k] = v
		}
		return locationCopy
	}

	return nil
}

func (s *SharedFunctionService) GetEventLocations(eventIds []uint32, filterFields models.FilterDataDto) (map[string]map[string]interface{}, error) {
	locationDataMap, err := s.FetchEventLocationData(eventIds, filterFields)
	if err != nil {
		return nil, err
	}

	locationsMap, err := s.FetchLocations(locationDataMap)
	if err != nil {
		return nil, err
	}

	result := make(map[string]map[string]interface{})
	for eventID, location := range locationsMap {
		eventIDStr := fmt.Sprintf("%d", eventID)
		result[eventIDStr] = location
	}

	return result, nil
}

func (s *SharedFunctionService) GetEventCountByStatus(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
) (map[string]interface{}, error) {
	today := time.Now().Format("2006-01-02")
	createdAt := filterFields.CreatedAt
	if createdAt == "" {
		createdAt = today
	}

	forecasted := filterFields.Forecasted
	endDateField := s.getEndDateFieldForPastActive(forecasted, filterFields, "ee")

	getNew := filterFields.ParsedGetNew
	searchByEntity := strings.ToLower(strings.TrimSpace(filterFields.SearchByEntity))
	isEventEntity := searchByEntity == "event"

	// Validate: searchByEntity = 'event' requires sourceEventIds
	if isEventEntity && len(filterFields.ParsedSourceEventIds) == 0 {
		result := make(map[string]interface{})
		result["total_ids"] = ""
		result["past"] = 0
		result["active"] = 0
		result["past_ids"] = ""
		result["active_ids"] = ""
		if getNew == nil || *getNew {
			result["new"] = 0
			result["new_ids"] = ""
		}
		return result, nil
	}

	// Check for special case: sourceEventIds[0] == -1 (only when searchByEntity = "event")
	if isEventEntity && len(filterFields.ParsedSourceEventIds) > 0 {
		// check for -1
		firstId := filterFields.ParsedSourceEventIds[0]
		if firstId == "'-1'" || firstId == "-1" {
			result := make(map[string]interface{})
			result["total_ids"] = ""
			result["past"] = 0
			result["active"] = 0
			result["past_ids"] = ""
			result["active_ids"] = ""
			if getNew == nil || *getNew {
				result["new"] = 0
				result["new_ids"] = ""
			}
			return result, nil
		}
	}

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "ee.")
	}

	selectClauses := []string{}

	if isEventEntity {
		// For event entity, return both counts and event IDs
		selectClauses = append(selectClauses,
			"arrayStringConcat(groupArray(DISTINCT toString(ee.event_uuid) || '#' || toString(ee.event_id)), ',') AS total_ids",
		)
		if getNew == nil || *getNew {
			selectClauses = append(selectClauses,
				// Count
				fmt.Sprintf("uniqIf(ee.event_id, ee.event_created >= '%s') AS new", createdAt),
				// IDs
				fmt.Sprintf("arrayStringConcat(groupArray(DISTINCT CASE WHEN ee.event_created >= '%s' THEN toString(ee.event_uuid) || '#' || toString(ee.event_id) END), ',') AS new_ids", createdAt),
			)
		}
		selectClauses = append(selectClauses,
			// Counts
			fmt.Sprintf("uniqIf(ee.event_id, %s < '%s') AS past", endDateField, today),
			fmt.Sprintf("uniqIf(ee.event_id, %s >= '%s') AS active", endDateField, today),
			// IDs
			fmt.Sprintf("arrayStringConcat(groupArray(DISTINCT CASE WHEN %s < '%s' THEN toString(ee.event_uuid) || '#' || toString(ee.event_id) END), ',') AS past_ids", endDateField, today),
			fmt.Sprintf("arrayStringConcat(groupArray(DISTINCT CASE WHEN %s >= '%s' THEN toString(ee.event_uuid) || '#' || toString(ee.event_id) END), ',') AS active_ids", endDateField, today),
		)
	} else {
		// Normal behavior: return only counts (no IDs)
		if getNew == nil || *getNew {
			selectClauses = append(selectClauses,
				fmt.Sprintf("uniqIf(ee.event_id, ee.event_created >= '%s') AS new", createdAt),
			)
		}

		if getNew == nil || !*getNew {
			selectClauses = append(selectClauses,
				fmt.Sprintf("uniqIf(ee.event_id, %s < '%s') AS past", endDateField, today),
				fmt.Sprintf("uniqIf(ee.event_id, %s >= '%s') AS active", endDateField, today),
			)
		}

		if len(selectClauses) == 0 {
			selectClauses = []string{
				fmt.Sprintf("uniqIf(ee.event_id, %s < '%s') AS past", endDateField, today),
				fmt.Sprintf("uniqIf(ee.event_id, %s >= '%s') AS active", endDateField, today),
				fmt.Sprintf("uniqIf(ee.event_id, ee.event_created >= '%s') AS new", createdAt),
			}
		}
	}

	selectStr := strings.Join(selectClauses, ", ")

	preFilterSelectFields := []string{"ee.event_id"}
	switch forecasted {
	case "only":
		preFilterSelectFields = append(preFilterSelectFields, endDateField)
	case "included":
		preFilterSelectFields = append(preFilterSelectFields, "ee.end_date", "ee.futureExpexctedEndDate")
	default:
		preFilterSelectFields = append(preFilterSelectFields, "ee.end_date")
	}
	if getNew == nil || *getNew || isEventEntity {
		preFilterSelectFields = append(preFilterSelectFields, "ee.event_created")
	}
	if isEventEntity {
		preFilterSelectFields = append(preFilterSelectFields, "ee.event_uuid")
	}
	preFilterSelectStr := strings.Join(preFilterSelectFields, ", ")

	// Build WHERE conditions similar to buildListDataCountQuery
	whereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "ee"),
	}

	if queryResult.WhereClause != "" {
		whereConditions = append(whereConditions, queryResult.WhereClause)
	}

	if queryResult.SearchClause != "" {
		whereConditions = append(whereConditions, queryResult.SearchClause)
	}

	// Add join conditions - this was missing!
	if len(cteAndJoinResult.JoinConditions) > 0 {
		whereConditions = append(whereConditions, cteAndJoinResult.JoinConditions...)
	}

	whereClause := strings.Join(whereConditions, "\n\t\t\tAND ")

	query := fmt.Sprintf(`
		WITH %spreFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS ee%s
			WHERE %s
		)
		SELECT
			%s
		FROM preFilterEvent ee
	`,
		cteClausesStr,
		preFilterSelectStr,
		func() string {
			if joinClausesStr != "" {
				return "\n\t\t" + joinClausesStr
			}
			return ""
		}(),
		whereClause,
		selectStr,
	)

	log.Printf("Event count by status query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	// OLD CODE - Commented out: Result processing with conditional logic
	// result := make(map[string]interface{})
	// if rows.Next() {
	// 	columns := rows.Columns()
	//
	// 	hasIdColumns := false
	// 	for _, col := range columns {
	// 		if strings.HasSuffix(col, "_ids") {
	// 			hasIdColumns = true
	// 			break
	// 		}
	// 	}
	//
	// 	if hasIdColumns {
	// 		values := make([]*string, len(columns))
	// 		for i := range columns {
	// 			values[i] = new(string)
	// 		}
	//
	// 		scanArgs := make([]interface{}, len(columns))
	// 		for i := range columns {
	// 			scanArgs[i] = values[i]
	// 		}
	//
	// 		if err := rows.Scan(scanArgs...); err != nil {
	// 			log.Printf("Error scanning result: %v", err)
	// 			return nil, err
	// 		}
	//
	// 		for i, col := range columns {
	// 			if values[i] != nil && *values[i] != "" {
	// 				result[col] = *values[i]
	// 			} else {
	// 				result[col] = ""
	// 			}
	// 		}
	// 	} else {
	// 		values := make([]*uint64, len(columns))
	// 		for i := range columns {
	// 			values[i] = new(uint64)
	// 		}
	//
	// 		scanArgs := make([]interface{}, len(columns))
	// 		for i := range columns {
	// 			scanArgs[i] = values[i]
	// 		}
	//
	// 		if err := rows.Scan(scanArgs...); err != nil {
	// 			log.Printf("Error scanning result: %v", err)
	// 			return nil, err
	// 		}
	//
	// 		for i, col := range columns {
	// 			if values[i] != nil {
	// 				result[col] = int(*values[i])
	// 			} else {
	// 				result[col] = 0
	// 			}
	// 		}
	// 	}
	// }

	// Initialize result map with default values only for fields that were queried
	result := make(map[string]interface{})

	if isEventEntity {
		// For event entity, set defaults for both counts and event IDs
		result["total_ids"] = ""
		result["past"] = 0
		result["active"] = 0
		result["past_ids"] = ""
		result["active_ids"] = ""
		if getNew == nil || *getNew {
			result["new"] = 0
			result["new_ids"] = ""
		}
	} else {
		// Normal behavior: set defaults for counts only (no IDs)
		if getNew == nil || *getNew {
			result["new"] = 0
		}
		if getNew == nil || !*getNew {
			result["past"] = 0
			result["active"] = 0
		}
	}

	if rows.Next() {
		columns := rows.Columns()

		scanArgs := make([]interface{}, len(columns))
		for i, col := range columns {
			if strings.HasSuffix(col, "_ids") {
				scanArgs[i] = new(string)
			} else {
				scanArgs[i] = new(uint64)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			log.Printf("Error scanning result: %v", err)
			return nil, err
		}

		for i, col := range columns {
			if strings.HasSuffix(col, "_ids") {
				if val, ok := scanArgs[i].(*string); ok && val != nil && *val != "" {
					result[col] = *val
				} else {
					result[col] = ""
				}
			} else {
				if val, ok := scanArgs[i].(*uint64); ok && val != nil {
					result[col] = int(*val)
				} else {
					result[col] = 0
				}
			}
		}
	}

	return result, nil
}

func (s *SharedFunctionService) GetEventCountByEventTypeGroup(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
) (interface{}, error) {
	today := time.Now().Format("2006-01-02")
	createdAt := filterFields.CreatedAt
	if createdAt == "" {
		createdAt = today
	}

	getNew := filterFields.ParsedGetNew
	eventGroupCount := filterFields.ParsedEventGroupCount
	searchByEntity := strings.ToLower(strings.TrimSpace(filterFields.SearchByEntity))
	isEventEntity := searchByEntity == "event"
	eventTypeGroup := filterFields.ParsedEventTypeGroup

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = cteAndJoinResult.JoinClausesStr
	}

	selectClauses := []string{}

	if createdAt != "" && (getNew == nil || *getNew) {
		if isEventEntity {
			selectClauses = append(selectClauses, fmt.Sprintf(
				"arrayStringConcat(groupArray(DISTINCT CASE WHEN e.event_created >= '%s' THEN toString(e.event_uuid) || '#' || toString(e.event_id) END), ',') AS new_event_ids",
				createdAt,
			))
		} else {
			selectClauses = append(selectClauses, fmt.Sprintf(
				"uniqIf(e.event_id, e.event_created >= '%s') AS new",
				createdAt,
			))
		}
	}

	if isEventEntity {
		selectClauses = append(selectClauses,
			"arrayStringConcat(groupArray(DISTINCT toString(e.event_uuid) || '#' || toString(e.event_id)), ',') AS total_event_ids",
		)
	}

	if (getNew == nil || !*getNew) && (eventGroupCount == nil || !*eventGroupCount) {
		if !isEventEntity {
			selectClauses = append(selectClauses, "uniq(e.event_id) AS count")
		}
	}

	selectStr := ""
	if len(selectClauses) > 0 {
		selectStr = ", " + strings.Join(selectClauses, ", ")
	}

	baseWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "ee"),
	}
	if queryResult.WhereClause != "" {
		baseWhereConditions = append(baseWhereConditions, queryResult.WhereClause)
	}
	if queryResult.SearchClause != "" {
		baseWhereConditions = append(baseWhereConditions, queryResult.SearchClause)
	}
	if joinConditionsStr != "" {
		baseWhereConditions = append(baseWhereConditions, strings.TrimPrefix(joinConditionsStr, "AND "))
	}
	whereClause := strings.Join(baseWhereConditions, " AND ")

	// !eventTypeGroup && !eventGroupCount - Group by all groups using arrayJoin
	if eventTypeGroup == nil && (eventGroupCount == nil || !*eventGroupCount) && searchByEntity != "eventestimatecount" && searchByEntity != "economicimpactbreakdowncount" {
		// preFilterEvent - only select required columns
		preFilterSelectFields := []string{"ee.event_id", "ee.event_uuid"}
		if createdAt != "" && (getNew == nil || *getNew) {
			preFilterSelectFields = append(preFilterSelectFields, "ee.event_created")
		}
		preFilterSelectStr := strings.Join(preFilterSelectFields, ", ")

		query := fmt.Sprintf(`
			WITH %spreFilterEvent AS (
				SELECT
					%s
				FROM testing_db.allevent_ch AS ee
				%s
				WHERE %s
			),
			grouped_counts AS (
				SELECT
					group_name%s
				FROM preFilterEvent AS ee
				INNER JOIN testing_db.event_type_ch AS et ON ee.event_id = et.event_id and et.published = 1
				ARRAY JOIN et.groups AS group_name
				WHERE group_name IN ('business', 'social', 'unattended')
				GROUP BY group_name
			)
			SELECT * FROM grouped_counts
		`,
			cteClausesStr,
			preFilterSelectStr,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			whereClause,
			strings.ReplaceAll(selectStr, "e.", "ee."),
		)

		log.Printf("Event count by event type group query (scenario 1): %s", query)

		rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
		if err != nil {
			log.Printf("ClickHouse query error: %v", err)
			return nil, err
		}
		defer rows.Close()

		result := make(map[string]interface{})
		for rows.Next() {
			columns := rows.Columns()
			var groupName string
			scanArgs := []interface{}{&groupName}

			if isEventEntity {
				for i := 1; i < len(columns); i++ {
					var val string
					scanArgs = append(scanArgs, &val)
				}
			} else {
				for i := 1; i < len(columns); i++ {
					var val uint64
					scanArgs = append(scanArgs, &val)
				}
			}

			if err := rows.Scan(scanArgs...); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			groupData := make(map[string]interface{})
			colIdx := 1
			for _, col := range columns[1:] {
				if isEventEntity {
					if val, ok := scanArgs[colIdx].(*string); ok && val != nil && *val != "" {
						groupData[col] = *val
					} else {
						groupData[col] = ""
					}
				} else {
					if val, ok := scanArgs[colIdx].(*uint64); ok && val != nil {
						groupData[col] = int(*val)
					} else {
						groupData[col] = 0
					}
				}
				colIdx++
			}
			result[groupName] = groupData
		}

		allGroups := []string{"business", "social", "unattended"}
		for _, group := range allGroups {
			if _, exists := result[group]; !exists {
				if isEventEntity {
					result[group] = map[string]interface{}{}
				} else {
					result[group] = map[string]interface{}{"count": 0}
				}
			}
		}

		return result, nil
	}

	// eventGroupCount = true - Special filtering with published status
	if eventGroupCount != nil && *eventGroupCount && searchByEntity != "eventestimatecount" && searchByEntity != "economicimpactbreakdowncount" {
		publishedValues := filterFields.ParsedPublished
		if len(publishedValues) == 0 {
			publishedValues = []string{"1", "2"}
		}

		publishedMap := make(map[string]bool)
		for _, pub := range publishedValues {
			publishedMap[pub] = true
		}

		eventGroupCountConditions := []string{}

		if publishedMap["4"] {
			eventGroupCountConditions = append(eventGroupCountConditions, "(ee.published = '4' AND has(et.groups, 'unattended'))")
		}

		businessSocialPublished := []string{}
		if publishedMap["1"] {
			businessSocialPublished = append(businessSocialPublished, "'1'")
		}
		if publishedMap["2"] {
			businessSocialPublished = append(businessSocialPublished, "'2'")
		}

		if len(businessSocialPublished) > 0 {
			publishedCondition := ""
			if len(businessSocialPublished) == 1 {
				publishedCondition = fmt.Sprintf("ee.published = %s", businessSocialPublished[0])
			} else {
				publishedCondition = fmt.Sprintf("ee.published IN (%s)", strings.Join(businessSocialPublished, ", "))
			}

			eventGroupCountConditions = append(eventGroupCountConditions, fmt.Sprintf("(%s AND has(et.groups, 'business') and et.event_audience = ee.editions_audiance_type)", publishedCondition))
			eventGroupCountConditions = append(eventGroupCountConditions, fmt.Sprintf("(%s AND has(et.groups, 'social') and et.event_audience = ee.editions_audiance_type)", publishedCondition))
		}

		if len(eventGroupCountConditions) == 0 {
			eventGroupCountConditions = []string{
				"(ee.published = '4' AND has(et.groups, 'unattended'))",
				"(ee.published = '1' AND has(et.groups, 'business') and et.event_audience = ee.editions_audiance_type)",
				"(ee.published = '1' AND has(et.groups, 'social') and et.event_audience = ee.editions_audiance_type)",
			}
		}

		eventGroupCountWhere := fmt.Sprintf("(%s)", strings.Join(eventGroupCountConditions, " OR "))

		groupedSelectClauses := []string{
			"CASE WHEN has(et.groups, 'unattended') THEN 'unattended' WHEN has(et.groups, 'business') THEN 'business' WHEN has(et.groups, 'social') THEN 'social' END AS group_name",
			"uniq(ee.event_id) AS event_count",
		}
		if len(selectClauses) > 0 {
			updatedSelectClauses := make([]string, len(selectClauses))
			for i, clause := range selectClauses {
				updatedSelectClauses[i] = strings.ReplaceAll(clause, "e.", "ee.")
			}
			groupedSelectClauses = append(groupedSelectClauses, updatedSelectClauses...)
		}
		groupedSelectStr := strings.Join(groupedSelectClauses, ", ")

		finalSelectClauses := []string{
			"g.group_name",
			"COALESCE(gr.event_count, 0) AS count",
		}
		if isEventEntity {
			finalSelectClauses = append(finalSelectClauses, "COALESCE(gr.total_event_ids, '') AS total_event_ids")
			if getNew != nil && *getNew {
				finalSelectClauses = append(finalSelectClauses, "COALESCE(gr.new_event_ids, '') AS new_event_ids")
			}
		} else {
			if createdAt != "" && (getNew == nil || *getNew) {
				finalSelectClauses = append(finalSelectClauses, "COALESCE(gr.new, 0) AS new")
			}
		}
		finalSelectStr := strings.Join(finalSelectClauses, ", ")

		// preFilterEvent - only select required columns
		preFilterSelectFields := []string{"ee.event_id", "ee.published", "ee.editions_audiance_type"}
		if isEventEntity {
			preFilterSelectFields = append(preFilterSelectFields, "ee.event_uuid")
		}
		if createdAt != "" && (getNew == nil || *getNew) {
			preFilterSelectFields = append(preFilterSelectFields, "ee.event_created")
		}
		preFilterSelectStr := strings.Join(preFilterSelectFields, ", ")

		query := fmt.Sprintf(`
			WITH %spreFilterEvent AS (
				SELECT
					%s
				FROM testing_db.allevent_ch AS ee
				%s
				WHERE %s
			),
			grouped AS (
				SELECT
					%s
				FROM preFilterEvent AS ee
				INNER JOIN testing_db.event_type_ch et ON ee.event_id = et.event_id and et.published = 1
				WHERE %s
				GROUP BY group_name
			)
			SELECT 
				%s
			FROM (
				SELECT 'unattended' AS group_name
				UNION ALL SELECT 'business'
				UNION ALL SELECT 'social'
			) g
			LEFT JOIN grouped gr ON g.group_name = gr.group_name
		`,
			cteClausesStr,
			preFilterSelectStr,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			whereClause,
			groupedSelectStr,
			eventGroupCountWhere,
			finalSelectStr,
		)

		log.Printf("Event count by event type group query (scenario 2): %s", query)

		rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
		if err != nil {
			log.Printf("ClickHouse query error: %v", err)
			return nil, err
		}
		defer rows.Close()

		result := make(map[string]interface{})
		for rows.Next() {
			var groupName string
			var count uint64
			scanArgs := []interface{}{&groupName, &count}

			if isEventEntity {
				var totalEventIds string
				scanArgs = append(scanArgs, &totalEventIds)
				if getNew != nil && *getNew {
					var newEventIds string
					scanArgs = append(scanArgs, &newEventIds)
				}
			} else {
				if createdAt != "" && (getNew == nil || *getNew) {
					var newCount uint64
					scanArgs = append(scanArgs, &newCount)
				}
			}

			if err := rows.Scan(scanArgs...); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			groupData := make(map[string]interface{})
			groupData["count"] = int(count)
			argIdx := 2
			if isEventEntity {
				if totalEventIds, ok := scanArgs[argIdx].(*string); ok {
					groupData["total_event_ids"] = *totalEventIds
				} else {
					groupData["total_event_ids"] = ""
				}
				argIdx++
				if getNew != nil && *getNew {
					if newEventIds, ok := scanArgs[argIdx].(*string); ok {
						groupData["new_event_ids"] = *newEventIds
					} else {
						groupData["new_event_ids"] = ""
					}
				}
			} else {
				if createdAt != "" && (getNew == nil || *getNew) {
					if newCount, ok := scanArgs[argIdx].(*uint64); ok {
						groupData["new"] = int(*newCount)
					} else {
						groupData["new"] = 0
					}
				}
			}
			result[groupName] = groupData
		}

		return result, nil
	}

	// eventEstimateCount - Group by business/social with past/upcoming counts and visitor estimates
	if searchByEntity == "eventestimatecount" {
		query := fmt.Sprintf(`
			WITH %spreFilterEvent AS (
				SELECT
					ee.event_id,
					ee.estimatedVisitorsMean,
					ee.end_date,
					ee.published
				FROM testing_db.allevent_ch AS ee
				%s
				WHERE %s
			),
			classified AS (
				SELECT
					ee.event_id,
					ee.estimatedVisitorsMean,
					CASE
						WHEN has(et.groups, 'business') THEN 'business'
						WHEN has(et.groups, 'social') THEN 'social'
					END AS group_name,
					if(ee.end_date < today(), 'past', 'upcoming') AS time_state
				FROM preFilterEvent AS ee
				INNER JOIN testing_db.event_type_ch AS et ON ee.event_id = et.event_id and et.published = 1
				WHERE has(et.groups, 'business') OR has(et.groups, 'social')
			),
			grouped AS (
				SELECT
					group_name,
					countIf(time_state = 'past') AS past_count,
					countIf(time_state = 'upcoming') AS upcoming_count,
					count(*) AS total_count,
					toFloat64(sum(estimatedVisitorsMean)) AS total_visitors
				FROM classified
				WHERE group_name != ''
				GROUP BY group_name
			)
			SELECT
				g.group_name,
				COALESCE(gr.past_count, 0) AS past_count,
				COALESCE(gr.upcoming_count, 0) AS upcoming_count,
				COALESCE(gr.total_count, 0) AS total_count,
				COALESCE(gr.total_visitors, 0) AS total_visitor_sum
			FROM (
				SELECT 'business' AS group_name
				UNION ALL SELECT 'social'
			) g
			LEFT JOIN grouped gr ON g.group_name = gr.group_name
			-- ORDER BY g.group_name
		`,
			cteClausesStr,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			whereClause,
		)

		log.Printf("Event count by event type group query (eventEstimateCount): %s", query)

		rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
		if err != nil {
			log.Printf("ClickHouse query error: %v", err)
			return nil, err
		}
		defer rows.Close()

		var response []map[string]interface{}
		for rows.Next() {
			var groupName string
			var pastCount, upcomingCount, totalCount uint64
			var totalVisitorSum float64

			if err := rows.Scan(&groupName, &pastCount, &upcomingCount, &totalCount, &totalVisitorSum); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			response = append(response, map[string]interface{}{
				"groupName":       groupName,
				"pastCount":       int(pastCount),
				"upcomingCount":   int(upcomingCount),
				"totalCount":      int(totalCount),
				"totalVisitorSum": totalVisitorSum,
			})
		}

		return response, nil
	}

	// economicImpactBreakdownCount - Sum economic impact breakdown by category
	if searchByEntity == "economicimpactbreakdowncount" {
		query := fmt.Sprintf(`
			WITH %spreFilterEvent AS (
				SELECT
					ee.event_id,
					ee.event_economic_breakdown
				FROM testing_db.allevent_ch AS ee
				WHERE %s
			),
			extracted AS (
				SELECT
					toJSONString(e.event_economic_breakdown) AS breakdown_json
				FROM preFilterEvent e
				%s
			)
			SELECT
				sum(toFloat64OrZero(JSONExtractString(breakdown_json, 'Accommodation'))) AS accommodation,
				sum(toFloat64OrZero(JSONExtractString(breakdown_json, 'Flights'))) AS inbound_transport,
				sum(toFloat64OrZero(JSONExtractString(breakdown_json, 'Food & Beverages'))) AS food,
				sum(toFloat64OrZero(JSONExtractString(breakdown_json, 'Transportation'))) AS local_transport,
				sum(toFloat64OrZero(JSONExtractString(breakdown_json, 'Utilities'))) AS utilities
			FROM extracted
		`,
			cteClausesStr,
			whereClause,
			func() string {
				if joinConditionsStr != "" {
					return "WHERE " + strings.TrimPrefix(joinConditionsStr, "AND ")
				}
				return ""
			}(),
		)

		log.Printf("Event count by event type group query (economicImpactBreakdownCount): %s", query)

		rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
		if err != nil {
			log.Printf("ClickHouse query error: %v", err)
			return nil, err
		}
		defer rows.Close()

		result := make(map[string]interface{})
		if rows.Next() {
			var accommodation, inboundTransport, food, localTransport, utilities float64

			if err := rows.Scan(&accommodation, &inboundTransport, &food, &localTransport, &utilities); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			result["accommodation"] = accommodation
			result["inbound_transport"] = inboundTransport
			result["food"] = food
			result["local_transport"] = localTransport
			result["utilities"] = utilities
		} else {
			result["accommodation"] = 0.0
			result["inbound_transport"] = 0.0
			result["food"] = 0.0
			result["local_transport"] = 0.0
			result["utilities"] = 0.0
		}

		return result, nil
	}

	// eventTypeGroup is provided - Filter to specific group
	groupValue := string(*eventTypeGroup)
	// preFilterEvent - only select required columns
	preFilterSelectFields := []string{"ee.event_id"}
	if isEventEntity {
		preFilterSelectFields = append(preFilterSelectFields, "ee.event_uuid")
	}
	if createdAt != "" && (getNew == nil || *getNew) {
		preFilterSelectFields = append(preFilterSelectFields, "ee.event_created")
	}
	preFilterSelectStr := strings.Join(preFilterSelectFields, ", ")

	finalSelectClauses := []string{"uniq(e.event_id) AS count"}
	if selectStr != "" {
		finalSelectClauses = append(finalSelectClauses, strings.TrimPrefix(selectStr, ", "))
	}
	finalSelectStr := strings.Join(finalSelectClauses, ", ")

	query := fmt.Sprintf(`
		WITH %spreFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS ee
			%s
			WHERE %s
		)
		SELECT %s
		FROM preFilterEvent e
		INNER JOIN testing_db.event_type_ch et ON e.event_id = et.event_id and et.published = 1
		WHERE has(et.groups, '%s')
		`,
		cteClausesStr,
		preFilterSelectStr,
		func() string {
			if joinClausesStr != "" {
				return "\t\t" + joinClausesStr
			}
			return ""
		}(),
		whereClause,
		finalSelectStr,
		groupValue,
	)

	log.Printf("Event count by event type group query (scenario 3): %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	if rows.Next() {
		columns := rows.Columns()

		if isEventEntity {
			values := make([]*string, len(columns))
			for i := range columns {
				values[i] = new(string)
			}
			scanArgs := make([]interface{}, len(columns))
			for i := range columns {
				scanArgs[i] = values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			for i, col := range columns {
				if values[i] != nil && *values[i] != "" {
					result[col] = *values[i]
				} else {
					result[col] = ""
				}
			}
		} else {
			values := make([]*uint64, len(columns))
			for i := range columns {
				values[i] = new(uint64)
			}
			scanArgs := make([]interface{}, len(columns))
			for i := range columns {
				scanArgs[i] = values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				log.Printf("Error scanning result: %v", err)
				return nil, err
			}

			for i, col := range columns {
				if values[i] != nil {
					result[col] = int(*values[i])
				} else {
					result[col] = 0
				}
			}
		}
	}

	return result, nil
}

func (s *SharedFunctionService) GetEventCountByLocation(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
	groupBy string,
) (interface{}, error) {
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "e.")
	}

	baseWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "e"),
	}
	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, whereClauseFixed)
	}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	whereClause := strings.Join(baseWhereConditions, " AND ")
	whereClause = strings.ReplaceAll(whereClause, "ee.", "e.")

	var locationJoinCondition string
	var locationWhereCondition string
	var selectFields string

	switch groupBy {
	case "city":
		selectFields = `e.event_id,
				e.edition_id,
				any(e.venue_id)      AS venue_id,
				any(e.edition_city)  AS venue_city`
		locationJoinCondition = `LEFT JOIN testing_db.location_ch l ON e.venue_id = l.id AND l.location_type IN ('CITY', 'VENUE', 'COUNTRY')
        LEFT JOIN testing_db.location_ch loc ON (e.venue_city = loc.id AND loc.location_type = 'CITY')`
		locationWhereCondition = "e.venue_city IS NOT NULL AND e.venue_city != 0 AND loc.id_uuid IS NOT NULL AND loc.id IS NOT NULL"
	case "state":
		selectFields = `e.event_id,
				e.edition_id,
				any(e.venue_id)      AS venue_id,
				any(e.edition_city_state_id) AS edition_city_state_id`
		locationJoinCondition = `LEFT JOIN testing_db.location_ch l ON e.venue_id = l.id AND l.location_type IN ('CITY', 'VENUE', 'COUNTRY')
        LEFT JOIN testing_db.location_ch loc ON e.edition_city_state_id = loc.id AND loc.location_type = 'STATE'`
		locationWhereCondition = "e.edition_city_state_id IS NOT NULL AND e.edition_city_state_id != 0 AND loc.id_uuid IS NOT NULL AND loc.id IS NOT NULL"
	case "country":
		selectFields = `e.event_id,
				e.edition_id,
				any(e.venue_id)      AS venue_id,
				any(e.edition_country) AS edition_country`
		locationJoinCondition = `LEFT JOIN testing_db.location_ch l ON e.venue_id = l.id AND l.location_type IN ('CITY', 'VENUE', 'COUNTRY')
        LEFT JOIN testing_db.location_ch loc ON e.edition_country = loc.iso AND loc.location_type = 'COUNTRY'`
		locationWhereCondition = "e.edition_country IS NOT NULL AND loc.id_uuid IS NOT NULL AND loc.id IS NOT NULL"
	default:
		return nil, fmt.Errorf("unsupported location groupBy: %s", groupBy)
	}

	query := fmt.Sprintf(`
		WITH %spreFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
			GROUP BY
				e.event_id,
				e.edition_id
		)
		SELECT
			COUNT(DISTINCT e.event_id) AS count,
			toUUIDOrNull(toString(loc.id_uuid)) AS id,
			loc.name AS name,
			loc.latitude AS latitude,
			loc.longitude AS longitude,
			loc.iso AS code
		FROM preFilterEvent e
		%s
		WHERE %s
		GROUP BY loc.id_uuid, loc.name, loc.latitude, loc.longitude, loc.iso
		ORDER BY count DESC
		`,
		cteClausesStr,
		selectFields,
		func() string {
			if joinClausesStr != "" {
				return "\t\t" + joinClausesStr
			}
			return ""
		}(),
		whereClause,
		locationJoinCondition,
		locationWhereCondition,
	)

	log.Printf("Event count by location query (%s): %s", groupBy, query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var count uint64
		var idUUID uuid.UUID
		var name, code *string
		var latitude, longitude *float64

		if err := rows.Scan(&count, &idUUID, &name, &latitude, &longitude, &code); err != nil {
			log.Printf("Error scanning result: %v", err)
			continue
		}

		item := map[string]interface{}{
			"count": int(count),
		}

		if idUUID != uuid.Nil {
			item["id"] = idUUID.String()
		} else {
			item["id"] = nil
		}

		if name != nil {
			item["name"] = *name
		} else {
			item["name"] = nil
		}

		if latitude != nil {
			item["latitude"] = *latitude
		} else {
			item["latitude"] = nil
		}

		if longitude != nil {
			item["longitude"] = *longitude
		} else {
			item["longitude"] = nil
		}

		if code != nil {
			item["code"] = *code
		} else {
			item["code"] = nil
		}

		result = append(result, item)
	}

	if result == nil {
		result = []map[string]interface{}{}
	}
	return result, nil
}

func (s *SharedFunctionService) GetEventCountByDate(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
	groupBy string,
) (interface{}, error) {
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "e.")
	}

	baseWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "e"),
	}
	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, whereClauseFixed)
	}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	whereClause := strings.Join(baseWhereConditions, " AND ")
	whereClause = strings.ReplaceAll(whereClause, "ee.", "e.")

	forecasted := filterFields.Forecasted
	startDateField := s.getDateFieldName(forecasted, "start", "e")
	endDateField := s.getDateFieldName(forecasted, "end", "e")

	var dateGroupByExpr string
	switch groupBy {
	case "day":
		dateGroupByExpr = fmt.Sprintf("formatDateTime(arrayJoin(arrayMap(x -> addDays(toDate(%s), x), range(0, dateDiff('day', toDate(%s), toDate(%s)) + 1))), '%%Y-%%m-%%d')", startDateField, startDateField, endDateField)
	case "month":
		dateGroupByExpr = fmt.Sprintf("formatDateTime(arrayJoin(arrayMap(x -> addDays(toDate(%s), x), range(0, dateDiff('day', toDate(%s), toDate(%s)) + 1))), '%%Y-%%m')", startDateField, startDateField, endDateField)
	case "year":
		dateGroupByExpr = fmt.Sprintf("formatDateTime(arrayJoin(arrayMap(x -> addDays(toDate(%s), x), range(0, dateDiff('day', toDate(%s), toDate(%s)) + 1))), '%%Y')", startDateField, startDateField, endDateField)
	default:
		return nil, fmt.Errorf("unsupported date groupBy: %s", groupBy)
	}

	preFilterSelect := "e.event_id"
	preFilterSelect = s.buildPreFilterSelectDates(preFilterSelect, forecasted)

	query := fmt.Sprintf(`
		WITH %spreFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		)
		SELECT
			%s AS date_key,
			uniq(e.event_id) AS count
		FROM preFilterEvent e
		GROUP BY date_key
		ORDER BY date_key ASC
	`,
		cteClausesStr,
		preFilterSelect,
		func() string {
			if joinClausesStr != "" {
				return "\t\t" + joinClausesStr
			}
			return ""
		}(),
		whereClause,
		dateGroupByExpr,
	)

	log.Printf("Event count by %s query: %s", groupBy, query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	var dayCount []map[string]interface{}
	var monthCount []map[string]interface{}
	var yearCount []map[string]interface{}

	for rows.Next() {
		var dateKey string
		var count uint64

		if err := rows.Scan(&dateKey, &count); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		item := map[string]interface{}{
			"date":  dateKey,
			"count": int(count),
		}

		dayCount = append(dayCount, item)

		if groupBy == "month" || groupBy == "year" {
			switch groupBy {
			case "month":
				monthCount = append(monthCount, item)
			case "year":
				yearCount = append(yearCount, item)
			}
		}
	}

	result["dayCount"] = dayCount
	if groupBy == "month" || groupBy == "year" {
		result["monthCount"] = monthCount
	}
	if groupBy == "year" {
		result["yearCount"] = yearCount
	}

	return result, nil
}

func (s *SharedFunctionService) GetEventCountByDay(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
	startDate string,
	endDate string,
	usecase string,
	groupBy []models.CountGroup,
) (interface{}, error) {
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "e.")
	}

	startDateParsed, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate format: %w", err)
	}
	endDateParsed, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid endDate format: %w", err)
	}
	daysDiff := int(endDateParsed.Sub(startDateParsed).Hours()/24) + 1

	forecasted := filterFields.Forecasted
	preFilterWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "e"),
	}
	dateCondition := s.buildTrendsDateCondition(forecasted, "e", "preFilterRange", startDate, endDate)
	if dateCondition != "" {
		preFilterWhereConditions = append(preFilterWhereConditions, dateCondition)
	}
	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		preFilterWhereConditions = append(preFilterWhereConditions, whereClauseFixed)
	}
	// Add join conditions to preFilterWhereConditions
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		preFilterWhereConditions = append(preFilterWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	preFilterWhereClause := strings.Join(preFilterWhereConditions, " AND ")
	preFilterWhereClause = strings.ReplaceAll(preFilterWhereClause, "ee.", "e.")

	filterWhereConditions := []string{}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	filterWhereClause := ""
	if len(filterWhereConditions) > 0 {
		filterWhereClause = strings.Join(filterWhereConditions, " AND ")
	}

	if usecase == "trends" {
		return s.getTrendsCountByDayInternal(
			cteClausesStr,
			startDate,
			daysDiff,
			preFilterWhereClause,
			filterWhereClause,
			groupBy,
			joinClausesStr,
			filterFields,
		)
	}

	preFilterSelect := "e.event_id, e.impactScore"
	preFilterSelect = s.buildPreFilterSelectDates(preFilterSelect, forecasted)

	if (filterWhereClause != "" && strings.Contains(filterWhereClause, "keywords")) ||
		(preFilterWhereClause != "" && strings.Contains(preFilterWhereClause, "keywords")) {
		preFilterSelect += ", e.keywords"
	}

	query := fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS date
			FROM numbers(%d)
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		),
		grouped_counts AS (
			SELECT
				ds.date,
				group_name,
				uniq(e.event_id) AS eventsCount,
				sum(e.impactScore) AS eventImpactScore
			FROM preFilterEvent e
			INNER JOIN date_series ds ON true
			INNER JOIN testing_db.event_type_ch et ON e.event_id = et.event_id and et.published = 1
			ARRAY JOIN et.groups AS group_name
			WHERE %s
			GROUP BY
				ds.date,
				group_name
			ORDER BY
				ds.date
		)
		SELECT * FROM grouped_counts
	`,
		cteClausesStr,
		startDate,
		daysDiff,
		preFilterSelect,
		func() string {
			if joinClausesStr != "" {
				return "\t\t" + joinClausesStr
			}
			return ""
		}(),
		preFilterWhereClause,
		func() string {
			conditions := []string{}
			if filterWhereClause != "" {
				cleaned := strings.ReplaceAll(filterWhereClause, "ee.", "e.")
				cleaned = strings.ReplaceAll(cleaned, "e.event_id = et.event_id", "")
				cleaned = strings.TrimSpace(cleaned)
				cleaned = strings.TrimPrefix(cleaned, "AND")
				cleaned = strings.TrimSuffix(cleaned, "AND")
				cleaned = strings.TrimSpace(cleaned)
				if cleaned != "" {
					conditions = append(conditions, cleaned)
				}
			}
			dateJoinCond := s.buildTrendsDateCondition(filterFields.Forecasted, "e", "dateJoin", "", "")
			if dateJoinCond != "" {
				conditions = append(conditions, dateJoinCond)
			}
			conditions = append(conditions, "group_name IN ('business', 'social', 'unattended')")
			return strings.Join(conditions, " AND ")
		}(),
	)

	log.Printf("Event count by day query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rowsData []map[string]interface{}
	for rows.Next() {
		var date time.Time
		var groupName string
		var eventsCount uint64
		var eventImpactScore uint64

		if err := rows.Scan(&date, &groupName, &eventsCount, &eventImpactScore); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowsData = append(rowsData, map[string]interface{}{
			"date":             date.Format("2006-01-02"),
			"group_name":       groupName,
			"eventsCount":      int(eventsCount),
			"eventImpactScore": float64(eventImpactScore),
		})
	}

	return rowsData, nil
}

func (s *SharedFunctionService) getTrendsCountByDayInternal(
	cteClausesStr string,
	startDate string,
	daysDiff int,
	preFilterWhereClause string,
	filterWhereClause string,
	groupBy []models.CountGroup,
	joinClausesStr string,
	filterFields models.FilterDataDto,
) (interface{}, error) {
	if len(groupBy) < 2 {
		return nil, fmt.Errorf("groupBy must have at least 2 elements: [dateView, column, ...secondaryGroups]")
	}
	_ = groupBy[0]
	column := string(groupBy[1])
	columnStr := column
	secondaryGroupBy := groupBy[2:]

	var selectors []string
	var groupByClauses []string
	var joinClauses []string
	needsEventTypeJoin := false

	switch column {
	case "eventCount":
		selectors = append(selectors, "uniq(e.event_id) AS eventCount")
	case "predictedAttendance":
		selectors = append(selectors, "sum(e.estimatedVisitorsMean) AS predictedAttendance")
	case "inboundEstimate":
		selectors = append(selectors, "sum(e.inboundAttendance) AS inboundEstimate")
	case "internationalEstimate":
		selectors = append(selectors, "sum(e.internationalAttendance) AS internationalEstimate")
	case "impactScore":
		selectors = append(selectors, "sum(e.impactScore) AS impactScore")
	case "economicImpact":
		selectors = append(selectors, "sum(e.event_economic_value) AS economicImpact")
	case "hotel", "food", "entertainment", "airline", "transport", "utilitie":
		switch column {
		case "hotel":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Accommodation'))) AS hotel")
		case "food":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Food & Beverages'))) AS food")
		case "entertainment":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Entertainment'))) AS entertainment")
		case "airline":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Flights'))) AS airline")
		case "transport":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Transportation'))) AS transport")
		case "utilitie":
			selectors = append(selectors, "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', 'Utilities'))) AS utilitie")
		}
	default:
		return nil, fmt.Errorf("unsupported column: %s", column)
	}

	for _, group := range secondaryGroupBy {
		switch group {
		case models.CountGroupEventType:
			selectors = append(selectors, "et.slug AS eventType")
			groupByClauses = append(groupByClauses, "et.slug")
			needsEventTypeJoin = true
		case models.CountGroupEventTypeGroup:
			selectors = append(selectors, "group_name AS eventTypeGroup")
			groupByClauses = append(groupByClauses, "group_name")
			needsEventTypeJoin = true
		}
	}

	if needsEventTypeJoin {
		joinClauses = append(joinClauses, "INNER JOIN testing_db.event_type_ch et ON e.event_id = et.event_id and et.published = 1")
		if len(secondaryGroupBy) > 0 && secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
			joinClauses = append(joinClauses, "ARRAY JOIN et.groups AS group_name")
		}
	}

	whereConditions := []string{}
	if filterWhereClause != "" {
		cleaned := strings.ReplaceAll(filterWhereClause, "ee.", "e.")
		cleaned = strings.ReplaceAll(cleaned, "e.event_id = et.event_id", "")
		cleaned = strings.TrimSpace(cleaned)
		cleaned = strings.TrimPrefix(cleaned, "AND")
		cleaned = strings.TrimSuffix(cleaned, "AND")
		cleaned = strings.TrimSpace(cleaned)
		if cleaned != "" {
			whereConditions = append(whereConditions, cleaned)
		}
	}
	dateJoinCond := s.buildTrendsDateCondition(filterFields.Forecasted, "e", "dateJoin", "", "")
	isDayWiseEconomicColumn := columnStr == "hotel" || columnStr == "food" || columnStr == "entertainment" || columnStr == "airline" || columnStr == "transport" || columnStr == "utilitie"
	if dateJoinCond != "" && !isDayWiseEconomicColumn {
		whereConditions = append(whereConditions, dateJoinCond)
	}
	if needsEventTypeJoin && len(secondaryGroupBy) > 0 && secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
		whereConditions = append(whereConditions, "group_name IN ('business', 'social', 'unattended')")
	}

	whereClause := strings.Join(whereConditions, " AND ")
	if whereClause == "" {
		whereClause = "1 = 1"
	}

	groupByStr := "ds.date"
	if len(groupByClauses) > 0 {
		groupByStr += ", " + strings.Join(groupByClauses, ", ")
	}

	selectStr := "ds.date"
	if len(selectors) > 0 {
		selectStr += ", " + strings.Join(selectors, ", ")
	}

	joinStr := ""
	if len(joinClauses) > 0 {
		joinStr = strings.Join(joinClauses, "\n            ")
	}

	preFilterSelect := "e.event_id"
	if columnStr != "eventCount" {
		switch columnStr {
		case "predictedAttendance":
			preFilterSelect += ", e.estimatedVisitorsMean"
		case "inboundEstimate":
			preFilterSelect += ", e.inboundAttendance"
		case "internationalEstimate":
			preFilterSelect += ", e.internationalAttendance"
		case "impactScore":
			preFilterSelect += ", e.impactScore"
		case "economicImpact":
			preFilterSelect += ", e.event_economic_value"
		case "hotel", "food", "entertainment", "airline", "transport", "utilitie":
			preFilterSelect += ", e.event_economic_dayWiseEconomicImpact"
		}
	}

	forecasted := filterFields.Forecasted
	preFilterSelect = s.buildPreFilterSelectDates(preFilterSelect, forecasted)

	if filterWhereClause != "" && strings.Contains(filterWhereClause, "keywords") {
		preFilterSelect += ", e.keywords"
	}

	dateJoinCondition := s.buildTrendsDateCondition(forecasted, "e", "dateJoin", "", "")
	if dateJoinCondition == "" {
		dateJoinCondition = "e.start_date <= ds.date AND e.end_date >= ds.date"
	}
	if columnStr == "hotel" || columnStr == "food" || columnStr == "entertainment" || columnStr == "airline" || columnStr == "transport" || columnStr == "utilitie" {
		dateJoinCondition = "JSONHas(toJSONString(e.event_economic_dayWiseEconomicImpact), formatDateTime(ds.date, '%Y-%m-%d'))"
	}

	startDateParsed, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate format: %w", err)
	}
	endDate := startDateParsed.AddDate(0, 0, daysDiff-1).Format("2006-01-02")

	preFilterDateCondition := s.buildTrendsDateCondition(forecasted, "e", "preFilterRange", startDate, endDate)
	preFilterWhereWithDate := preFilterWhereClause
	if preFilterDateCondition != "" {
		preFilterWhereWithDate = fmt.Sprintf("%s AND %s", preFilterWhereClause, preFilterDateCondition)
	}

	var query string
	if isDayWiseEconomicColumn {
		joinStrE2 := strings.ReplaceAll(joinStr, "e.event_id", "e2.event_id")
		selectStrWithDateAlias := strings.Replace(selectStr, "ds.date", "ds.date AS date", 1)
		query = fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS date
			FROM numbers(%d)
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		),
		exploded AS (
			SELECT e.event_id AS event_id, toDate(kv.1) AS date, kv.2 AS value_json
			FROM preFilterEvent e
			ARRAY JOIN JSONExtractKeysAndValuesRaw(ifNull(toJSONString(e.event_economic_dayWiseEconomicImpact), '{}')) AS kv
			WHERE toJSONString(e.event_economic_dayWiseEconomicImpact) != '{}' AND toJSONString(e.event_economic_dayWiseEconomicImpact) != 'null'
		)
		SELECT
			%s
		FROM exploded e2
		INNER JOIN date_series ds ON e2.date = ds.date
		%s
		WHERE %s
		GROUP BY %s
		ORDER BY ds.date
		`,
			cteClausesStr,
			startDate,
			daysDiff,
			preFilterSelect,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			preFilterWhereWithDate,
			selectStrWithDateAlias,
			func() string {
				if joinStrE2 != "" {
					return "\n            " + joinStrE2
				}
				return ""
			}(),
			whereClause,
			groupByStr,
		)
	} else {
		query = fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS date
			FROM numbers(%d)
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		)
		SELECT
			%s
		FROM preFilterEvent e
		INNER JOIN date_series ds ON (%s)
		%s
		WHERE %s
		GROUP BY %s
		ORDER BY ds.date
		`,
			cteClausesStr,
			startDate,
			daysDiff,
			preFilterSelect,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			preFilterWhereWithDate,
			selectStr,
			dateJoinCondition,
			joinStr,
			whereClause,
			groupByStr,
		)
	}

	log.Printf("Trends count by day query: %s", query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	return s.transformTrendsCountByDay(rows, groupBy)
}

func (s *SharedFunctionService) GetTrendsCountByLongDurations(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
	duration string,
	startDate string,
	endDate string,
	groupBy []models.CountGroup,
) (interface{}, error) {
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "e.")
	}

	forecasted := filterFields.Forecasted
	preFilterWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "e"),
	}
	dateCondition := s.buildTrendsDateCondition(forecasted, "e", "preFilterRange", startDate, endDate)
	if dateCondition != "" {
		preFilterWhereConditions = append(preFilterWhereConditions, dateCondition)
	}
	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		preFilterWhereConditions = append(preFilterWhereConditions, whereClauseFixed)
	}
	preFilterWhereClause := strings.Join(preFilterWhereConditions, " AND ")
	preFilterWhereClause = strings.ReplaceAll(preFilterWhereClause, "ee.", "e.")

	filterWhereConditions := []string{}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	filterWhereClause := ""
	if len(filterWhereConditions) > 0 {
		filterWhereClause = strings.Join(filterWhereConditions, " AND ")
	}

	return s.getTrendsCountByLongDurationsInternal(
		cteClausesStr,
		duration,
		startDate,
		endDate,
		preFilterWhereClause,
		filterWhereClause,
		groupBy,
		joinClausesStr,
		filterFields,
	)
}

func (s *SharedFunctionService) getTrendsCountByLongDurationsInternal(
	cteClausesStr string,
	duration string,
	startDate string,
	endDate string,
	preFilterWhereClause string,
	filterWhereClause string,
	groupBy []models.CountGroup,
	joinClausesStr string,
	filterFields models.FilterDataDto,
) (interface{}, error) {
	if len(groupBy) < 2 {
		return nil, fmt.Errorf("groupBy must have at least 2 elements: [dateView, column, ...secondaryGroups]")
	}
	_ = groupBy[0]
	column := string(groupBy[1])
	columnStr := column
	secondaryGroupBy := groupBy[2:]

	var intervalUnit string
	var dateFormat string
	switch duration {
	case "month":
		intervalUnit = "MONTH"
		dateFormat = "formatDateTime(fd.start_date, '%Y-%m')"
	case "year":
		intervalUnit = "YEAR"
		dateFormat = "toString(toYear(fd.start_date))"
	case "week":
		intervalUnit = "WEEK"
		dateFormat = "concat(toString(fd.start_date), '_', toString(fd.end_date))"
	default:
		return nil, fmt.Errorf("unsupported duration: %s. Valid options are: week, month, year", duration)
	}

	var selectors []string
	var groupByClauses []string
	var joinClauses []string
	needsEventTypeJoin := false
	useDayWiseExplodedCTE := false

	switch column {
	case "eventCount":
		selectors = append(selectors, "uniq(e.event_id) AS eventCount")
	case "predictedAttendance":
		selectors = append(selectors, "sum(e.estimatedVisitorsMean) AS predictedAttendance")
	case "inboundEstimate":
		selectors = append(selectors, "sum(e.inboundAttendance) AS inboundEstimate")
	case "internationalEstimate":
		selectors = append(selectors, "sum(e.internationalAttendance) AS internationalEstimate")
	case "impactScore":
		selectors = append(selectors, "sum(e.impactScore) AS impactScore")
	case "economicImpact":
		selectors = append(selectors, "sum(e.event_economic_value) AS economicImpact")
	case "hotel", "food", "entertainment", "airline", "transport", "utilitie":
		useDayWiseExplodedCTE = true
		dayWiseSumFromE2 := func(categoryKey string) string {
			return "sum(toFloat64OrZero(JSONExtractString(e2.value_json, 'breakdown', " + "'" + categoryKey + "'" + ")))"
		}
		switch column {
		case "hotel":
			selectors = append(selectors, dayWiseSumFromE2("Accommodation")+" AS hotel")
		case "food":
			selectors = append(selectors, dayWiseSumFromE2("Food & Beverages")+" AS food")
		case "entertainment":
			selectors = append(selectors, dayWiseSumFromE2("Entertainment")+" AS entertainment")
		case "airline":
			selectors = append(selectors, dayWiseSumFromE2("Flights")+" AS airline")
		case "transport":
			selectors = append(selectors, dayWiseSumFromE2("Transportation")+" AS transport")
		case "utilitie":
			selectors = append(selectors, dayWiseSumFromE2("Utilities")+" AS utilitie")
		}
	default:
		return nil, fmt.Errorf("unsupported column: %s", column)
	}

	for _, group := range secondaryGroupBy {
		switch group {
		case models.CountGroupEventType:
			selectors = append(selectors, "et.slug AS eventType")
			groupByClauses = append(groupByClauses, "et.slug")
			needsEventTypeJoin = true
		case models.CountGroupEventTypeGroup:
			selectors = append(selectors, "group_name AS eventTypeGroup")
			groupByClauses = append(groupByClauses, "group_name")
			needsEventTypeJoin = true
		}
	}

	if needsEventTypeJoin {
		eventTableAlias := "e"
		if useDayWiseExplodedCTE {
			eventTableAlias = "e2"
		}
		joinClauses = append(joinClauses, "INNER JOIN testing_db.event_type_ch et ON "+eventTableAlias+".event_id = et.event_id and et.published = 1")
		if len(secondaryGroupBy) > 0 && secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
			joinClauses = append(joinClauses, "ARRAY JOIN et.groups AS group_name")
		}
	}

	whereConditions := []string{}
	if !useDayWiseExplodedCTE && filterWhereClause != "" {
		cleaned := strings.ReplaceAll(filterWhereClause, "ee.", "e.")
		cleaned = strings.ReplaceAll(cleaned, "e.event_id = et.event_id", "")
		cleaned = strings.TrimSpace(cleaned)
		cleaned = strings.TrimPrefix(cleaned, "AND")
		cleaned = strings.TrimSuffix(cleaned, "AND")
		cleaned = strings.TrimSpace(cleaned)
		if cleaned != "" {
			whereConditions = append(whereConditions, cleaned)
		}
	}
	forecasted := filterFields.Forecasted
	finalDatesJoinCond := s.buildTrendsDateCondition(forecasted, "e", "finalDatesJoin", "", "")
	if finalDatesJoinCond != "" && !useDayWiseExplodedCTE {
		whereConditions = append(whereConditions, finalDatesJoinCond)
	}
	if needsEventTypeJoin && len(secondaryGroupBy) > 0 && secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
		whereConditions = append(whereConditions, "group_name IN ('business', 'social', 'unattended')")
	}

	whereClause := strings.Join(whereConditions, " AND ")
	if whereClause == "" {
		whereClause = "1 = 1"
	}

	groupByStr := "fd.start_date, fd.end_date"
	if len(groupByClauses) > 0 {
		groupByStr += ", " + strings.Join(groupByClauses, ", ")
	}

	selectStr := dateFormat + " AS start_date"
	if len(selectors) > 0 {
		selectStr += ", " + strings.Join(selectors, ", ")
	}

	joinStr := ""
	if len(joinClauses) > 0 {
		joinStr = strings.Join(joinClauses, "\n            ")
	}

	preFilterSelect := "e.event_id"
	if columnStr != "eventCount" {
		switch columnStr {
		case "predictedAttendance":
			preFilterSelect += ", e.estimatedVisitorsMean"
		case "inboundEstimate":
			preFilterSelect += ", e.inboundAttendance"
		case "internationalEstimate":
			preFilterSelect += ", e.internationalAttendance"
		case "impactScore":
			preFilterSelect += ", e.impactScore"
		case "economicImpact":
			preFilterSelect += ", e.event_economic_value"
		case "hotel", "food", "entertainment", "airline", "transport", "utilitie":
			preFilterSelect += ", e.event_economic_dayWiseEconomicImpact"
		}
	}
	preFilterSelect = s.buildPreFilterSelectDates(preFilterSelect, forecasted)

	if filterWhereClause != "" && strings.Contains(filterWhereClause, "keywords") {
		preFilterSelect += ", e.keywords"
	}

	var query string
	if useDayWiseExplodedCTE {
		query = fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL number %s AS duration_start
			FROM numbers(toUInt32(dateDiff('%s', toStartOfInterval(toDate('%s'), INTERVAL 1 %s), toStartOfInterval(toDate('%s'), INTERVAL 1 %s)) + 1))
		),
		final_dates AS (
			SELECT
				toDate('%s') AS start_date,
				least(
					toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			UNION ALL
			SELECT
				duration_start AS start_date,
				least(
					duration_start + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			FROM date_series
			WHERE duration_start > toDate('%s')
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		),
		exploded AS (
			SELECT e.event_id AS event_id, toDate(kv.1) AS date, kv.2 AS value_json
			FROM preFilterEvent e
			ARRAY JOIN JSONExtractKeysAndValuesRaw(ifNull(toJSONString(e.event_economic_dayWiseEconomicImpact), '{}')) AS kv
			WHERE toJSONString(e.event_economic_dayWiseEconomicImpact) != '{}' AND toJSONString(e.event_economic_dayWiseEconomicImpact) != 'null'
		)
		SELECT
			%s
		FROM exploded e2
		INNER JOIN final_dates fd ON e2.date >= fd.start_date AND e2.date <= fd.end_date
		%s
		WHERE %s
		GROUP BY %s
		ORDER BY fd.start_date
		`,
			cteClausesStr,
			startDate, intervalUnit, intervalUnit, intervalUnit, startDate, intervalUnit, endDate, intervalUnit,
			startDate, startDate, intervalUnit, intervalUnit, endDate,
			intervalUnit, endDate, startDate,
			preFilterSelect,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			preFilterWhereClause,
			selectStr,
			func() string {
				if joinStr != "" {
					return "\n            " + joinStr
				}
				return ""
			}(),
			whereClause,
			groupByStr,
		)
	} else {
		query = fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL number %s AS duration_start
			FROM numbers(toUInt32(dateDiff('%s', toStartOfInterval(toDate('%s'), INTERVAL 1 %s), toStartOfInterval(toDate('%s'), INTERVAL 1 %s)) + 1))
		),
		final_dates AS (
			SELECT
				toDate('%s') AS start_date,
				least(
					toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			UNION ALL
			SELECT
				duration_start AS start_date,
				least(
					duration_start + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			FROM date_series
			WHERE duration_start > toDate('%s')
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		)
		SELECT
			%s
		FROM preFilterEvent e
		INNER JOIN final_dates fd ON true
		%s
		WHERE %s
		GROUP BY %s
		ORDER BY fd.start_date
		`,
			cteClausesStr,
			startDate, intervalUnit, intervalUnit, intervalUnit, startDate, intervalUnit, endDate, intervalUnit,
			startDate, startDate, intervalUnit, intervalUnit, endDate,
			intervalUnit, endDate, startDate,
			preFilterSelect,
			func() string {
				if joinClausesStr != "" {
					return "\t\t" + joinClausesStr
				}
				return ""
			}(),
			preFilterWhereClause,
			selectStr,
			func() string {
				if joinStr != "" {
					return "\n            " + joinStr
				}
				return ""
			}(),
			whereClause,
			groupByStr,
		)
	}

	log.Printf("Trends count by %s query: %s", duration, query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	return s.transformTrendsCountByLongDurations(rows, groupBy, duration)
}

func (s *SharedFunctionService) GetEventCountByLongDurations(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	filterFields models.FilterDataDto,
	duration string,
	startDate string,
	endDate string,
) (interface{}, error) {
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	joinClausesStr := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClausesStr = strings.ReplaceAll(cteAndJoinResult.JoinClausesStr, "ee.", "e.")
	}

	forecasted := filterFields.Forecasted
	preFilterWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "e"),
	}
	dateCondition := s.buildTrendsDateCondition(forecasted, "e", "preFilterRange", startDate, endDate)
	if dateCondition != "" {
		preFilterWhereConditions = append(preFilterWhereConditions, dateCondition)
	}
	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		preFilterWhereConditions = append(preFilterWhereConditions, whereClauseFixed)
	}
	// Add join conditions to preFilterWhereConditions - this was missing!
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		preFilterWhereConditions = append(preFilterWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	preFilterWhereClause := strings.Join(preFilterWhereConditions, " AND ")
	preFilterWhereClause = strings.ReplaceAll(preFilterWhereClause, "ee.", "e.")

	filterWhereConditions := []string{}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(joinConditionsStr, "ee.", "e.")
		filterWhereConditions = append(filterWhereConditions, strings.TrimPrefix(joinConditionsFixed, "AND "))
	}
	filterWhereClause := ""
	if len(filterWhereConditions) > 0 {
		filterWhereClause = strings.Join(filterWhereConditions, " AND ")
	}

	var intervalUnit string
	var dateFormat string
	switch duration {
	case "month":
		intervalUnit = "MONTH"
		dateFormat = "formatDateTime(fd.start_date, '%Y-%m')"
	case "year":
		intervalUnit = "YEAR"
		dateFormat = "toString(toYear(fd.start_date))"
	case "week":
		intervalUnit = "WEEK"
		dateFormat = "concat(toString(fd.start_date), '_', toString(fd.end_date))"
	default:
		return nil, fmt.Errorf("unsupported duration: %s. Valid options are: month, year, week", duration)
	}

	preFilterSelect := "e.event_id"
	preFilterSelect = s.buildPreFilterSelectDates(preFilterSelect, forecasted)

	if (filterWhereClause != "" && strings.Contains(filterWhereClause, "keywords")) ||
		(preFilterWhereClause != "" && strings.Contains(preFilterWhereClause, "keywords")) {
		preFilterSelect += ", e.keywords"
	}

	finalWhereDateCondition := s.buildTrendsDateCondition(forecasted, "e", "finalDatesJoin", "", "")
	if finalWhereDateCondition == "" {
		finalWhereDateCondition = "e.start_date <= fd.end_date AND e.end_date >= fd.start_date"
	}

	query := fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL number %s AS duration_start
			FROM numbers(toUInt32(dateDiff('%s', toStartOfInterval(toDate('%s'), INTERVAL 1 %s), toStartOfInterval(toDate('%s'), INTERVAL 1 %s)) + 1))
		),
		final_dates AS (
			SELECT
				toDate('%s') AS start_date,
				least(
					toStartOfInterval(toDate('%s'), INTERVAL 1 %s) + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			UNION ALL
			SELECT
				duration_start AS start_date,
				least(
					duration_start + INTERVAL 1 %s - INTERVAL 1 DAY,
					toDate('%s')
				) AS end_date
			FROM date_series
			WHERE duration_start > toDate('%s')
		),
		preFilterEvent AS (
			SELECT
				%s
			FROM testing_db.allevent_ch AS e
			%s
			WHERE %s
		)
		SELECT
			%s AS start_date,
			uniq(e.event_id) AS count
		FROM preFilterEvent e
		INNER JOIN final_dates fd ON true
		WHERE %s
		GROUP BY fd.start_date, fd.end_date
		ORDER BY fd.start_date
	`,
		cteClausesStr,
		startDate, intervalUnit, intervalUnit, intervalUnit, startDate, intervalUnit, endDate, intervalUnit,
		startDate, startDate, intervalUnit, intervalUnit, endDate,
		intervalUnit, endDate, startDate,
		preFilterSelect,
		func() string {
			if joinClausesStr != "" {
				return "\t\t" + joinClausesStr
			}
			return ""
		}(),
		preFilterWhereClause,
		dateFormat,
		func() string {
			conditions := []string{}
			if filterWhereClause != "" {
				cleaned := strings.ReplaceAll(filterWhereClause, "ee.", "e.")
				cleaned = strings.ReplaceAll(cleaned, "e.event_id = et.event_id", "")
				cleaned = strings.TrimSpace(cleaned)
				cleaned = strings.TrimPrefix(cleaned, "AND")
				cleaned = strings.TrimSuffix(cleaned, "AND")
				cleaned = strings.TrimSpace(cleaned)
				if cleaned != "" {
					conditions = append(conditions, cleaned)
				}
			}
			conditions = append(conditions, finalWhereDateCondition)
			return strings.Join(conditions, " AND ")
		}(),
	)

	log.Printf("Event count by long durations (%s) query: %s", duration, query)

	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rowsData []map[string]interface{}
	for rows.Next() {
		var startDateStr string
		var count uint64

		if err := rows.Scan(&startDateStr, &count); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowsData = append(rowsData, map[string]interface{}{
			"start_date": startDateStr,
			"count":      int(count),
		})
	}

	return rowsData, nil
}

func (s *SharedFunctionService) BuildGroupByResponse(count interface{}, startTime time.Time) fiber.Map {
	responseTime := time.Since(startTime).Seconds()
	if countMap, ok := count.(map[string]interface{}); ok {
		for key, value := range countMap {
			if value == nil {
				countMap[key] = 0
			}
		}
		return fiber.Map{
			"status":     "success",
			"statusCode": 200,
			"meta": fiber.Map{
				"responseTime": responseTime,
			},
			"data": countMap,
		}
	}

	data := count
	if count == nil {
		data = []interface{}{}
	} else if v := reflect.ValueOf(count); v.Kind() == reflect.Slice && v.IsNil() {
		data = []interface{}{}
	}

	return fiber.Map{
		"status":     "success",
		"statusCode": 200,
		"meta": fiber.Map{
			"responseTime": responseTime,
		},
		"data": data,
	}
}

func (s *SharedFunctionService) BuildAlertQuery(params models.AlertSearchParams) (string, error) {
	searchBy := ""
	if len(params.LocationIds) > 0 {
		searchBy = "locationIds"
	} else if params.Coordinates != nil {
		searchBy = "coordinates"
	} else if len(params.EventIds) > 0 {
		searchBy = "eventIds"
	} else {
		searchBy = "default"
	}

	hasDay := false
	for _, v := range params.GroupBy {
		if v == models.AlertSearchGroupByDay {
			hasDay = true
			break
		}
	}

	if hasDay && (params.StartDate == nil || params.EndDate == nil) {
		return "", fmt.Errorf("startDate and endDate are required when grouping by day")
	}

	dateRangeConditions := []string{}
	if params.StartDate != nil && params.EndDate != nil {
		if hasDay {
			dateRangeConditions = append(dateRangeConditions, "a.startDate <= ds.day")
			dateRangeConditions = append(dateRangeConditions, "a.endDate >= ds.day")
		} else {
			if strings.TrimSpace(*params.EndDate) != "" {
				dateRangeConditions = append(dateRangeConditions, fmt.Sprintf("a.startDate <= '%s'", *params.EndDate))
			}
			if strings.TrimSpace(*params.StartDate) != "" {
				dateRangeConditions = append(dateRangeConditions, fmt.Sprintf("a.endDate >= '%s'", *params.StartDate))
			}
		}
	} else if params.EndDate != nil && strings.TrimSpace(*params.EndDate) != "" {
		dateRangeConditions = append(dateRangeConditions, fmt.Sprintf("a.startDate <= '%s'", *params.EndDate))
	} else if params.StartDate != nil && strings.TrimSpace(*params.StartDate) != "" {
		dateRangeConditions = append(dateRangeConditions, fmt.Sprintf("a.endDate >= '%s'", *params.StartDate))
	}

	sortBy := s.BuildAlertSortClause(params.SortBy)
	if sortBy == "" {
		sortBy = "a.startDate DESC"
	}

	groupByClause := ""
	if len(params.GroupBy) > 0 {
		groupByFields := []string{}
		for _, group := range params.GroupBy {
			switch group {
			case models.AlertSearchGroupByDay:
				groupByFields = append(groupByFields, "ds.day")
			case models.AlertSearchGroupByAlertType:
				groupByFields = append(groupByFields, "a.type")
			}
		}
		if len(groupByFields) > 0 {
			groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupByFields, ", "))
		}
	}

	var query string
	var err error

	switch searchBy {
	case "locationIds":
		query, err = s.BuildLocationIdsAlertQuery(params, dateRangeConditions, sortBy, groupByClause, hasDay)
	case "coordinates":
		query, err = s.BuildCoordinatesAlertQuery(params, dateRangeConditions, sortBy, groupByClause, hasDay)
	case "eventIds":
		query, err = s.BuildEventIdsAlertQuery(params, dateRangeConditions, sortBy, groupByClause, hasDay)
	default:
		query, err = s.BuildDefaultAlertQuery(params, dateRangeConditions, sortBy, groupByClause, hasDay)
	}

	if err != nil {
		log.Printf("Error building alert query: %v", err)
		return "", err
	}

	return query, nil
}

func (s *SharedFunctionService) BuildAlertSortClause(sortBy []models.AlertSearchSortBy) string {
	if len(sortBy) == 0 {
		return ""
	}

	sortParts := []string{}
	for _, sort := range sortBy {
		switch sort {
		case models.AlertSearchSortBySeverity:
			sortParts = append(sortParts, "a.level ASC")
		case models.AlertSearchSortBySeverityDesc:
			sortParts = append(sortParts, "a.level DESC")
		case models.AlertSearchSortByStart:
			sortParts = append(sortParts, "a.startDate ASC")
		case models.AlertSearchSortByStartDesc:
			sortParts = append(sortParts, "a.startDate DESC")
		}
	}

	if len(sortParts) > 0 {
		return strings.Join(sortParts, ", ")
	}
	return ""
}

func (s *SharedFunctionService) BuildDefaultAlertQuery(params models.AlertSearchParams, dateRangeConditions []string, sortBy, groupByClause string, hasDay bool) (string, error) {
	dateRangeStr := ""
	if len(dateRangeConditions) > 0 {
		dateRangeStr = "WHERE " + strings.Join(dateRangeConditions, " AND ")
	}

	dateSeriesCTE := ""
	dateSeriesJoin := ""
	if hasDay && params.StartDate != nil && params.EndDate != nil {
		startDate := *params.StartDate
		endDate := *params.EndDate
		startTime, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return "", fmt.Errorf("invalid startDate format: %w", err)
		}
		endTime, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			return "", fmt.Errorf("invalid endDate format: %w", err)
		}
		daysDiff := int(endTime.Sub(startTime).Hours() / 24)

		dateSeriesCTE = fmt.Sprintf(`
		date_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS day
			FROM numbers(%d)
		),`, startDate, daysDiff+1)

		dateSeriesJoin = "INNER JOIN date_series ds ON (a.startDate <= ds.day AND a.endDate >= ds.day)"
	}

	withClause := ""
	if dateSeriesCTE != "" {
		withClause = "WITH " + strings.TrimSpace(dateSeriesCTE)
	}

	if params.Required == "count" {
		selectClause := "count(distinct a.id) as count"
		if hasDay {
			selectClause = "ds.day, count(distinct a.id) as count"
		}
		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.alerts_ch AS a
		%s
		%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, groupByClause)
		return strings.TrimSpace(query), nil
	} else {
		selectClause := `a.id AS id, a.name AS name, a.description AS description, a.type AS type, a.level AS level, 
			a.startDate AS startDate, a.endDate AS endDate, a.lastModified AS lastModified, 
			a.originLongitude AS originLongitude, a.originLatitude AS originLatitude`
		if hasDay {
			selectClause = "ds.day AS day, " + selectClause
		}

		limitClause := ""
		if params.Limit != nil {
			limitClause = fmt.Sprintf("LIMIT %d", *params.Limit)
			if params.Offset != nil {
				limitClause += fmt.Sprintf(" OFFSET %d", *params.Offset)
			}
		}

		orderClause := ""
		if sortBy != "" {
			orderClause = "ORDER BY " + sortBy
		}

		query := fmt.Sprintf(`
		%s
		SELECT DISTINCT %s
		FROM testing_db.alerts_ch AS a
		%s
		%s
		%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, orderClause, limitClause)
		return strings.TrimSpace(query), nil
	}
}

func (s *SharedFunctionService) BuildEventIdsAlertQuery(params models.AlertSearchParams, dateRangeConditions []string, sortBy, groupByClause string, hasDay bool) (string, error) {
	if len(params.EventIds) == 0 {
		return "", nil
	}

	eventUuidsStr := []string{}
	for _, eventId := range params.EventIds {
		eventId = strings.Trim(eventId, "'")
		eventUuidsStr = append(eventUuidsStr, fmt.Sprintf("'%s'", eventId))
	}
	eventUuidsClause := strings.Join(eventUuidsStr, ", ")

	dateRangeStr := ""
	if len(dateRangeConditions) > 0 {
		dateRangeStr = "AND " + strings.Join(dateRangeConditions, " AND ")
	}

	dateSeriesCTE := ""
	dateSeriesJoin := ""
	if hasDay && params.StartDate != nil && params.EndDate != nil {
		startDate := *params.StartDate
		endDate := *params.EndDate
		startTime, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return "", fmt.Errorf("invalid startDate format: %w", err)
		}
		endTime, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			return "", fmt.Errorf("invalid endDate format: %w", err)
		}
		daysDiff := int(endTime.Sub(startTime).Hours() / 24)

		dateSeriesCTE = fmt.Sprintf(`
		date_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS day
			FROM numbers(%d)
		),`, startDate, daysDiff+1)

		dateSeriesJoin = "INNER JOIN date_series ds ON (a.startDate <= ds.day AND a.endDate >= ds.day)"
	}

	cteParts := []string{}
	if dateSeriesCTE != "" {
		cteParts = append(cteParts, strings.TrimSpace(dateSeriesCTE))
	}
	defaultFilterFields := models.FilterDataDto{}
	defaultFilterFields.SetDefaultValues()
	editionTypeCondition := s.buildEditionTypeCondition(defaultFilterFields, "ee")
	cteParts = append(cteParts, fmt.Sprintf(`filtered_event_ids AS (
			SELECT DISTINCT ee.event_id
			FROM testing_db.allevent_ch AS ee
			WHERE ee.event_uuid IN (%s)
				AND %s
		)`, eventUuidsClause, editionTypeCondition))
	withClause := "WITH " + strings.Join(cteParts, ",\n\t\t")

	if params.Required == "count" {
		selectClause := "count(distinct a.id) as count"
		if hasDay {
			selectClause = "ds.day, count(distinct a.id) as count"
		}
		hasAlertTypeGroup := false
		for _, group := range params.GroupBy {
			if group == models.AlertSearchGroupByAlertType {
				hasAlertTypeGroup = true
				break
			}
		}
		if hasAlertTypeGroup {
			selectClause = "a.type, " + selectClause
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			AND et.event_id IN (SELECT event_id FROM filtered_event_ids)
			%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, groupByClause)
		return strings.TrimSpace(query), nil
	} else {
		selectClause := `DISTINCT a.id AS id, a.name AS name, a.description AS description, a.type AS type, a.level AS level,
			a.startDate AS startDate, a.endDate AS endDate, a.lastModified AS lastModified,
			a.originLongitude AS originLongitude, a.originLatitude AS originLatitude`
		if hasDay {
			selectClause = "ds.day AS day, " + selectClause
		}

		limitClause := ""
		if params.Limit != nil {
			limitClause = fmt.Sprintf("LIMIT %d", *params.Limit)
			if params.Offset != nil {
				limitClause += fmt.Sprintf(" OFFSET %d", *params.Offset)
			}
		}

		orderClause := ""
		if sortBy != "" {
			orderClause = "ORDER BY " + sortBy
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			AND et.event_id IN (SELECT event_id FROM filtered_event_ids)
			%s
		%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, orderClause, limitClause)
		return strings.TrimSpace(query), nil
	}
}

func (s *SharedFunctionService) BuildLocationIdsAlertQuery(params models.AlertSearchParams, dateRangeConditions []string, sortBy, groupByClause string, hasDay bool) (string, error) {
	if len(params.LocationIds) == 0 {
		return "", nil
	}

	locationIdsStr := []string{}
	for _, locationId := range params.LocationIds {
		locationId = strings.Trim(locationId, "'")
		locationIdsStr = append(locationIdsStr, fmt.Sprintf("'%s'", locationId))
	}
	locationIdsClause := strings.Join(locationIdsStr, ", ")

	dateRangeStr := ""
	if len(dateRangeConditions) > 0 {
		dateRangeStr = "AND " + strings.Join(dateRangeConditions, " AND ")
	}

	dateSeriesCTE := ""
	dateSeriesJoin := ""
	if hasDay && params.StartDate != nil && params.EndDate != nil {
		startDate := *params.StartDate
		endDate := *params.EndDate
		startTime, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return "", fmt.Errorf("invalid startDate format: %w", err)
		}
		endTime, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			return "", fmt.Errorf("invalid endDate format: %w", err)
		}
		daysDiff := int(endTime.Sub(startTime).Hours() / 24)

		dateSeriesCTE = fmt.Sprintf(`
		date_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS day
			FROM numbers(%d)
		),`, startDate, daysDiff+1)

		dateSeriesJoin = "INNER JOIN date_series ds ON (a.startDate <= ds.day AND a.endDate >= ds.day)"
	}

	cteParts := []string{}
	if dateSeriesCTE != "" {
		cteParts = append(cteParts, strings.TrimSpace(dateSeriesCTE))
	}
	cteParts = append(cteParts, fmt.Sprintf(`filtered_locations AS (
			SELECT id, id_uuid, location_type, iso
			FROM testing_db.location_ch
			WHERE id_uuid IN (%s)
		)`, locationIdsClause))
	defaultFilterFields := models.FilterDataDto{}
	defaultFilterFields.SetDefaultValues()
	editionTypeCondition := s.buildEditionTypeCondition(defaultFilterFields, "ee")
	cteParts = append(cteParts, fmt.Sprintf(`filtered_events AS (
			SELECT DISTINCT ee.event_id
			FROM testing_db.allevent_ch AS ee
			INNER JOIN filtered_locations AS loc ON (
				ee.venue_id = loc.id OR
				ee.venue_city = loc.id OR
				ee.edition_city_state_id = loc.id OR
				(loc.location_type = 'COUNTRY' AND ee.edition_country = loc.iso)
			)
			WHERE %s
		)`, editionTypeCondition))
	withClause := "WITH " + strings.Join(cteParts, ",\n\t\t")

	if params.Required == "count" {
		selectClause := "count(distinct a.id) as count"
		if hasDay {
			selectClause = "ds.day, count(distinct a.id) as count"
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN filtered_events AS fe ON et.event_id = fe.event_id
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, groupByClause)
		return strings.TrimSpace(query), nil
	} else {
		selectClause := `DISTINCT a.id AS id, a.name AS name, a.description AS description, a.type AS type, a.level AS level,
			a.startDate AS startDate, a.endDate AS endDate, a.lastModified AS lastModified,
			a.originLongitude AS originLongitude, a.originLatitude AS originLatitude`
		if hasDay {
			selectClause = "ds.day AS day, " + selectClause
		}

		limitClause := ""
		if params.Limit != nil {
			limitClause = fmt.Sprintf("LIMIT %d", *params.Limit)
			if params.Offset != nil {
				limitClause += fmt.Sprintf(" OFFSET %d", *params.Offset)
			}
		}

		orderClause := ""
		if sortBy != "" {
			orderClause = "ORDER BY " + sortBy
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN filtered_events AS fe ON et.event_id = fe.event_id
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			%s
		%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, orderClause, limitClause)
		return strings.TrimSpace(query), nil
	}
}

func (s *SharedFunctionService) BuildCoordinatesAlertQuery(params models.AlertSearchParams, dateRangeConditions []string, sortBy, groupByClause string, hasDay bool) (string, error) {
	if params.Coordinates == nil {
		return "", nil
	}

	lat := params.Coordinates.Latitude
	lon := params.Coordinates.Longitude
	radius := 10.0
	if params.Coordinates.Radius != nil {
		radius = *params.Coordinates.Radius
	}
	radiusMeters := radius * 1000

	dateRangeStr := ""
	if len(dateRangeConditions) > 0 {
		dateRangeStr = "AND " + strings.Join(dateRangeConditions, " AND ")
	}

	dateSeriesCTE := ""
	dateSeriesJoin := ""
	if hasDay && params.StartDate != nil && params.EndDate != nil {
		startDate := *params.StartDate
		endDate := *params.EndDate
		startTime, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return "", fmt.Errorf("invalid startDate format: %w", err)
		}
		endTime, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			return "", fmt.Errorf("invalid endDate format: %w", err)
		}
		daysDiff := int(endTime.Sub(startTime).Hours() / 24)

		dateSeriesCTE = fmt.Sprintf(`
		date_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS day
			FROM numbers(%d)
		),`, startDate, daysDiff+1)

		dateSeriesJoin = "INNER JOIN date_series ds ON (a.startDate <= ds.day AND a.endDate >= ds.day)"
	}

	cteParts := []string{}
	if dateSeriesCTE != "" {
		cteParts = append(cteParts, strings.TrimSpace(dateSeriesCTE))
	}
	cteParts = append(cteParts, fmt.Sprintf(`nearby_cities AS (
			SELECT id
			FROM testing_db.location_ch
			WHERE location_type = 'CITY'
				AND greatCircleDistance(longitude, latitude, %f, %f) <= %f
		)`, lon, lat, radiusMeters))
	defaultFilterFields := models.FilterDataDto{}
	defaultFilterFields.SetDefaultValues()
	editionTypeCondition := s.buildEditionTypeCondition(defaultFilterFields, "ee")
	cteParts = append(cteParts, fmt.Sprintf(`filtered_events AS (
			SELECT DISTINCT ee.event_id
			FROM testing_db.allevent_ch AS ee
			WHERE ee.venue_city IN (SELECT id FROM nearby_cities)
				AND %s
		)`, editionTypeCondition))
	withClause := "WITH " + strings.Join(cteParts, ",\n\t\t")

	if params.Required == "count" {
		selectClause := "count(distinct a.id) as count"
		if hasDay {
			selectClause = "ds.day, count(distinct a.id) as count"
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN filtered_events AS fe ON et.event_id = fe.event_id
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, groupByClause)
		return strings.TrimSpace(query), nil
	} else {
		selectClause := `DISTINCT a.id AS id, a.name AS name, a.description AS description, a.type AS type, a.level AS level,
			a.startDate AS startDate, a.endDate AS endDate, a.lastModified AS lastModified,
			a.originLongitude AS originLongitude, a.originLatitude AS originLatitude`
		if hasDay {
			selectClause = "ds.day AS day, " + selectClause
		}

		limitClause := ""
		if params.Limit != nil {
			limitClause = fmt.Sprintf("LIMIT %d", *params.Limit)
			if params.Offset != nil {
				limitClause += fmt.Sprintf(" OFFSET %d", *params.Offset)
			}
		}

		orderClause := ""
		if sortBy != "" {
			orderClause = "ORDER BY " + sortBy
		} else {
			orderClause = "ORDER BY a.level, a.startDate DESC"
		}

		query := fmt.Sprintf(`
		%s
		SELECT %s
		FROM testing_db.event_type_ch AS et
		INNER JOIN filtered_events AS fe ON et.event_id = fe.event_id
		INNER JOIN testing_db.alerts_ch AS a ON et.alert_id = a.id
		%s
		WHERE et.alert_id IS NOT NULL
			%s
		%s
		%s`, withClause, selectClause, dateSeriesJoin, dateRangeStr, orderClause, limitClause)
		return strings.TrimSpace(query), nil
	}
}

func (s *SharedFunctionService) GetCalendarEvents(filterFields models.FilterDataDto) (*ListResult, error) {
	if filterFields.ParsedCalendarType == nil {
		return &ListResult{
			StatusCode:   http.StatusBadRequest,
			ErrorMessage: "calendar_type is required for calendar view",
		}, nil
	}

	calendarType := *filterFields.ParsedCalendarType

	switch calendarType {
	case "day":
		var startDate, endDate string
		if len(filterFields.ParsedTrackerDates) == 2 {
			startDate = filterFields.ParsedTrackerDates[0]
			endDate = filterFields.ParsedTrackerDates[1]
		} else {
			if filterFields.ActiveGte != "" && filterFields.ActiveLte != "" {
				startDate = filterFields.ActiveGte
				endDate = filterFields.ActiveLte
			} else {
				return &ListResult{
					StatusCode:   http.StatusBadRequest,
					ErrorMessage: "startDate and endDate are required (use trackerDates, active.gte/active.lte, activeBetween, or pastBetween)",
				}, nil
			}
		}

		queryResult, err := s.buildClickHouseQuery(filterFields)
		if err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error building ClickHouse query: %v", err),
			}, nil
		}

		cteAndJoinResult := s.buildFilterCTEsAndJoins(
			queryResult.NeedsVisitorJoin, queryResult.NeedsSpeakerJoin, queryResult.NeedsExhibitorJoin,
			queryResult.NeedsSponsorJoin, queryResult.NeedsCategoryJoin, queryResult.NeedsTypeJoin,
			queryResult.NeedsEventRankingJoin, queryResult.needsDesignationJoin, queryResult.needsAudienceSpreadJoin,
			queryResult.NeedsRegionsJoin, queryResult.NeedsLocationIdsJoin, queryResult.NeedsCountryIdsJoin,
			queryResult.NeedsStateIdsJoin, queryResult.NeedsCityIdsJoin, queryResult.NeedsVenueIdsJoin,
			queryResult.NeedsUserIdUnionCTE,
			queryResult.VisitorWhereConditions, queryResult.SpeakerWhereConditions, queryResult.ExhibitorWhereConditions,
			queryResult.SponsorWhereConditions, queryResult.OrganizerWhereConditions, queryResult.CategoryWhereConditions, queryResult.TypeWhereConditions,
			queryResult.EventRankingWhereConditions, queryResult.JobCompositeWhereConditions, queryResult.AudienceSpreadWhereConditions,
			queryResult.RegionsWhereConditions, queryResult.LocationIdsWhereConditions, queryResult.CountryIdsWhereConditions,
			queryResult.StateIdsWhereConditions, queryResult.CityIdsWhereConditions, queryResult.VenueIdsWhereConditions,
			queryResult.UserIdWhereConditions,
			queryResult.NeedsCompanyIdUnionCTE,
			queryResult.CompanyIdWhereConditions,
			queryResult.WhereClause,
			queryResult.SearchClause,
			nil,
			nil,
			filterFields,
		)

		if !queryResult.NeedsTypeJoin {
			cteAndJoinResult.JoinConditions = append(cteAndJoinResult.JoinConditions, "e.event_id = et.event_id")
		}

		type dayCountResult struct {
			data interface{}
			err  error
		}
		type totalCountResult struct {
			count int
			err   error
		}

		dayCountChan := make(chan dayCountResult, 1)
		totalCountChan := make(chan totalCountResult, 1)

		go func() {
			dayCountData, err := s.GetEventCountByDay(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				startDate,
				endDate,
				"calendar",
				nil, // calendar doesn't use groupBy array
			)
			dayCountChan <- dayCountResult{data: dayCountData, err: err}
		}()

		go func() {
			count, err := s.getCountOnly(filterFields)
			totalCountChan <- totalCountResult{count: count, err: err}
		}()

		dayResult := <-dayCountChan
		countRes := <-totalCountChan

		if dayResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting day count: %v", dayResult.err),
			}, nil
		}

		if countRes.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting total count: %v", countRes.err),
			}, nil
		}

		dayCountData := dayResult.data
		dayCountMap, ok := dayCountData.([]map[string]interface{})
		if !ok {
			dayCountMap = []map[string]interface{}{}
		}

		transformedDayCount := s.transformDataService.TransformEventCountByDay(dayCountMap)

		return &ListResult{
			StatusCode: 200,
			Data: fiber.Map{
				"count": countRes.count,
				"data":  transformedDayCount,
			},
		}, nil

	case "week":
		var startDate, endDate string
		if len(filterFields.ParsedTrackerDates) == 2 {
			startDate = filterFields.ParsedTrackerDates[0]
			endDate = filterFields.ParsedTrackerDates[1]
		} else {
			if filterFields.ActiveGte == "" || filterFields.ActiveLte == "" {
				return &ListResult{
					StatusCode:   http.StatusBadRequest,
					ErrorMessage: "startDate and endDate are required (use trackerDates or active.gte/active.lte)",
				}, nil
			}
			startDate = filterFields.ActiveGte
			endDate = filterFields.ActiveLte
		}

		type weekEventsResult struct {
			data fiber.Map
			err  error
		}
		type totalCountResult struct {
			count int
			err   error
		}

		weekEventsChan := make(chan weekEventsResult, 1)
		totalCountChan := make(chan totalCountResult, 1)

		go func() {
			result, err := s.getEventsByWeek(filterFields, startDate, endDate)
			weekEventsChan <- weekEventsResult{data: result, err: err}
		}()

		go func() {
			count, err := s.getCountOnly(filterFields)
			totalCountChan <- totalCountResult{count: count, err: err}
		}()

		weekResult := <-weekEventsChan
		countRes := <-totalCountChan

		if weekResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: weekResult.err.Error(),
			}, nil
		}

		if countRes.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting total count: %v", countRes.err),
			}, nil
		}

		result := weekResult.data
		if result == nil {
			result = fiber.Map{}
		}
		result["count"] = countRes.count

		return &ListResult{
			StatusCode: 200,
			Data:       result,
		}, nil

	case "month":
		var startDate, endDate string
		if len(filterFields.ParsedTrackerDates) == 2 {
			startDate = filterFields.ParsedTrackerDates[0]
			endDate = filterFields.ParsedTrackerDates[1]
		} else {
			if filterFields.ActiveGte == "" || filterFields.ActiveLte == "" {
				return &ListResult{
					StatusCode:   http.StatusBadRequest,
					ErrorMessage: "startDate and endDate are required (use trackerDates or active.gte/active.lte)",
				}, nil
			}
			startDate = filterFields.ActiveGte
			endDate = filterFields.ActiveLte
		}

		queryResult, err := s.buildClickHouseQuery(filterFields)
		if err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error building ClickHouse query: %v", err),
			}, nil
		}

		cteAndJoinResult := s.buildFilterCTEsAndJoins(
			queryResult.NeedsVisitorJoin, queryResult.NeedsSpeakerJoin, queryResult.NeedsExhibitorJoin,
			queryResult.NeedsSponsorJoin, queryResult.NeedsCategoryJoin, queryResult.NeedsTypeJoin,
			queryResult.NeedsEventRankingJoin, queryResult.needsDesignationJoin, queryResult.needsAudienceSpreadJoin,
			queryResult.NeedsRegionsJoin, queryResult.NeedsLocationIdsJoin, queryResult.NeedsCountryIdsJoin,
			queryResult.NeedsStateIdsJoin, queryResult.NeedsCityIdsJoin, queryResult.NeedsVenueIdsJoin,
			queryResult.NeedsUserIdUnionCTE,
			queryResult.VisitorWhereConditions, queryResult.SpeakerWhereConditions, queryResult.ExhibitorWhereConditions,
			queryResult.SponsorWhereConditions, queryResult.OrganizerWhereConditions, queryResult.CategoryWhereConditions, queryResult.TypeWhereConditions,
			queryResult.EventRankingWhereConditions, queryResult.JobCompositeWhereConditions, queryResult.AudienceSpreadWhereConditions,
			queryResult.RegionsWhereConditions, queryResult.LocationIdsWhereConditions, queryResult.CountryIdsWhereConditions,
			queryResult.StateIdsWhereConditions, queryResult.CityIdsWhereConditions, queryResult.VenueIdsWhereConditions,
			queryResult.UserIdWhereConditions,
			queryResult.NeedsCompanyIdUnionCTE,
			queryResult.CompanyIdWhereConditions,
			queryResult.WhereClause,
			queryResult.SearchClause,
			nil,
			nil,
			filterFields,
		)

		if !queryResult.NeedsTypeJoin {
			cteAndJoinResult.JoinConditions = append(cteAndJoinResult.JoinConditions, "e.event_id = et.event_id")
		}

		type dayCountResult struct {
			data interface{}
			err  error
		}
		type monthCountResult struct {
			data interface{}
			err  error
		}
		type totalCountResult struct {
			count int
			err   error
		}

		dayCountChan := make(chan dayCountResult, 1)
		monthCountChan := make(chan monthCountResult, 1)
		totalCountChan := make(chan totalCountResult, 1)

		go func() {
			dayCountData, err := s.GetEventCountByDay(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				startDate,
				endDate,
				"calendar",
				nil, // calendar doesn't use groupBy array
			)
			dayCountChan <- dayCountResult{data: dayCountData, err: err}
		}()

		go func() {
			monthCountData, err := s.GetEventCountByLongDurations(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				"month",
				startDate,
				endDate,
			)
			monthCountChan <- monthCountResult{data: monthCountData, err: err}
		}()

		go func() {
			count, err := s.getCountOnly(filterFields)
			totalCountChan <- totalCountResult{count: count, err: err}
		}()

		dayResult := <-dayCountChan
		monthResult := <-monthCountChan
		countRes := <-totalCountChan

		if dayResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting day count: %v", dayResult.err),
			}, nil
		}

		if monthResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting month count: %v", monthResult.err),
			}, nil
		}

		if countRes.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting total count: %v", countRes.err),
			}, nil
		}

		dayCountData := dayResult.data
		monthCountData := monthResult.data

		dayCountMap, ok := dayCountData.([]map[string]interface{})
		if !ok {
			dayCountMap = []map[string]interface{}{}
		}

		transformedDayCount := s.transformDataService.TransformEventCountByDay(dayCountMap)

		monthCountRows, ok := monthCountData.([]map[string]interface{})
		if !ok {
			monthCountRows = []map[string]interface{}{}
		}

		transformedMonthCount := s.transformDataService.TransformEventCountByLongDurations(monthCountRows, "month")

		return &ListResult{
			StatusCode: 200,
			Data: fiber.Map{
				"count":        countRes.count,
				"data":         transformedDayCount,
				"countByMonth": transformedMonthCount,
			},
		}, nil

	case "year":
		var startDate, endDate string
		if len(filterFields.ParsedTrackerDates) == 2 {
			startDate = filterFields.ParsedTrackerDates[0]
			endDate = filterFields.ParsedTrackerDates[1]
		} else {
			if filterFields.ActiveGte == "" || filterFields.ActiveLte == "" {
				return &ListResult{
					StatusCode:   http.StatusBadRequest,
					ErrorMessage: "startDate and endDate are required (use trackerDates or active.gte/active.lte)",
				}, nil
			}
			startDate = filterFields.ActiveGte
			endDate = filterFields.ActiveLte
		}

		queryResult, err := s.buildClickHouseQuery(filterFields)
		if err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error building ClickHouse query: %v", err),
			}, nil
		}

		cteAndJoinResult := s.buildFilterCTEsAndJoins(
			queryResult.NeedsVisitorJoin, queryResult.NeedsSpeakerJoin, queryResult.NeedsExhibitorJoin,
			queryResult.NeedsSponsorJoin, queryResult.NeedsCategoryJoin, queryResult.NeedsTypeJoin,
			queryResult.NeedsEventRankingJoin, queryResult.needsDesignationJoin, queryResult.needsAudienceSpreadJoin,
			queryResult.NeedsRegionsJoin, queryResult.NeedsLocationIdsJoin, queryResult.NeedsCountryIdsJoin,
			queryResult.NeedsStateIdsJoin, queryResult.NeedsCityIdsJoin, queryResult.NeedsVenueIdsJoin,
			queryResult.NeedsUserIdUnionCTE,
			queryResult.VisitorWhereConditions, queryResult.SpeakerWhereConditions, queryResult.ExhibitorWhereConditions,
			queryResult.SponsorWhereConditions, queryResult.OrganizerWhereConditions, queryResult.CategoryWhereConditions, queryResult.TypeWhereConditions,
			queryResult.EventRankingWhereConditions, queryResult.JobCompositeWhereConditions, queryResult.AudienceSpreadWhereConditions,
			queryResult.RegionsWhereConditions, queryResult.LocationIdsWhereConditions, queryResult.CountryIdsWhereConditions,
			queryResult.StateIdsWhereConditions, queryResult.CityIdsWhereConditions, queryResult.VenueIdsWhereConditions,
			queryResult.UserIdWhereConditions,
			queryResult.NeedsCompanyIdUnionCTE,
			queryResult.CompanyIdWhereConditions,
			queryResult.WhereClause,
			queryResult.SearchClause,
			nil,
			nil,
			filterFields,
		)

		if !queryResult.NeedsTypeJoin {
			cteAndJoinResult.JoinConditions = append(cteAndJoinResult.JoinConditions, "e.event_id = et.event_id")
		}

		type dayCountResult struct {
			data interface{}
			err  error
		}
		type monthCountResult struct {
			data interface{}
			err  error
		}
		type yearCountResult struct {
			data interface{}
			err  error
		}
		type totalCountResult struct {
			count int
			err   error
		}

		dayCountChan := make(chan dayCountResult, 1)
		monthCountChan := make(chan monthCountResult, 1)
		yearCountChan := make(chan yearCountResult, 1)
		totalCountChan := make(chan totalCountResult, 1)

		go func() {
			dayCountData, err := s.GetEventCountByDay(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				startDate,
				endDate,
				"calendar",
				nil, // calendar doesn't use groupBy array
			)
			dayCountChan <- dayCountResult{data: dayCountData, err: err}
		}()

		go func() {
			monthCountData, err := s.GetEventCountByLongDurations(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				"month",
				startDate,
				endDate,
			)
			monthCountChan <- monthCountResult{data: monthCountData, err: err}
		}()

		go func() {
			yearCountData, err := s.GetEventCountByLongDurations(
				queryResult,
				&cteAndJoinResult,
				filterFields,
				"year",
				startDate,
				endDate,
			)
			yearCountChan <- yearCountResult{data: yearCountData, err: err}
		}()

		go func() {
			count, err := s.getCountOnly(filterFields)
			totalCountChan <- totalCountResult{count: count, err: err}
		}()

		dayResult := <-dayCountChan
		monthResult := <-monthCountChan
		yearResult := <-yearCountChan
		countRes := <-totalCountChan

		if dayResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting day count: %v", dayResult.err),
			}, nil
		}

		if monthResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting month count: %v", monthResult.err),
			}, nil
		}

		if yearResult.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting year count: %v", yearResult.err),
			}, nil
		}

		if countRes.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting total count: %v", countRes.err),
			}, nil
		}

		dayCountData := dayResult.data
		monthCountData := monthResult.data
		yearCountData := yearResult.data

		dayCountMap, ok := dayCountData.([]map[string]interface{})
		if !ok {
			dayCountMap = []map[string]interface{}{}
		}

		transformedDayCount := s.transformDataService.TransformEventCountByDay(dayCountMap)

		monthCountRows, ok := monthCountData.([]map[string]interface{})
		if !ok {
			monthCountRows = []map[string]interface{}{}
		}

		transformedMonthCount := s.transformDataService.TransformEventCountByLongDurations(monthCountRows, "month")

		yearCountRows, ok := yearCountData.([]map[string]interface{})
		if !ok {
			yearCountRows = []map[string]interface{}{}
		}

		transformedYearCount := s.transformDataService.TransformEventCountByLongDurations(yearCountRows, "year")

		return &ListResult{
			StatusCode: 200,
			Data: fiber.Map{
				"count":        countRes.count,
				"data":         transformedDayCount,
				"countByMonth": transformedMonthCount,
				"countByYear":  transformedYearCount,
			},
		}, nil
	default:
		return &ListResult{
			StatusCode:   http.StatusBadRequest,
			ErrorMessage: fmt.Sprintf("Invalid calendar_type: %s. Valid options are: week, month, year, day", calendarType),
		}, nil
	}
}

func (s *SharedFunctionService) getEventsByWeek(filterFields models.FilterDataDto, startDate string, endDate string) (fiber.Map, error) {
	queryResult, err := s.buildClickHouseQuery(filterFields)
	if err != nil {
		return nil, fmt.Errorf("error building ClickHouse query: %w", err)
	}

	cteAndJoinResult := s.buildFilterCTEsAndJoins(
		queryResult.NeedsVisitorJoin,
		queryResult.NeedsSpeakerJoin,
		queryResult.NeedsExhibitorJoin,
		queryResult.NeedsSponsorJoin,
		queryResult.NeedsCategoryJoin,
		queryResult.NeedsTypeJoin,
		queryResult.NeedsEventRankingJoin,
		queryResult.needsDesignationJoin,
		queryResult.needsAudienceSpreadJoin,
		queryResult.NeedsRegionsJoin,
		queryResult.NeedsLocationIdsJoin,
		queryResult.NeedsCountryIdsJoin,
		queryResult.NeedsStateIdsJoin,
		queryResult.NeedsCityIdsJoin,
		queryResult.NeedsVenueIdsJoin,
		queryResult.NeedsUserIdUnionCTE,
		queryResult.VisitorWhereConditions,
		queryResult.SpeakerWhereConditions,
		queryResult.ExhibitorWhereConditions,
		queryResult.SponsorWhereConditions,
		queryResult.OrganizerWhereConditions,
		queryResult.CategoryWhereConditions,
		queryResult.TypeWhereConditions,
		queryResult.EventRankingWhereConditions,
		queryResult.JobCompositeWhereConditions,
		queryResult.AudienceSpreadWhereConditions,
		queryResult.RegionsWhereConditions,
		queryResult.LocationIdsWhereConditions,
		queryResult.CountryIdsWhereConditions,
		queryResult.StateIdsWhereConditions,
		queryResult.CityIdsWhereConditions,
		queryResult.VenueIdsWhereConditions,
		queryResult.UserIdWhereConditions,
		queryResult.NeedsCompanyIdUnionCTE,
		queryResult.CompanyIdWhereConditions,
		queryResult.WhereClause,
		queryResult.SearchClause,
		nil,
		nil,
		filterFields,
	)

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	forecasted := filterFields.Forecasted
	baseWhereConditions := []string{
		s.buildPublishedCondition(filterFields),
		s.buildStatusCondition(filterFields),
		s.buildEditionTypeCondition(filterFields, "ee"),
	}
	dateCondition := s.buildTrendsDateCondition(forecasted, "e", "preFilterRange", startDate, endDate)
	if dateCondition != "" {
		baseWhereConditions = append(baseWhereConditions, dateCondition)
	}

	if queryResult.WhereClause != "" {
		whereClauseFixed := strings.ReplaceAll(queryResult.WhereClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, whereClauseFixed)
	}
	if queryResult.SearchClause != "" {
		searchClauseFixed := strings.ReplaceAll(queryResult.SearchClause, "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, searchClauseFixed)
	}
	if joinConditionsStr != "" {
		joinConditionsFixed := strings.ReplaceAll(strings.TrimPrefix(joinConditionsStr, "AND "), "ee.", "e.")
		baseWhereConditions = append(baseWhereConditions, joinConditionsFixed)
	}
	whereClause := strings.Join(baseWhereConditions, " AND ")
	whereClause = strings.ReplaceAll(whereClause, "ee.", "e.")

	startDateParsed, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate format: %w", err)
	}
	endDateParsed, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid endDate format: %w", err)
	}
	daysDiff := int(endDateParsed.Sub(startDateParsed).Hours()/24) + 1

	startDateField := s.getDateFieldName(forecasted, "start", "e")
	endDateField := s.getDateFieldName(forecasted, "end", "e")
	preFilterSelect := fmt.Sprintf(`
				e.event_uuid,
				e.event_name,
				%s AS start_date,
				%s AS end_date,
				e.editions_audiance_type,
				e.impactScore,
				e.event_score,
				e.PrimaryEventType,
				toUInt32(dateDiff('day', toDate(%s), toDate(%s))) AS duration`, startDateField, endDateField, startDateField, endDateField)

	dateJoinCondition := "e.start_date <= ds.date AND e.end_date >= ds.date"

	orderByDateField := "e.start_date"

	query := fmt.Sprintf(`
		WITH %sdate_series AS (
			SELECT toDate(addDays(toDate('%s'), number)) AS date
			FROM numbers(%d)
		),
		preFilterEvent AS (
			SELECT %s
			FROM testing_db.allevent_ch AS e
			WHERE %s
		),
		events AS (
			SELECT 
				e.event_uuid AS id,
				e.event_name AS name,
				ds.date AS startDateTime,
				ds.date AS endDateTime,
				e.duration,
				multiIf(
					e.editions_audiance_type = 11000, 'B2B',
					e.editions_audiance_type = 10100, 'B2C',
					''
				) AS audienceType,
				e.impactScore AS impactScore,
				e.event_score AS score,
				e.PrimaryEventType AS primaryEventType,
				row_number() OVER (
					PARTITION BY ds.date 
					ORDER BY 
						e.event_score DESC,
						COALESCE(e.editions_audiance_type, 0) ASC,
						%s DESC,
						e.duration DESC
				) AS rn
			FROM preFilterEvent e
			INNER JOIN date_series ds ON (
				%s
			)
		)
		SELECT 
			id,
			name,
			toString(startDateTime) AS startDateTime,
			toString(endDateTime) AS endDateTime,
			audienceType,
			primaryEventType,
			impactScore,
			duration,
			score
		FROM events
		WHERE rn <= 5
		ORDER BY 
			startDateTime ASC,
			score DESC,
			audienceType ASC,
			duration DESC
	`,
		cteClausesStr,
		startDate,
		daysDiff,
		preFilterSelect,
		whereClause,
		orderByDateField,
		dateJoinCondition,
	)

	log.Printf("Events by week query: %s", query)

	queryStartTime := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(context.Background(), query)
	queryDuration := time.Since(queryStartTime)
	log.Printf("Calendar week query execution time: %v", queryDuration)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return nil, fmt.Errorf("ClickHouse query error: %w", err)
	}
	defer rows.Close()

	var events []map[string]interface{}
	for rows.Next() {
		var id, name, startDateTime, endDateTime, audienceType string
		var primaryEventType *string
		var impactScore uint32
		var score int32
		var duration uint32

		if err := rows.Scan(&id, &name, &startDateTime, &endDateTime, &audienceType, &primaryEventType, &impactScore, &duration, &score); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		event := map[string]interface{}{
			"id":            id,
			"name":          name,
			"startDateTime": startDateTime,
			"endDateTime":   endDateTime,
			"audienceType":  audienceType,
			"impactScore":   impactScore,
			"duration":      duration,
			"score":         score,
		}

		if primaryEventType != nil {
			if slug, exists := models.EventTypeById[*primaryEventType]; exists {
				event["primaryEventType"] = slug
			} else {
				event["primaryEventType"] = nil
			}
		} else {
			event["primaryEventType"] = nil
		}

		events = append(events, event)
	}

	transformedEvents := s.transformDataService.TransformEventsByWeek(events)

	return fiber.Map{
		"data": transformedEvents,
	}, nil
}

func (s *SharedFunctionService) GetTrendsEvents(filterFields models.FilterDataDto) (*ListResult, error) {
	if filterFields.ParsedDateView == nil {
		return &ListResult{
			StatusCode:   http.StatusBadRequest,
			ErrorMessage: "dateView is required for trends view",
		}, nil
	}

	dateView := *filterFields.ParsedDateView

	if dateView != "day" && dateView != "week" && dateView != "month" && dateView != "year" {
		return &ListResult{
			StatusCode:   http.StatusBadRequest,
			ErrorMessage: fmt.Sprintf("dateView '%s' not supported. Valid options are: day, week, month, year", dateView),
		}, nil
	}

	var startDate, endDate string
	if len(filterFields.ParsedTrackerDates) == 2 {
		startDate = filterFields.ParsedTrackerDates[0]
		endDate = filterFields.ParsedTrackerDates[1]
	} else {
		if filterFields.ActiveGte != "" && filterFields.ActiveLte != "" {
			startDate = filterFields.ActiveGte
			endDate = filterFields.ActiveLte
		} else if filterFields.ParsedActiveBetween != nil {
			startDate = filterFields.ParsedActiveBetween.Start
			endDate = filterFields.ParsedActiveBetween.End
		} else if filterFields.ParsedPastBetween != nil {
			startDate = filterFields.ParsedPastBetween.Start
			endDate = filterFields.ParsedPastBetween.End
		} else {
			return &ListResult{
				StatusCode:   http.StatusBadRequest,
				ErrorMessage: "startDate and endDate are required (use trackerDates, active.gte/active.lte, activeBetween, or pastBetween)",
			}, nil
		}
	}

	if len(filterFields.ParsedColumns) == 0 {
		return &ListResult{
			StatusCode:   http.StatusBadRequest,
			ErrorMessage: "columns are required for trends view",
		}, nil
	}

	queryResult, err := s.buildClickHouseQuery(filterFields)
	if err != nil {
		return &ListResult{
			StatusCode:   http.StatusInternalServerError,
			ErrorMessage: fmt.Sprintf("error building ClickHouse query: %v", err),
		}, nil
	}

	cteAndJoinResult := s.buildFilterCTEsAndJoins(
		queryResult.NeedsVisitorJoin, queryResult.NeedsSpeakerJoin, queryResult.NeedsExhibitorJoin,
		queryResult.NeedsSponsorJoin, queryResult.NeedsCategoryJoin, queryResult.NeedsTypeJoin,
		queryResult.NeedsEventRankingJoin, queryResult.needsDesignationJoin, queryResult.needsAudienceSpreadJoin,
		queryResult.NeedsRegionsJoin, queryResult.NeedsLocationIdsJoin, queryResult.NeedsCountryIdsJoin,
		queryResult.NeedsStateIdsJoin, queryResult.NeedsCityIdsJoin, queryResult.NeedsVenueIdsJoin,
		queryResult.NeedsUserIdUnionCTE,
		queryResult.VisitorWhereConditions, queryResult.SpeakerWhereConditions, queryResult.ExhibitorWhereConditions,
		queryResult.SponsorWhereConditions, queryResult.OrganizerWhereConditions, queryResult.CategoryWhereConditions, queryResult.TypeWhereConditions,
		queryResult.EventRankingWhereConditions, queryResult.JobCompositeWhereConditions, queryResult.AudienceSpreadWhereConditions,
		queryResult.RegionsWhereConditions, queryResult.LocationIdsWhereConditions, queryResult.CountryIdsWhereConditions,
		queryResult.StateIdsWhereConditions, queryResult.CityIdsWhereConditions, queryResult.VenueIdsWhereConditions,
		queryResult.UserIdWhereConditions,
		queryResult.NeedsCompanyIdUnionCTE,
		queryResult.CompanyIdWhereConditions,
		queryResult.WhereClause,
		queryResult.SearchClause,
		nil,
		nil,
		filterFields,
	)

	type trendsCountResult struct {
		result map[string]interface{}
		group  string
		column string
		err    error
	}

	// all queries run in parallel
	resultsChan := make(chan trendsCountResult, len(filterFields.ParsedColumns)*3)

	for _, column := range filterFields.ParsedColumns {
		col := column
		if dateView == "day" {
			// Day view uses GetEventCountByDay
			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView (day)
					models.CountGroup(col),                          // column
				}
				result, err := s.GetEventCountByDay(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					startDate,
					endDate,
					"trends",
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "total"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "total"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for total"), column: col, group: "total"}
				}
			}(col)

			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView
					models.CountGroup(col),                          // column
					models.CountGroupEventType,                      // eventType
				}
				result, err := s.GetEventCountByDay(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					startDate,
					endDate,
					"trends",
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "byEventType"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "byEventType"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for byEventType"), column: col, group: "byEventType"}
				}
			}(col)

			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView
					models.CountGroup(col),                          // column
					models.CountGroupEventTypeGroup,                 // eventTypeGroup
				}
				result, err := s.GetEventCountByDay(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					startDate,
					endDate,
					"trends",
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "byEventTypeGroup"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "byEventTypeGroup"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for byEventTypeGroup"), column: col, group: "byEventTypeGroup"}
				}
			}(col)
		} else {
			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView (week/month/year)
					models.CountGroup(col),                          // column
				}
				result, err := s.GetTrendsCountByLongDurations(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					dateView,
					startDate,
					endDate,
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "total"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "total"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for total"), column: col, group: "total"}
				}
			}(col)

			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView
					models.CountGroup(col),                          // column
					models.CountGroupEventType,                      // eventType
				}
				result, err := s.GetTrendsCountByLongDurations(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					dateView,
					startDate,
					endDate,
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "byEventType"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "byEventType"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for byEventType"), column: col, group: "byEventType"}
				}
			}(col)

			go func(col string) {
				groupBy := []models.CountGroup{
					models.CountGroup(*filterFields.ParsedDateView), // dateView
					models.CountGroup(col),                          // column
					models.CountGroupEventTypeGroup,                 // eventTypeGroup
				}
				result, err := s.GetTrendsCountByLongDurations(
					queryResult,
					&cteAndJoinResult,
					filterFields,
					dateView,
					startDate,
					endDate,
					groupBy,
				)
				if err != nil {
					resultsChan <- trendsCountResult{err: err, column: col, group: "byEventTypeGroup"}
					return
				}
				if resultMap, ok := result.(map[string]interface{}); ok {
					resultsChan <- trendsCountResult{result: resultMap, column: col, group: "byEventTypeGroup"}
				} else {
					resultsChan <- trendsCountResult{err: fmt.Errorf("unexpected result type for byEventTypeGroup"), column: col, group: "byEventTypeGroup"}
				}
			}(col)
		}
	}

	count := make(map[string]map[string]interface{})
	for i := 0; i < len(filterFields.ParsedColumns)*3; i++ {
		result := <-resultsChan
		if result.err != nil {
			return &ListResult{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: fmt.Sprintf("error getting trends count for column %s, group %s: %v", result.column, result.group, result.err),
			}, nil
		}

		resultMap := result.result
		for date, dateData := range resultMap {
			if count[date] == nil {
				count[date] = make(map[string]interface{})
			}
			if count[date][result.column] == nil {
				count[date][result.column] = make(map[string]interface{})
			}

			columnData := count[date][result.column].(map[string]interface{})
			switch result.group {
			case "total":
				if totalVal, ok := dateData.(map[string]interface{})["total"]; ok {
					columnData["total"] = totalVal
				}
			case "byEventType":
				columnData["byEventType"] = dateData
			case "byEventTypeGroup":
				columnData["byEventTypeGroup"] = dateData
			}
			count[date][result.column] = columnData
		}
	}

	defaultGroupedValues := make(map[string]interface{})
	for _, col := range filterFields.ParsedColumns {
		defaultGroupedValues[col] = map[string]interface{}{
			"total":            0,
			"byEventType":      make(map[string]interface{}),
			"byEventTypeGroup": make(map[string]interface{}),
		}
	}

	groupedDataWithEmptyDates, err := s.addEmptyDates(startDate, endDate, count, defaultGroupedValues, dateView)
	if err != nil {
		return &ListResult{
			StatusCode:   http.StatusInternalServerError,
			ErrorMessage: fmt.Sprintf("error adding empty dates: %v", err),
		}, nil
	}

	thresholdData := s.getThresholdData(groupedDataWithEmptyDates)

	return &ListResult{
		StatusCode: 200,
		Data: fiber.Map{
			"thresholds":  thresholdData,
			"groupedData": groupedDataWithEmptyDates,
		},
	}, nil
}

func (s *SharedFunctionService) transformTrendsCountByDay(rows driver.Rows, groupBy []models.CountGroup) (map[string]interface{}, error) {
	if len(groupBy) < 2 {
		return nil, fmt.Errorf("groupBy must have at least 2 elements")
	}

	column := groupBy[1]
	secondaryGroupBy := groupBy[2:]

	result := make(map[string]interface{})

	var columns []string
	var dateIdx, columnIdx, groupByIdx int = -1, -1, -1
	columnStr := string(column)
	firstRow := true

	for rows.Next() {
		if firstRow {
			columns = rows.Columns()
			log.Printf("transformTrendsCountByDay: Available columns: %v, looking for column: %s, secondaryGroupBy: %v", columns, columnStr, secondaryGroupBy)

			if len(columns) == 0 {
				return nil, fmt.Errorf("no columns returned from query")
			}

			for i, col := range columns {
				if col == "date" {
					dateIdx = i
				} else if col == columnStr {
					columnIdx = i
				} else if len(secondaryGroupBy) > 0 {
					if secondaryGroupBy[0] == models.CountGroupEventType && col == "eventType" {
						groupByIdx = i
					} else if secondaryGroupBy[0] == models.CountGroupEventTypeGroup && col == "eventTypeGroup" {
						groupByIdx = i
					}
				}
			}

			if dateIdx == -1 {
				return nil, fmt.Errorf("required columns not found in result: missing 'date' column. Available columns: %v", columns)
			}
			if columnIdx == -1 {
				return nil, fmt.Errorf("required columns not found in result: missing '%s' column. Available columns: %v", columnStr, columns)
			}
			if len(secondaryGroupBy) > 0 && groupByIdx == -1 {
				expectedCol := "eventType"
				if secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
					expectedCol = "eventTypeGroup"
				}
				return nil, fmt.Errorf("required columns not found in result: missing '%s' column for secondary group by. Available columns: %v", expectedCol, columns)
			}

			firstRow = false
		}

		scanArgs := make([]interface{}, len(columns))
		for i, col := range columns {
			if col == "date" {
				scanArgs[i] = new(time.Time)
			} else if col == columnStr {
				if columnStr == "eventCount" || columnStr == "inboundEstimate" || columnStr == "internationalEstimate" || columnStr == "impactScore" || columnStr == "predictedAttendance" {
					scanArgs[i] = new(uint64)
				} else {
					scanArgs[i] = new(float64)
				}
			} else if len(secondaryGroupBy) > 0 && ((secondaryGroupBy[0] == models.CountGroupEventType && col == "eventType") || (secondaryGroupBy[0] == models.CountGroupEventTypeGroup && col == "eventTypeGroup")) {
				scanArgs[i] = new(string)
			} else {
				scanArgs[i] = new(string)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		var dateStr string
		if datePtr, ok := scanArgs[dateIdx].(*time.Time); ok && datePtr != nil {
			dateStr = datePtr.Format("2006-01-02")
		} else {
			log.Printf("Error: date column is not *time.Time")
			continue
		}

		var columnValue interface{}
		if columnStr == "eventCount" || columnStr == "inboundEstimate" || columnStr == "internationalEstimate" || columnStr == "impactScore" || columnStr == "predictedAttendance" {
			if valPtr, ok := scanArgs[columnIdx].(*uint64); ok && valPtr != nil {
				columnValue = float64(*valPtr)
			} else {
				columnValue = 0
			}
		} else {
			if valPtr, ok := scanArgs[columnIdx].(*float64); ok && valPtr != nil {
				columnValue = *valPtr
			} else {
				columnValue = 0
			}
		}

		if result[dateStr] == nil {
			if len(secondaryGroupBy) > 0 {
				result[dateStr] = make(map[string]interface{})
			} else {
				result[dateStr] = map[string]interface{}{
					"total": columnValue,
				}
			}
		}

		if len(secondaryGroupBy) > 0 && groupByIdx != -1 {
			dateData := result[dateStr].(map[string]interface{})
			var groupKey string
			if groupValPtr, ok := scanArgs[groupByIdx].(*string); ok && groupValPtr != nil {
				groupKey = *groupValPtr
			} else {
				continue
			}

			if dateData[groupKey] == nil {
				dateData[groupKey] = columnValue
			} else {
				if existingVal, ok := dateData[groupKey].(float64); ok {
					if newVal, ok := columnValue.(float64); ok {
						dateData[groupKey] = existingVal + newVal
					}
				}
			}
		}
	}

	return result, nil
}

func (s *SharedFunctionService) transformTrendsCountByLongDurations(rows driver.Rows, groupBy []models.CountGroup, duration string) (map[string]interface{}, error) {
	if len(groupBy) < 2 {
		return nil, fmt.Errorf("groupBy must have at least 2 elements")
	}

	column := groupBy[1]
	secondaryGroupBy := groupBy[2:]

	result := make(map[string]interface{})

	var columns []string
	var dateIdx, columnIdx, groupByIdx int = -1, -1, -1
	columnStr := string(column)
	firstRow := true

	for rows.Next() {
		if firstRow {
			columns = rows.Columns()
			log.Printf("transformTrendsCountByLongDurations: Available columns: %v, looking for column: %s, secondaryGroupBy: %v, duration: %s", columns, columnStr, secondaryGroupBy, duration)

			if len(columns) == 0 {
				return nil, fmt.Errorf("no columns returned from query")
			}

			for i, col := range columns {
				if col == "start_date" {
					dateIdx = i
				} else if col == columnStr {
					columnIdx = i
				} else if len(secondaryGroupBy) > 0 {
					if secondaryGroupBy[0] == models.CountGroupEventType && col == "eventType" {
						groupByIdx = i
					} else if secondaryGroupBy[0] == models.CountGroupEventTypeGroup && col == "eventTypeGroup" {
						groupByIdx = i
					}
				}
			}

			if dateIdx == -1 {
				return nil, fmt.Errorf("required columns not found in result: missing 'start_date' column. Available columns: %v", columns)
			}
			if columnIdx == -1 {
				return nil, fmt.Errorf("required columns not found in result: missing '%s' column. Available columns: %v", columnStr, columns)
			}
			if len(secondaryGroupBy) > 0 && groupByIdx == -1 {
				expectedCol := "eventType"
				if secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
					expectedCol = "eventTypeGroup"
				}
				return nil, fmt.Errorf("required columns not found in result: missing '%s' column for secondary group by. Available columns: %v", expectedCol, columns)
			}

			firstRow = false
		}

		scanArgs := make([]interface{}, len(columns))
		for i, col := range columns {
			if col == "start_date" {
				scanArgs[i] = new(string)
			} else if col == columnStr {
				if columnStr == "eventCount" || columnStr == "inboundEstimate" || columnStr == "internationalEstimate" || columnStr == "impactScore" || columnStr == "predictedAttendance" {
					scanArgs[i] = new(uint64)
				} else {
					scanArgs[i] = new(float64)
				}
			} else if len(secondaryGroupBy) > 0 && ((secondaryGroupBy[0] == models.CountGroupEventType && col == "eventType") || (secondaryGroupBy[0] == models.CountGroupEventTypeGroup && col == "eventTypeGroup")) {
				scanArgs[i] = new(string)
			} else {
				scanArgs[i] = new(string)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		var dateStr string
		if datePtr, ok := scanArgs[dateIdx].(*string); ok && datePtr != nil {
			dateStr = *datePtr
			switch duration {
			case "month":
				parts := strings.Split(dateStr, "-")
				if len(parts) >= 2 {
					dateStr = parts[0] + "-" + parts[1]
				}
			case "year":
				parts := strings.Split(dateStr, "-")
				if len(parts) > 0 {
					dateStr = parts[0]
				}
			}
		} else {
			log.Printf("Error: start_date column is not *string")
			continue
		}

		var columnValue interface{}
		if columnStr == "eventCount" || columnStr == "inboundEstimate" || columnStr == "internationalEstimate" || columnStr == "impactScore" || columnStr == "predictedAttendance" {
			if valPtr, ok := scanArgs[columnIdx].(*uint64); ok && valPtr != nil {
				columnValue = float64(*valPtr)
			} else {
				columnValue = 0
			}
		} else {
			if valPtr, ok := scanArgs[columnIdx].(*float64); ok && valPtr != nil {
				columnValue = *valPtr
			} else {
				columnValue = 0
			}
		}

		if result[dateStr] == nil {
			if len(secondaryGroupBy) > 0 {
				result[dateStr] = make(map[string]interface{})
			} else {
				result[dateStr] = map[string]interface{}{
					"total": columnValue,
				}
			}
		}

		if len(secondaryGroupBy) > 0 && groupByIdx != -1 {
			dateData := result[dateStr].(map[string]interface{})
			var groupKey string
			if groupValPtr, ok := scanArgs[groupByIdx].(*string); ok && groupValPtr != nil {
				groupKey = *groupValPtr
			} else {
				continue
			}

			if secondaryGroupBy[0] == models.CountGroupEventTypeGroup {
				// Initialize with business, social, unattended if not exists
				if dateData[columnStr] == nil {
					dateData[columnStr] = map[string]interface{}{
						"business":   0,
						"social":     0,
						"unattended": 0,
					}
				}
				columnData := dateData[columnStr].(map[string]interface{})
				if groupKey == "business" || groupKey == "social" || groupKey == "unattended" {
					columnData[groupKey] = columnValue
				}
			} else {
				if dateData[columnStr] == nil {
					dateData[columnStr] = make(map[string]interface{})
				}
				columnData := dateData[columnStr].(map[string]interface{})
				columnData[groupKey] = columnValue
			}
		}
	}

	return result, nil
}

func (s *SharedFunctionService) addEmptyDates(startDate, endDate string, data map[string]map[string]interface{}, defaultValues map[string]interface{}, dateView string) (map[string]map[string]interface{}, error) {
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate format: %w", err)
	}
	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid endDate format: %w", err)
	}

	allDates := []string{}

	switch dateView {
	case "day":
		current := start
		for !current.After(end) {
			allDates = append(allDates, current.Format("2006-01-02"))
			current = current.AddDate(0, 0, 1)
		}
	case "week":
		firstWeekStart := start
		weekday := int(firstWeekStart.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		firstWeekStart = firstWeekStart.AddDate(0, 0, -(weekday - 1))
		firstWeekEnd := firstWeekStart.AddDate(0, 0, 6)
		if firstWeekEnd.After(end) {
			firstWeekEnd = end
		}
		allDates = append(allDates, firstWeekStart.Format("2006-01-02")+"_"+firstWeekEnd.Format("2006-01-02"))

		current := firstWeekStart.AddDate(0, 0, 7)
		for !current.After(end) {
			weekStart := current
			weekEnd := weekStart.AddDate(0, 0, 6)
			if weekEnd.After(end) {
				weekEnd = end
			}
			allDates = append(allDates, weekStart.Format("2006-01-02")+"_"+weekEnd.Format("2006-01-02"))
			current = current.AddDate(0, 0, 7)
		}

		lastWeekStart := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, end.Location())
		lastWeekday := int(lastWeekStart.Weekday())
		if lastWeekday == 0 {
			lastWeekday = 7
		}
		lastWeekStart = lastWeekStart.AddDate(0, 0, -(lastWeekday - 1))
		if lastWeekStart.After(firstWeekStart) && lastWeekStart != firstWeekStart {
			lastWeekEnd := end
			allDates = append(allDates, lastWeekStart.Format("2006-01-02")+"_"+lastWeekEnd.Format("2006-01-02"))
		}
	case "month":
		current := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, start.Location())
		for !current.After(end) {
			allDates = append(allDates, current.Format("2006-01"))
			current = current.AddDate(0, 1, 0)
		}
	case "year":
		current := time.Date(start.Year(), 1, 1, 0, 0, 0, 0, start.Location())
		for !current.After(end) {
			allDates = append(allDates, current.Format("2006"))
			current = current.AddDate(1, 0, 0)
		}
	default:
		return nil, fmt.Errorf("unsupported dateView: %s. Valid options are: day, week, month, year", dateView)
	}

	for _, date := range allDates {
		if data[date] == nil {
			defaultCopy := make(map[string]interface{})
			for k, v := range defaultValues {
				if vMap, ok := v.(map[string]interface{}); ok {
					vCopy := make(map[string]interface{})
					for k2, v2 := range vMap {
						if v2Map, ok2 := v2.(map[string]interface{}); ok2 {
							v2Copy := make(map[string]interface{})
							for k3, v3 := range v2Map {
								v2Copy[k3] = v3
							}
							vCopy[k2] = v2Copy
						} else {
							vCopy[k2] = v2
						}
					}
					defaultCopy[k] = vCopy
				} else {
					defaultCopy[k] = v
				}
			}
			data[date] = defaultCopy
		}
	}

	sortedData := make(map[string]map[string]interface{})
	dateKeys := make([]string, 0, len(data))
	for k := range data {
		dateKeys = append(dateKeys, k)
	}
	for i := 0; i < len(dateKeys)-1; i++ {
		for j := i + 1; j < len(dateKeys); j++ {
			if dateKeys[i] > dateKeys[j] {
				dateKeys[i], dateKeys[j] = dateKeys[j], dateKeys[i]
			}
		}
	}
	for _, k := range dateKeys {
		sortedData[k] = data[k]
	}

	return sortedData, nil
}

func (s *SharedFunctionService) getThresholdData(groupedData map[string]map[string]interface{}) map[string]interface{} {
	days := len(groupedData)
	if days == 0 {
		return make(map[string]interface{})
	}

	totals := make(map[string]float64)

	for _, dateData := range groupedData {
		for column, columnData := range dateData {
			if columnMap, ok := columnData.(map[string]interface{}); ok {
				var total float64
				if totalVal, ok := columnMap["total"].(float64); ok {
					total = totalVal
				} else if totalVal, ok := columnMap["total"].(int); ok {
					total = float64(totalVal)
				} else if totalVal, ok := columnMap["total"].(int64); ok {
					total = float64(totalVal)
				} else {
					total = 0
				}
				totals[column] += total
			}
		}
	}

	thresholds := make(map[string]interface{})
	for column, total := range totals {
		thresholds[column] = int(math.Round(total / float64(days)))
	}

	return thresholds
}

func (s *SharedFunctionService) audienceTrackerMatchInfo(eventIds []uint32, jobCompositeValues *models.JobCompositeProperty) (map[string]interface{}, error) {
	if len(eventIds) == 0 || jobCompositeValues == nil {
		return make(map[string]interface{}), nil
	}

	totalDesignationCount := len(jobCompositeValues.Department) + len(jobCompositeValues.Role) + len(jobCompositeValues.Name)
	if totalDesignationCount == 0 {
		return make(map[string]interface{}), nil
	}

	totalKeywords := make([]string, 0, totalDesignationCount)
	totalKeywords = append(totalKeywords, jobCompositeValues.Department...)
	totalKeywords = append(totalKeywords, jobCompositeValues.Role...)
	totalKeywords = append(totalKeywords, jobCompositeValues.Name...)

	var designationOrConditions []string

	if len(jobCompositeValues.Name) > 0 {
		escapedNames := make([]string, len(jobCompositeValues.Name))
		for i, name := range jobCompositeValues.Name {
			escapedNames[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(name, "'", "''"))
		}
		designationOrConditions = append(designationOrConditions, fmt.Sprintf("display_name IN (%s)", strings.Join(escapedNames, ",")))
	}

	if len(jobCompositeValues.Department) > 0 {
		escapedDepts := make([]string, len(jobCompositeValues.Department))
		for i, dept := range jobCompositeValues.Department {
			escapedDepts[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(dept, "'", "''"))
		}
		designationOrConditions = append(designationOrConditions, fmt.Sprintf("department IN (%s)", strings.Join(escapedDepts, ",")))
	}

	if len(jobCompositeValues.Role) > 0 {
		escapedRoles := make([]string, len(jobCompositeValues.Role))
		for i, role := range jobCompositeValues.Role {
			escapedRoles[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(role, "'", "''"))
		}
		designationOrConditions = append(designationOrConditions, fmt.Sprintf("role IN (%s)", strings.Join(escapedRoles, ",")))
	}

	if len(designationOrConditions) == 0 {
		return make(map[string]interface{}), nil
	}

	eventIdsStr := make([]string, len(eventIds))
	for i, eventId := range eventIds {
		eventIdsStr[i] = fmt.Sprintf("'%d'", eventId)
	}

	query := fmt.Sprintf(`
		SELECT 
			toString(event_id) AS eventid,
			count(*) AS count,
			arrayStringConcat(
				arrayMap(
					x -> concat(x.1, '<val-sep>', x.2, '<val-sep>', x.3),
					groupArray(tuple(toString(display_name), toString(role), toString(department)))
				),
				'<line-sep>'
			) AS descriptions
		FROM testing_db.event_designation_ch
		WHERE toString(event_id) IN (%s)
			AND (%s)
		GROUP BY event_id`, strings.Join(eventIdsStr, ","), strings.Join(designationOrConditions, " OR "))

	log.Printf("Audience tracker match info query: %s", query)

	ctx := context.Background()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query audience tracker match info: %w", err)
	}
	defer rows.Close()

	result := make(map[string]interface{})
	totalKeywordsSet := make(map[string]bool)
	for _, keyword := range totalKeywords {
		totalKeywordsSet[keyword] = true
	}

	for rows.Next() {
		var eventId string
		var count uint64
		var descriptions *string

		if err := rows.Scan(&eventId, &count, &descriptions); err != nil {
			log.Printf("Error scanning audience match info row: %v", err)
			continue
		}

		allDesignationDepartmentRole := make([]string, 0)
		if descriptions != nil && *descriptions != "" {
			rowWiseSplit := strings.Split(*descriptions, "<line-sep>")
			for _, row := range rowWiseSplit {
				values := strings.Split(row, "<val-sep>")
				for _, val := range values {
					trimmed := strings.TrimSpace(val)
					if trimmed != "" {
						allDesignationDepartmentRole = append(allDesignationDepartmentRole, trimmed)
					}
				}
			}
		}

		seen := make(map[string]bool)
		uniqueValues := make([]string, 0)
		for _, val := range allDesignationDepartmentRole {
			if !seen[val] {
				seen[val] = true
				uniqueValues = append(uniqueValues, val)
			}
		}

		countMatched := 0
		matchedKeywords := make([]string, 0)
		for _, val := range uniqueValues {
			if totalKeywordsSet[val] {
				countMatched++
				matchedKeywords = append(matchedKeywords, val)
			}
		}

		matchedKeywordsPercentage := 0.0
		if totalDesignationCount > 0 {
			matchedKeywordsPercentage = (float64(countMatched) / float64(totalDesignationCount)) * 100.0
			matchedKeywordsPercentage = math.Round(matchedKeywordsPercentage*100) / 100
		}

		result[eventId] = map[string]interface{}{
			"matchedKeywordsPercentage": matchedKeywordsPercentage,
			"matchedKeywords":           matchedKeywords,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading audience match info rows: %w", err)
	}

	return result, nil
}

func (s *SharedFunctionService) getEntityQualificationsForCompanyName(
	ctx context.Context,
	eventIds []uint32,
	filterFields models.FilterDataDto,
) (map[uint32][]string, error) {
	result := make(map[uint32][]string)

	hasCompanyName := len(filterFields.ParsedCompanyName) > 0
	hasUserCompanyName := len(filterFields.ParsedUserCompanyName) > 0
	hasCompanyWebsite := len(filterFields.ParsedCompanyWebsite) > 0
	hasCompanyId := len(filterFields.ParsedCompanyId) > 0
	isSpeakerEntity := false
	isUserEntity := false

	if len(filterFields.ParsedSearchByEntity) > 0 {
		searchByEntityLower := strings.ToLower(strings.TrimSpace(filterFields.ParsedSearchByEntity[0]))
		isSpeakerEntity = searchByEntityLower == "speaker"
		isUserEntity = searchByEntityLower == "user"
	}

	hasUserNameForEntity := (isSpeakerEntity || isUserEntity) && len(filterFields.ParsedUserName) > 0
	hasUserIdForEntity := (isSpeakerEntity || isUserEntity) && len(filterFields.ParsedUserId) > 0

	if (!hasCompanyName && !hasUserCompanyName && !hasCompanyWebsite && !hasCompanyId && !hasUserNameForEntity && !hasUserIdForEntity) || len(eventIds) == 0 {
		return result, nil
	}

	hasExhibitor := false
	hasSponsor := false
	hasOrganizer := false
	hasVisitor := false
	hasSpeaker := false

	if len(filterFields.ParsedAdvancedSearchBy) > 0 {
		for _, entity := range filterFields.ParsedAdvancedSearchBy {
			switch entity {
			case "exhibitor":
				hasExhibitor = true
			case "sponsor":
				hasSponsor = true
			case "organizer":
				hasOrganizer = true
			case "visitor":
				hasVisitor = true
			case "speaker":
				hasSpeaker = true
			}
		}
	} else {
		// Default: if no advancedSearchBy specified, run company-related queries
		hasExhibitor = true
		hasSponsor = true
		hasOrganizer = true
	}

	if !hasExhibitor && !hasSponsor && !hasOrganizer && !hasVisitor && !hasSpeaker {
		return result, nil
	}

	eventIdsStr := make([]string, len(eventIds))
	for i, id := range eventIds {
		eventIdsStr[i] = fmt.Sprintf("%d", id)
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")

	var exhibitorConditions []string
	var sponsorConditions []string
	var organizerConditions []string
	var visitorConditions []string
	var speakerConditions []string

	for _, companyName := range filterFields.ParsedCompanyName {
		if hasExhibitor {
			exhibitorCondition := s.matchPhraseConverter("company_id_name", companyName)
			if exhibitorCondition != "" {
				exhibitorConditions = append(exhibitorConditions, exhibitorCondition)
			}
		}
		if hasSponsor {
			sponsorCondition := s.matchPhraseConverter("company_id_name", companyName)
			if sponsorCondition != "" {
				sponsorConditions = append(sponsorConditions, sponsorCondition)
			}
		}
		if hasOrganizer {
			organizerCondition := s.matchPhraseConverter("company_name", companyName)
			if organizerCondition != "" {
				organizerConditions = append(organizerConditions, organizerCondition)
			}
		}
		if hasVisitor {
			visitorCondition := s.matchPhraseConverter("user_company", companyName)
			if visitorCondition != "" {
				visitorConditions = append(visitorConditions, visitorCondition)
			}
		}
		if hasSpeaker {
			isSpeakerEntityLocal := false
			if len(filterFields.ParsedSearchByEntity) > 0 {
				searchByEntityLower := strings.ToLower(strings.TrimSpace(filterFields.ParsedSearchByEntity[0]))
				if searchByEntityLower == "speaker" {
					isSpeakerEntityLocal = true
				}
			}

			if isSpeakerEntityLocal && len(filterFields.ParsedUserName) > 0 {
			} else {
				speakerCondition := s.matchPhraseConverter("user_company", companyName)
				if speakerCondition != "" {
					speakerConditions = append(speakerConditions, speakerCondition)
				}
			}
		}
	}

	for _, companyWebsite := range filterFields.ParsedCompanyWebsite {
		if hasExhibitor {
			exhibitorCondition := s.matchWebsiteConverter("company_website", companyWebsite)
			if exhibitorCondition != "" {
				exhibitorConditions = append(exhibitorConditions, exhibitorCondition)
			}
		}
		if hasSponsor {
			sponsorCondition := s.matchWebsiteConverter("company_website", companyWebsite)
			if sponsorCondition != "" {
				sponsorConditions = append(sponsorConditions, sponsorCondition)
			}
		}
		if hasOrganizer {
			organizerCondition := s.matchWebsiteConverter("company_website", companyWebsite)
			if organizerCondition != "" {
				organizerConditions = append(organizerConditions, organizerCondition)
			}
		}
	}

	useILikeForSpeaker := hasSpeaker && (len(filterFields.ParsedUserName) > 0 || len(filterFields.ParsedUserCompanyName) > 0)
	if useILikeForSpeaker {
		speakerUserNameCompanyCond := s.buildUserNameUserCompanyCondition(filterFields, true, false)
		if speakerUserNameCompanyCond != "" && hasSpeaker {
			speakerConditions = append(speakerConditions, speakerUserNameCompanyCond)
		}
		visitorUserNameCompanyCond := s.buildUserNameUserCompanyCondition(filterFields, false, true)
		if visitorUserNameCompanyCond != "" && hasVisitor {
			visitorConditions = append(visitorConditions, visitorUserNameCompanyCond)
		}
	} else {
		if hasSpeaker && (isSpeakerEntity || isUserEntity) && len(filterFields.ParsedUserName) > 0 {
			for _, userName := range filterFields.ParsedUserName {
				speakerCondition := s.matchPhraseConverter("user_name", userName)
				if speakerCondition != "" {
					speakerConditions = append(speakerConditions, speakerCondition)
				}
			}
		}

		if hasSpeaker && (isSpeakerEntity || isUserEntity) && len(filterFields.ParsedUserCompanyName) > 0 {
			for _, companyName := range filterFields.ParsedUserCompanyName {
				speakerCondition := s.matchPhraseConverter("user_company", companyName)
				if speakerCondition != "" {
					speakerConditions = append(speakerConditions, speakerCondition)
				}
			}
		}

		if hasVisitor && (isUserEntity || isSpeakerEntity) && len(filterFields.ParsedUserName) > 0 {
			for _, userName := range filterFields.ParsedUserName {
				visitorCondition := s.matchPhraseConverter("user_name", userName)
				if visitorCondition != "" {
					visitorConditions = append(visitorConditions, visitorCondition)
				}
			}
		}

		if hasVisitor && (isUserEntity || isSpeakerEntity) && len(filterFields.ParsedUserCompanyName) > 0 {
			for _, companyName := range filterFields.ParsedUserCompanyName {
				visitorCondition := s.matchPhraseConverter("user_company", companyName)
				if visitorCondition != "" {
					visitorConditions = append(visitorConditions, visitorCondition)
				}
			}
		}
	}

	var speakerUserIdConditions []string
	var visitorUserIdConditions []string
	if isUserEntity && len(filterFields.ParsedUserId) > 0 {
		escapeSqlValue := func(value string) string {
			return strings.ReplaceAll(value, "'", "''")
		}
		userIds := make([]string, 0, len(filterFields.ParsedUserId))
		for _, userId := range filterFields.ParsedUserId {
			userIds = append(userIds, escapeSqlValue(userId))
		}
		userIdCondition := fmt.Sprintf("user_id IN (%s)", strings.Join(userIds, ","))

		if hasSpeaker {
			speakerUserIdConditions = append(speakerUserIdConditions, userIdCondition)
		}
		if hasVisitor {
			visitorUserIdConditions = append(visitorUserIdConditions, userIdCondition)
		}
	}

	// Prepare companyId flag for ID-based queries
	var hasCompanyIdQueries bool
	if hasCompanyId && len(filterFields.ParsedCompanyId) > 0 {
		// Set flags to indicate we have companyId queries
		if hasExhibitor || hasSponsor || hasOrganizer || hasVisitor || hasSpeaker {
			hasCompanyIdQueries = true
		}
	}

	type queryResult struct {
		EntityType string
		Data       map[uint32][]string
		Err        error
	}

	queryChan := make(chan queryResult, 5)

	if hasExhibitor && len(exhibitorConditions) > 0 {
		go func() {
			exhibitorWhereClause := fmt.Sprintf("(%s)", strings.Join(exhibitorConditions, " OR "))
			hasBothNameAndWebsite := hasCompanyName && hasCompanyWebsite
			var selectField string
			if hasBothNameAndWebsite {
				selectField = "company_id_name as company_name, company_website"
			} else if hasCompanyWebsite {
				selectField = "company_website as company_name"
			} else {
				selectField = "company_id_name as company_name"
			}

			exhibitorQuery := fmt.Sprintf(`
				SELECT DISTINCT event_id, %s
				FROM testing_db.event_exhibitor_ch
				WHERE event_id IN (%s)
				AND %s
			`, selectField, eventIdsStrJoined, exhibitorWhereClause)
			log.Printf("Exhibitor query: %s", exhibitorQuery)

			rows, err := s.clickhouseService.ExecuteQuery(ctx, exhibitorQuery)
			if err != nil {
				queryChan <- queryResult{EntityType: "exhibitor", Data: nil, Err: err}
				return
			}
			defer rows.Close()

			data := make(map[uint32][]string)
			for rows.Next() {
				var eventID uint32
				var companyName string
				var companyWebsite string

				if hasBothNameAndWebsite {
					if err := rows.Scan(&eventID, &companyName, &companyWebsite); err != nil {
						log.Printf("Error scanning exhibitor row: %v", err)
						continue
					}
					if companyName != "" {
						entityQualification := fmt.Sprintf("exhibitor_%s", companyName)
						data[eventID] = append(data[eventID], entityQualification)
					}
					if companyWebsite != "" {
						domain := s.extractDomain(companyWebsite)
						if domain != "" {
							entityQualification := fmt.Sprintf("exhibitor_%s", domain)
							data[eventID] = append(data[eventID], entityQualification)
						}
					}
				} else if hasCompanyWebsite {
					if err := rows.Scan(&eventID, &companyWebsite); err != nil {
						log.Printf("Error scanning exhibitor row: %v", err)
						continue
					}
					domain := s.extractDomain(companyWebsite)
					if domain != "" {
						entityQualification := fmt.Sprintf("exhibitor_%s", domain)
						data[eventID] = append(data[eventID], entityQualification)
					}
				} else {
					if err := rows.Scan(&eventID, &companyName); err != nil {
						log.Printf("Error scanning exhibitor row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("exhibitor_%s", companyName)
					data[eventID] = append(data[eventID], entityQualification)
				}
			}

			if err := rows.Err(); err != nil {
				queryChan <- queryResult{EntityType: "exhibitor", Data: nil, Err: err}
				return
			}

			queryChan <- queryResult{EntityType: "exhibitor", Data: data, Err: nil}
		}()
	}

	if hasSponsor && len(sponsorConditions) > 0 {
		go func() {
			sponsorWhereClause := fmt.Sprintf("(%s)", strings.Join(sponsorConditions, " OR "))
			hasBothNameAndWebsite := hasCompanyName && hasCompanyWebsite
			var selectField string
			if hasBothNameAndWebsite {
				selectField = "company_id_name as company_name, company_website"
			} else if hasCompanyWebsite {
				selectField = "company_website as company_name"
			} else {
				selectField = "company_id_name as company_name"
			}

			sponsorQuery := fmt.Sprintf(`
				SELECT DISTINCT event_id, %s
				FROM testing_db.event_sponsors_ch
				WHERE event_id IN (%s)
				AND %s
			`, selectField, eventIdsStrJoined, sponsorWhereClause)
			log.Printf("Sponsor query: %s", sponsorQuery)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, sponsorQuery)
			if err != nil {
				queryChan <- queryResult{EntityType: "sponsor", Data: nil, Err: err}
				return
			}
			defer rows.Close()

			data := make(map[uint32][]string)
			for rows.Next() {
				var eventID uint32
				var companyName string
				var companyWebsite string

				if hasBothNameAndWebsite {
					if err := rows.Scan(&eventID, &companyName, &companyWebsite); err != nil {
						log.Printf("Error scanning sponsor row: %v", err)
						continue
					}
					if companyName != "" {
						entityQualification := fmt.Sprintf("sponsor_%s", companyName)
						data[eventID] = append(data[eventID], entityQualification)
					}
					if companyWebsite != "" {
						domain := s.extractDomain(companyWebsite)
						if domain != "" {
							entityQualification := fmt.Sprintf("sponsor_%s", domain)
							data[eventID] = append(data[eventID], entityQualification)
						}
					}
				} else if hasCompanyWebsite {
					if err := rows.Scan(&eventID, &companyWebsite); err != nil {
						log.Printf("Error scanning sponsor row: %v", err)
						continue
					}
					domain := s.extractDomain(companyWebsite)
					if domain != "" {
						entityQualification := fmt.Sprintf("sponsor_%s", domain)
						data[eventID] = append(data[eventID], entityQualification)
					}
				} else {
					if err := rows.Scan(&eventID, &companyName); err != nil {
						log.Printf("Error scanning sponsor row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("sponsor_%s", companyName)
					data[eventID] = append(data[eventID], entityQualification)
				}
			}

			if err := rows.Err(); err != nil {
				queryChan <- queryResult{EntityType: "sponsor", Data: nil, Err: err}
				return
			}

			queryChan <- queryResult{EntityType: "sponsor", Data: data, Err: nil}
		}()
	}

	if hasOrganizer && len(organizerConditions) > 0 {
		go func() {
			organizerWhereClause := fmt.Sprintf("(%s)", strings.Join(organizerConditions, " OR "))
			hasBothNameAndWebsite := hasCompanyName && hasCompanyWebsite
			var selectField string
			if hasBothNameAndWebsite {
				selectField = "company_name, company_website"
			} else if hasCompanyWebsite {
				selectField = "company_website as company_name"
			} else {
				selectField = "company_name"
			}

			organizerQuery := fmt.Sprintf(`
				SELECT DISTINCT event_id, %s
				FROM testing_db.allevent_ch
				WHERE event_id IN (%s)
				AND %s
			`, selectField, eventIdsStrJoined, organizerWhereClause)
			log.Printf("Organizer query: %s", organizerQuery)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, organizerQuery)
			if err != nil {
				queryChan <- queryResult{EntityType: "organizer", Data: nil, Err: err}
				return
			}
			defer rows.Close()

			data := make(map[uint32][]string)
			for rows.Next() {
				var eventID uint32
				var companyName string
				var companyWebsite string

				if hasBothNameAndWebsite {
					if err := rows.Scan(&eventID, &companyName, &companyWebsite); err != nil {
						log.Printf("Error scanning organizer row: %v", err)
						continue
					}
					if companyName != "" {
						entityQualification := fmt.Sprintf("organizer_%s", companyName)
						data[eventID] = append(data[eventID], entityQualification)
					}
					if companyWebsite != "" {
						domain := s.extractDomain(companyWebsite)
						if domain != "" {
							entityQualification := fmt.Sprintf("organizer_%s", domain)
							data[eventID] = append(data[eventID], entityQualification)
						}
					}
				} else if hasCompanyWebsite {
					if err := rows.Scan(&eventID, &companyWebsite); err != nil {
						log.Printf("Error scanning organizer row: %v", err)
						continue
					}
					domain := s.extractDomain(companyWebsite)
					if domain != "" {
						entityQualification := fmt.Sprintf("organizer_%s", domain)
						data[eventID] = append(data[eventID], entityQualification)
					}
				} else {
					if err := rows.Scan(&eventID, &companyName); err != nil {
						log.Printf("Error scanning organizer row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("organizer_%s", companyName)
					data[eventID] = append(data[eventID], entityQualification)
				}
			}

			if err := rows.Err(); err != nil {
				queryChan <- queryResult{EntityType: "organizer", Data: nil, Err: err}
				return
			}

			queryChan <- queryResult{EntityType: "organizer", Data: data, Err: nil}
		}()
	}

	if hasVisitor && (len(visitorConditions) > 0 || len(visitorUserIdConditions) > 0) {
		go func() {
			var whereClauses []string
			if len(visitorConditions) > 0 {
				whereClauses = append(whereClauses, fmt.Sprintf("(%s)", strings.Join(visitorConditions, " OR ")))
			}
			if len(visitorUserIdConditions) > 0 {
				whereClauses = append(whereClauses, strings.Join(visitorUserIdConditions, " AND "))
			}
			visitorWhereClause := strings.Join(whereClauses, " AND ")

			isFilteringByUserName := (isUserEntity || isSpeakerEntity) && len(filterFields.ParsedUserName) > 0
			isFilteringByUserId := isUserEntity && len(filterFields.ParsedUserId) > 0
			hasCompanyNameConditions := (hasCompanyName || hasUserCompanyName) && len(visitorConditions) > 0
			hasBothNameAndCompanyConditions := isFilteringByUserName && hasCompanyNameConditions
			selectBothFields := isUserEntity && hasBothNameAndCompanyConditions && !isFilteringByUserId

			var selectField string
			var entityQualificationPrefix string
			if isFilteringByUserId {
				selectField = "user_id as person_id"
				entityQualificationPrefix = "visitor_"
			} else if selectBothFields {
				selectField = "user_name as person_name, user_company as company_name"
				entityQualificationPrefix = "visitor_"
			} else if isFilteringByUserName {
				selectField = "user_name as person_name"
				entityQualificationPrefix = "visitor_"
			} else {
				selectField = "user_company as company_name"
				entityQualificationPrefix = "visitor_"
			}

			visitorQuery := fmt.Sprintf(`
				SELECT DISTINCT event_id, %s
				FROM testing_db.event_visitors_ch
				WHERE event_id IN (%s)
				AND %s
			`, selectField, eventIdsStrJoined, visitorWhereClause)
			log.Printf("Visitor query: %s", visitorQuery)

			rows, err := s.clickhouseService.ExecuteQuery(ctx, visitorQuery)
			if err != nil {
				queryChan <- queryResult{EntityType: "visitor", Data: nil, Err: err}
				return
			}
			defer rows.Close()

			data := make(map[uint32][]string)
			for rows.Next() {
				var eventID uint32
				var value string

				if isFilteringByUserId {
					var userId uint32
					if err := rows.Scan(&eventID, &userId); err != nil {
						log.Printf("Error scanning visitor row: %v", err)
						continue
					}
					value = fmt.Sprintf("%d", userId)
					entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, value)
					data[eventID] = append(data[eventID], entityQualification)
				} else if selectBothFields {
					var personName string
					var companyName string
					if err := rows.Scan(&eventID, &personName, &companyName); err != nil {
						log.Printf("Error scanning visitor row: %v", err)
						continue
					}
					if personName != "" && companyName != "" {
						entityQualification := fmt.Sprintf("visitor_%s_%s", personName, companyName)
						data[eventID] = append(data[eventID], entityQualification)
					} else if personName != "" {
						entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, personName)
						data[eventID] = append(data[eventID], entityQualification)
					} else if companyName != "" {
						entityQualification := fmt.Sprintf("visitor_%s", companyName)
						data[eventID] = append(data[eventID], entityQualification)
					}
				} else {
					if err := rows.Scan(&eventID, &value); err != nil {
						log.Printf("Error scanning visitor row: %v", err)
						continue
					}
					if isFilteringByUserName {
						entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, value)
						data[eventID] = append(data[eventID], entityQualification)
					} else {
						entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, value)
						data[eventID] = append(data[eventID], entityQualification)
					}
				}
			}

			if err := rows.Err(); err != nil {
				queryChan <- queryResult{EntityType: "visitor", Data: nil, Err: err}
				return
			}

			queryChan <- queryResult{EntityType: "visitor", Data: data, Err: nil}
		}()
	}

	if hasSpeaker && (len(speakerConditions) > 0 || len(speakerUserIdConditions) > 0) {
		go func() {
			var whereClauses []string
			if len(speakerConditions) > 0 {
				whereClauses = append(whereClauses, fmt.Sprintf("(%s)", strings.Join(speakerConditions, " OR ")))
			}
			if len(speakerUserIdConditions) > 0 {
				whereClauses = append(whereClauses, strings.Join(speakerUserIdConditions, " AND "))
			}
			speakerWhereClause := strings.Join(whereClauses, " AND ")

			isFilteringByUserId := isUserEntity && len(filterFields.ParsedUserId) > 0
			isFilteringByUserName := (isSpeakerEntity || isUserEntity) && len(filterFields.ParsedUserName) > 0
			// Check if company name conditions exist (from either ParsedCompanyName or ParsedUserCompanyName)
			hasCompanyNameConditions := (hasCompanyName || hasUserCompanyName) && len(speakerConditions) > 0
			// Check if there are both user name and company name conditions
			hasBothNameAndCompanyConditions := isFilteringByUserName && hasCompanyNameConditions

			// When searchByEntity=user and both userName and companyName exist, select both fields
			selectBothFields := isUserEntity && hasBothNameAndCompanyConditions && !isFilteringByUserId

			var selectField string
			var entityQualificationPrefix string
			if isFilteringByUserId {
				selectField = "user_id as person_id"
				entityQualificationPrefix = "speaker_"
			} else if selectBothFields {
				selectField = "user_name as person_name, user_company as company_name"
				entityQualificationPrefix = "speaker_"
			} else {
				selectField = "user_name as person_name"
				entityQualificationPrefix = "speaker_"
			}

			speakerQuery := fmt.Sprintf(`
				SELECT DISTINCT event_id, %s
				FROM testing_db.event_speaker_ch
				WHERE event_id IN (%s)
				AND %s
			`, selectField, eventIdsStrJoined, speakerWhereClause)
			log.Printf("Speaker query: %s", speakerQuery)

			rows, err := s.clickhouseService.ExecuteQuery(ctx, speakerQuery)
			if err != nil {
				queryChan <- queryResult{EntityType: "speaker", Data: nil, Err: err}
				return
			}
			defer rows.Close()

			data := make(map[uint32][]string)
			for rows.Next() {
				var eventID uint32
				var value string

				if isFilteringByUserId {
					var userId uint32
					if err := rows.Scan(&eventID, &userId); err != nil {
						log.Printf("Error scanning speaker row: %v", err)
						continue
					}
					value = fmt.Sprintf("%d", userId)
					entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, value)
					data[eventID] = append(data[eventID], entityQualification)
				} else if selectBothFields {
					var personName string
					var companyName string
					if err := rows.Scan(&eventID, &personName, &companyName); err != nil {
						log.Printf("Error scanning speaker row: %v", err)
						continue
					}
					personName = strings.TrimSpace(personName)
					companyName = strings.TrimSpace(companyName)
					if personName != "" && companyName != "" {
						entityQualification := fmt.Sprintf("speaker_%s_%s", personName, companyName)
						data[eventID] = append(data[eventID], entityQualification)
					} else if personName != "" {
						entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, personName)
						data[eventID] = append(data[eventID], entityQualification)
					} else if companyName != "" {
						entityQualification := fmt.Sprintf("speaker_%s", companyName)
						data[eventID] = append(data[eventID], entityQualification)
					}
				} else {
					if err := rows.Scan(&eventID, &value); err != nil {
						log.Printf("Error scanning speaker row: %v", err)
						continue
					}
					value = strings.TrimSpace(value)
					entityQualification := fmt.Sprintf("%s%s", entityQualificationPrefix, value)
					data[eventID] = append(data[eventID], entityQualification)
				}
			}

			if err := rows.Err(); err != nil {
				queryChan <- queryResult{EntityType: "speaker", Data: nil, Err: err}
				return
			}

			queryChan <- queryResult{EntityType: "speaker", Data: data, Err: nil}
		}()
	}

	// Check if we have name-based conditions (companyId queries should only run if no name-based conditions exist)
	hasNameBasedConditions := len(exhibitorConditions) > 0 || len(sponsorConditions) > 0 || len(organizerConditions) > 0 || len(visitorConditions) > 0 || len(speakerConditions) > 0 || len(speakerUserIdConditions) > 0 || len(visitorUserIdConditions) > 0

	// Execute companyId-based queries (only if no name-based conditions exist)
	if hasCompanyIdQueries && !hasNameBasedConditions && len(filterFields.ParsedCompanyId) > 0 {
		escapeSqlValue := func(value string) string {
			return strings.ReplaceAll(value, "'", "''")
		}
		companyIds := make([]string, 0, len(filterFields.ParsedCompanyId))
		for _, companyId := range filterFields.ParsedCompanyId {
			escaped := escapeSqlValue(companyId)
			companyIds = append(companyIds, fmt.Sprintf("'%s'", escaped))
		}
		companyIdCondition := fmt.Sprintf("company_id IN (%s)", strings.Join(companyIds, ","))
		userCompanyIdCondition := fmt.Sprintf("user_company_id IN (%s)", strings.Join(companyIds, ","))
		companyIdValue := filterFields.ParsedCompanyId[0]

		if hasExhibitor {
			go func() {
				exhibitorQuery := fmt.Sprintf(`
					SELECT DISTINCT event_id
					FROM testing_db.event_exhibitor_ch
					WHERE event_id IN (%s)
					AND %s
					AND published IN (1, 2)
				`, eventIdsStrJoined, companyIdCondition)
				log.Printf("Exhibitor companyId query: %s", exhibitorQuery)

				rows, err := s.clickhouseService.ExecuteQuery(ctx, exhibitorQuery)
				if err != nil {
					queryChan <- queryResult{EntityType: "exhibitor", Data: nil, Err: err}
					return
				}
				defer rows.Close()

				data := make(map[uint32][]string)
				for rows.Next() {
					var eventID uint32
					if err := rows.Scan(&eventID); err != nil {
						log.Printf("Error scanning exhibitor companyId row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("exhibitor_%s", companyIdValue)
					data[eventID] = append(data[eventID], entityQualification)
				}

				if err := rows.Err(); err != nil {
					queryChan <- queryResult{EntityType: "exhibitor", Data: nil, Err: err}
					return
				}

				queryChan <- queryResult{EntityType: "exhibitor", Data: data, Err: nil}
			}()
		}

		if hasSponsor {
			go func() {
				sponsorQuery := fmt.Sprintf(`
					SELECT DISTINCT event_id
					FROM testing_db.event_sponsors_ch
					WHERE event_id IN (%s)
					AND %s
					AND published IN (1, 2)
				`, eventIdsStrJoined, companyIdCondition)
				log.Printf("Sponsor companyId query: %s", sponsorQuery)

				rows, err := s.clickhouseService.ExecuteQuery(ctx, sponsorQuery)
				if err != nil {
					queryChan <- queryResult{EntityType: "sponsor", Data: nil, Err: err}
					return
				}
				defer rows.Close()

				data := make(map[uint32][]string)
				for rows.Next() {
					var eventID uint32
					if err := rows.Scan(&eventID); err != nil {
						log.Printf("Error scanning sponsor companyId row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("sponsor_%s", companyIdValue)
					data[eventID] = append(data[eventID], entityQualification)
				}

				if err := rows.Err(); err != nil {
					queryChan <- queryResult{EntityType: "sponsor", Data: nil, Err: err}
					return
				}

				queryChan <- queryResult{EntityType: "sponsor", Data: data, Err: nil}
			}()
		}

		if hasOrganizer {
			go func() {
				editionTypeCondition := s.buildEditionTypeCondition(filterFields, "")
				organizerQuery := fmt.Sprintf(`
					SELECT DISTINCT event_id
					FROM testing_db.allevent_ch
					WHERE event_id IN (%s)
					AND %s
					AND %s
				`, eventIdsStrJoined, companyIdCondition, editionTypeCondition)
				log.Printf("Organizer companyId query: %s", organizerQuery)

				rows, err := s.clickhouseService.ExecuteQuery(ctx, organizerQuery)
				if err != nil {
					queryChan <- queryResult{EntityType: "organizer", Data: nil, Err: err}
					return
				}
				defer rows.Close()

				data := make(map[uint32][]string)
				for rows.Next() {
					var eventID uint32
					if err := rows.Scan(&eventID); err != nil {
						log.Printf("Error scanning organizer companyId row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("organizer_%s", companyIdValue)
					data[eventID] = append(data[eventID], entityQualification)
				}

				if err := rows.Err(); err != nil {
					queryChan <- queryResult{EntityType: "organizer", Data: nil, Err: err}
					return
				}

				queryChan <- queryResult{EntityType: "organizer", Data: data, Err: nil}
			}()
		}

		if hasVisitor {
			go func() {
				visitorQuery := fmt.Sprintf(`
					SELECT DISTINCT event_id
					FROM testing_db.event_visitors_ch
					WHERE event_id IN (%s)
					AND %s
					AND published IN (1, 2)
				`, eventIdsStrJoined, userCompanyIdCondition)
				log.Printf("Visitor companyId query: %s", visitorQuery)

				rows, err := s.clickhouseService.ExecuteQuery(ctx, visitorQuery)
				if err != nil {
					queryChan <- queryResult{EntityType: "visitor", Data: nil, Err: err}
					return
				}
				defer rows.Close()

				data := make(map[uint32][]string)
				for rows.Next() {
					var eventID uint32
					if err := rows.Scan(&eventID); err != nil {
						log.Printf("Error scanning visitor companyId row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("visitor_%s", companyIdValue)
					data[eventID] = append(data[eventID], entityQualification)
				}

				if err := rows.Err(); err != nil {
					queryChan <- queryResult{EntityType: "visitor", Data: nil, Err: err}
					return
				}

				queryChan <- queryResult{EntityType: "visitor", Data: data, Err: nil}
			}()
		}

		if hasSpeaker {
			go func() {
				speakerQuery := fmt.Sprintf(`
					SELECT DISTINCT event_id
					FROM testing_db.event_speaker_ch
					WHERE event_id IN (%s)
					AND %s
					AND published IN (1, 2)
				`, eventIdsStrJoined, userCompanyIdCondition)
				log.Printf("Speaker companyId query: %s", speakerQuery)

				rows, err := s.clickhouseService.ExecuteQuery(ctx, speakerQuery)
				if err != nil {
					queryChan <- queryResult{EntityType: "speaker", Data: nil, Err: err}
					return
				}
				defer rows.Close()

				data := make(map[uint32][]string)
				for rows.Next() {
					var eventID uint32
					if err := rows.Scan(&eventID); err != nil {
						log.Printf("Error scanning speaker companyId row: %v", err)
						continue
					}
					entityQualification := fmt.Sprintf("speaker_%s", companyIdValue)
					data[eventID] = append(data[eventID], entityQualification)
				}

				if err := rows.Err(); err != nil {
					queryChan <- queryResult{EntityType: "speaker", Data: nil, Err: err}
					return
				}

				queryChan <- queryResult{EntityType: "speaker", Data: data, Err: nil}
			}()
		}
	}

	queryCount := 0
	if hasExhibitor && len(exhibitorConditions) > 0 {
		queryCount++
	}
	if hasSponsor && len(sponsorConditions) > 0 {
		queryCount++
	}
	if hasOrganizer && len(organizerConditions) > 0 {
		queryCount++
	}
	if hasVisitor && (len(visitorConditions) > 0 || len(visitorUserIdConditions) > 0) {
		queryCount++
	}
	if hasSpeaker && (len(speakerConditions) > 0 || len(speakerUserIdConditions) > 0) {
		queryCount++
	}
	// Count companyId queries (only if no name-based conditions exist)
	if hasCompanyIdQueries && !hasNameBasedConditions {
		if hasExhibitor {
			queryCount++
		}
		if hasSponsor {
			queryCount++
		}
		if hasOrganizer {
			queryCount++
		}
		if hasVisitor {
			queryCount++
		}
		if hasSpeaker {
			queryCount++
		}
	}

	for i := 0; i < queryCount; i++ {
		queryRes := <-queryChan
		if queryRes.Err != nil {
			log.Printf("Error fetching %s entity qualifications: %v", queryRes.EntityType, queryRes.Err)
			continue
		}

		for eventID, qualifications := range queryRes.Data {
			result[eventID] = append(result[eventID], qualifications...)
		}
	}

	return result, nil
}

func (s *SharedFunctionService) getTrackerMatchInfo(
	eventEntityQualifications map[uint32][]string,
	searchByEntity string,
	filterFields models.FilterDataDto,
) (map[uint32]map[string]interface{}, error) {
	result := make(map[uint32]map[string]interface{})

	if len(eventEntityQualifications) == 0 {
		return result, nil
	}

	searchByEntityLower := strings.ToLower(strings.TrimSpace(searchByEntity))

	switch searchByEntityLower {
	case "company":
		return s.getTrackerMatchInfoForCompany(eventEntityQualifications), nil
	case "visitor", "exhibitor", "sponsor", "organizer":
		return s.getTrackerMatchInfoForCompany(eventEntityQualifications), nil
	case "speaker":
		return s.getTrackerMatchInfoForSpeaker(eventEntityQualifications, filterFields), nil
	case "user":
		return s.getTrackerMatchInfoForUser(eventEntityQualifications, filterFields), nil
	default:
		return result, nil
	}
}

func (s *SharedFunctionService) getTrackerMatchInfoForCompany(
	eventEntityQualifications map[uint32][]string,
) map[uint32]map[string]interface{} {
	result := make(map[uint32]map[string]interface{})

	priorityVal := []string{
		"organizer",
		"exhibitor",
		"sponsor",
		"speaker",
		"visitor",
	}

	priority := map[string]string{
		"organizer": "High",
		"exhibitor": "Medium",
		"sponsor":   "Medium",
		"speaker":   "Medium",
		"visitor":   "Low",
	}

	for eventID, fromData := range eventEntityQualifications {
		if len(fromData) == 0 {
			continue
		}

		var association string
		var associationLevel string
		domain := make([]string, 0)

		for _, priorityEntity := range priorityVal {
			for _, dataItem := range fromData {
				parts := strings.Split(dataItem, "_")
				if len(parts) > 0 && parts[0] == priorityEntity {
					association = priorityEntity
					associationLevel = priority[priorityEntity]
					break
				}
			}
			if association != "" {
				break
			}
		}

		domainSet := make(map[string]bool)
		for _, dataItem := range fromData {
			if !domainSet[dataItem] {
				domain = append(domain, dataItem)
				domainSet[dataItem] = true
			}
		}

		result[eventID] = map[string]interface{}{
			"competitorDomainOrName": domain,
			"competitorAssociation":  association,
			"competitorPercentage":   associationLevel,
		}
	}

	return result
}

func (s *SharedFunctionService) getTrackerMatchInfoForSpeaker(
	eventEntityQualifications map[uint32][]string,
	filterFields models.FilterDataDto,
) map[uint32]map[string]interface{} {
	result := make(map[uint32]map[string]interface{})

	orgPerson := make([]string, 0)
	if len(filterFields.ParsedUserName) > 0 {
		orgPersonSet := make(map[string]bool)
		for _, userName := range filterFields.ParsedUserName {
			userNameLower := strings.ToLower(strings.TrimSpace(userName))
			if userNameLower != "" && !orgPersonSet[userNameLower] {
				orgPerson = append(orgPerson, userNameLower)
				orgPersonSet[userNameLower] = true
			}
		}
	}

	if len(orgPerson) == 0 {
		for eventID := range eventEntityQualifications {
			result[eventID] = map[string]interface{}{
				"speakerPersonAssociation":           []string{},
				"speakerPersonAssociationPercentage": "",
			}
		}
		return result
	}

	for eventID, fromData := range eventEntityQualifications {
		uniqueSpeakers := make(map[string]string)
		speakerPersonAssociation := make([]string, 0)
		speakerNamesForMatching := make([]string, 0)

		for _, val := range fromData {
			if strings.HasPrefix(val, "speaker_") {
				personName := strings.TrimPrefix(val, "speaker_")
				parts := strings.Split(personName, "_")
				if len(parts) > 1 {
					personName = parts[0]
				}
				lowerCaseVal := strings.ToLower(personName)

				if _, exists := uniqueSpeakers[lowerCaseVal]; !exists {
					speakerNamesForMatching = append(speakerNamesForMatching, personName)
					uniqueSpeakers[lowerCaseVal] = personName
					speakerPersonAssociation = append(speakerPersonAssociation, val)
				}
			}
		}

		countMatched := 0
		for _, personData := range speakerNamesForMatching {
			personDataLower := strings.ToLower(personData)
			personDataParts := strings.Split(personDataLower, "_")
			if len(personDataParts) > 0 {
				personDataToMatch := personDataParts[0]
				for _, orgPersonName := range orgPerson {
					if strings.Contains(orgPersonName, personDataToMatch) {
						countMatched++
					}
				}
			}
		}

		var matchType string
		if len(orgPerson) > 0 {
			match := float64(countMatched) / float64(len(orgPerson)) * 100.0
			if match > 20 {
				matchType = "High"
			} else if match >= 10 && match <= 20 {
				matchType = "Medium"
			} else if match > 0 && match < 10 {
				matchType = "Low"
			} else {
				matchType = ""
			}
		}

		result[eventID] = map[string]interface{}{
			"speakerPersonAssociation":           speakerPersonAssociation,
			"speakerPersonAssociationPercentage": matchType,
		}
	}

	return result
}

func (s *SharedFunctionService) getTrackerMatchInfoForUser(
	eventEntityQualifications map[uint32][]string,
	filterFields models.FilterDataDto,
) map[uint32]map[string]interface{} {
	result := make(map[uint32]map[string]interface{})

	orgPerson := make([]string, 0)
	if len(filterFields.ParsedUserId) > 0 {
		for _, userId := range filterFields.ParsedUserId {
			orgPerson = append(orgPerson, strings.ToLower(strings.TrimSpace(userId)))
		}
	} else if len(filterFields.ParsedUserName) > 0 {
		orgPersonSet := make(map[string]bool)
		for _, userName := range filterFields.ParsedUserName {
			userNameLower := strings.ToLower(strings.TrimSpace(userName))
			if userNameLower != "" && !orgPersonSet[userNameLower] {
				orgPerson = append(orgPerson, userNameLower)
				orgPersonSet[userNameLower] = true
			}
		}
	}

	if len(orgPerson) == 0 {
		for eventID := range eventEntityQualifications {
			result[eventID] = map[string]interface{}{
				"peopleFinderPersonAssociation":           []string{},
				"peopleFinderPersonAssociationPercentage": "",
			}
		}
		return result
	}

	isMatchingByUserId := len(filterFields.ParsedUserId) > 0

	for eventID, fromData := range eventEntityQualifications {
		uniquePeopleFinder := make(map[string]string)
		peopleFinderPersonAssociation := make([]string, 0)
		valuesForMatching := make([]string, 0)

		for _, val := range fromData {
			lowerCaseProcessed := strings.ToLower(val)
			if _, exists := uniquePeopleFinder[lowerCaseProcessed]; !exists {
				uniquePeopleFinder[lowerCaseProcessed] = val
				peopleFinderPersonAssociation = append(peopleFinderPersonAssociation, val)

				parts := strings.Split(val, "_")
				var personValue string

				if strings.HasPrefix(val, "speaker_") {
					if len(parts) >= 3 {
						personValue = parts[1]
					} else if len(parts) >= 2 {
						personValue = strings.TrimPrefix(val, "speaker_")
					}
				} else if strings.HasPrefix(val, "visitor_") {
					if len(parts) >= 3 {
						personValue = parts[1]
					} else if len(parts) >= 2 {
						personValue = strings.TrimPrefix(val, "visitor_")
					}
				} else {
					processedValue := strings.Replace(val, "user_", "", 1)
					if strings.HasPrefix(processedValue, "speaker_") {
						personValue = strings.TrimPrefix(processedValue, "speaker_")
					} else if strings.HasPrefix(processedValue, "visitor_") {
						personValue = strings.TrimPrefix(processedValue, "visitor_")
					} else {
						personValue = processedValue
					}
				}

				if personValue != "" {
					valuesForMatching = append(valuesForMatching, personValue)
				}
			}
		}

		countMatched := 0
		if isMatchingByUserId {
			for _, personId := range valuesForMatching {
				personIdLower := strings.ToLower(personId)
				for _, orgPersonId := range orgPerson {
					if strings.ToLower(orgPersonId) == personIdLower {
						countMatched++
						break
					}
				}
			}
		} else {
			for _, personData := range valuesForMatching {
				personDataLower := strings.ToLower(personData)
				for _, orgPersonName := range orgPerson {
					if strings.Contains(orgPersonName, personDataLower) || strings.Contains(personDataLower, orgPersonName) {
						countMatched++
						break
					}
				}
			}
		}

		matchType := ""
		if len(orgPerson) > 0 {
			match := float64(countMatched) / float64(len(orgPerson)) * 100.0
			if match > 20 {
				matchType = "High"
			} else if match >= 10 {
				matchType = "Medium"
			} else if match > 0 {
				matchType = "Low"
			}
		}

		result[eventID] = map[string]interface{}{
			"peopleFinderPersonAssociation":           peopleFinderPersonAssociation,
			"peopleFinderPersonAssociationPercentage": matchType,
		}
	}

	return result
}
