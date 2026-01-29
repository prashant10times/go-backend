package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"reflect"
	"search-event-go/config"
	"search-event-go/middleware"
	"search-event-go/models"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type SearchEventService struct {
	db                    *gorm.DB
	sharedFunctionService *SharedFunctionService
	transformDataService  *TransformDataService
	clickhouseService     *ClickHouseService
	cfg                   *config.Config
}

type ListResult struct {
	Data         interface{}
	Count        int
	StatusCode   int
	ErrorMessage string
}

type AggregationResult struct {
	StatusCode int
	Response   interface{}
	Error      error
}

type FieldSelectionContext struct {
	Processor          *ShowValuesProcessor
	FieldsSelector     *BaseFieldsSelector
	BaseFields         []string
	BaseFieldMap       map[string]bool
	DBColumnToAliasMap map[string]string
	DBToAliasMap       map[string]string
}

type EventFilterFields struct {
	SelectFields  []string
	GroupByFields []string
	OrderBy       string
}

type QueryComponents struct {
	CTEClausesStr         string
	EventFilterSelectStr  string
	EventFilterGroupByStr string
	EventFilterOrderBy    string
	FieldsString          string
	FinalGroupByClause    string
	InnerOrderBy          string
	FinalOrderByClause    string
	JoinConditionsStr     string
	HasEndDateFilters     bool
}

type QueryResults struct {
	EventDataRows driver.Rows
	TotalCount    int
	EventDataErr  error
	CountErr      error
}

type RelatedData struct {
	CategoriesMap        map[string][]map[string]string
	TagsMap              map[string][]map[string]string
	TypesMap             map[string][]map[string]string
	JobCompositeMap      map[string][]map[string]string
	CountrySpreadMap     map[string][]map[string]interface{}
	DesignationSpreadMap map[string][]map[string]interface{}
	RankingsMap          map[string]string
	EventLocationsMap    map[string]map[string]interface{}
}

func NewSearchEventService(db *gorm.DB, sharedFunctionService *SharedFunctionService, transformDataService *TransformDataService, clickhouseService *ClickHouseService, cfg *config.Config) *SearchEventService {
	return &SearchEventService{
		db:                    db,
		sharedFunctionService: sharedFunctionService,
		transformDataService:  transformDataService,
		clickhouseService:     clickhouseService,
		cfg:                   cfg,
	}
}

func getMatchedKeywords(keywordsString string, includeKeywords []string) (map[string]interface{}, error) {
	if keywordsString == "" || len(includeKeywords) == 0 {
		return map[string]interface{}{
			"matchedKeywords":           []string{},
			"matchedKeywordsPercentage": 0.0,
		}, nil
	}

	keywordsSet := make(map[string]bool)
	keywordsLower := strings.ToLower(keywordsString)
	keywordParts := strings.Fields(keywordsLower)
	for _, keyword := range keywordParts {
		if keyword != "" {
			keywordsSet[keyword] = true
		}
	}

	matchedKeywordsCount := 0
	matchedKeywords := []string{}

	unescapeSqlLikePattern := func(s string) string {
		s = strings.ReplaceAll(s, "\\%", "%")
		s = strings.ReplaceAll(s, "\\_", "_")
		return s
	}

	for _, keyword := range includeKeywords {
		keyword = unescapeSqlLikePattern(keyword)
		keywordLower := strings.ToLower(keyword)

		if strings.Contains(keyword, "_") {
			parts := strings.Split(keywordLower, "_")
			if len(parts) == 2 {
				if keywordsSet[parts[0]] && keywordsSet[parts[1]] {
					matchedKeywordsCount++
					matchedKeywords = append(matchedKeywords, keyword)
				}
			}
		} else if strings.Contains(keyword, " ") {
			subSpaceKeywords := strings.Fields(keywordLower)
			allMatched := true
			for _, subKeyword := range subSpaceKeywords {
				if !keywordsSet[subKeyword] {
					allMatched = false
					break
				}
			}
			if allMatched {
				matchedKeywordsCount++
				matchedKeywords = append(matchedKeywords, keyword)
			}
		} else {
			if keywordsSet[keywordLower] {
				matchedKeywordsCount++
				matchedKeywords = append(matchedKeywords, keyword)
			}
		}
	}

	matchedKeywordsPercentage := 0.0
	if len(includeKeywords) > 0 {
		matchedKeywordsPercentage = float64(matchedKeywordsCount) / float64(len(includeKeywords)) * 100
		matchedKeywordsPercentage = math.Round(matchedKeywordsPercentage*100) / 100
	}

	return map[string]interface{}{
		"matchedKeywords":           matchedKeywords,
		"matchedKeywordsPercentage": matchedKeywordsPercentage,
	}, nil
}

func (s *SearchEventService) validateAndProcessParameterAccess(
	filterFields *models.FilterDataDto,
	basicKeys []string,
	advancedKeys []string,
	allowedAdvancedParameters []string,
	allowedFilters []string,
	byPassAccess bool,
) error {
	type ParameterConfig struct {
		ParameterName     string
		BooleanFieldName  string
		RangeFilterFields []string
		ResponseFields    []string
	}

	parameterConfigs := []ParameterConfig{
		{
			ParameterName:     "economicImpactData",
			BooleanFieldName:  "EventEstimate",
			RangeFilterFields: []string{"EconomicImpactGte", "EconomicImpactLte"},
			ResponseFields:    []string{"economicImpact", "economicImpactBreakdown"},
		},
		{
			ParameterName:     "impactScore",
			BooleanFieldName:  "ImpactScore",
			RangeFilterFields: []string{"ImpactScoreGte", "ImpactScoreLte"},
			ResponseFields:    []string{"impactScore"},
		},
	}

	if byPassAccess {
		log.Printf("User has unlimited access - automatically enabling all requested parameters")
		for _, config := range parameterConfigs {
			v := reflect.ValueOf(filterFields).Elem()
			boolField := v.FieldByName(config.BooleanFieldName)

			if boolField.IsValid() && boolField.Kind() == reflect.Bool {
				hasRangeFilter := false
				for _, filterField := range config.RangeFilterFields {
					field := v.FieldByName(filterField)
					if field.IsValid() && field.Kind() == reflect.String && field.String() != "" {
						hasRangeFilter = true
						break
					}
				}

				if boolField.Bool() || hasRangeFilter {
					boolField.SetBool(true)
				}
			}
		}
		return nil
	}

	v := reflect.ValueOf(filterFields).Elem()
	t := reflect.TypeOf(filterFields).Elem()

	booleanFieldToJSONTag := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		if fieldType.Type.Kind() == reflect.Bool {
			jsonTag := fieldType.Tag.Get("json")
			if jsonTag != "" && jsonTag != "-" {
				jsonTagName := strings.Split(jsonTag, ",")[0]
				booleanFieldToJSONTag[fieldType.Name] = jsonTagName
			}
		}
	}

	var unauthorizedParameters []string
	for _, config := range parameterConfigs {
		boolField := v.FieldByName(config.BooleanFieldName)

		if boolField.IsValid() && boolField.Kind() == reflect.Bool && boolField.Bool() {
			jsonTagName, hasJSONTag := booleanFieldToJSONTag[config.BooleanFieldName]
			isFilter := hasJSONTag && slices.Contains(allowedFilters, jsonTagName)

			if isFilter {
				log.Printf("Boolean field %s (JSON tag: %s) is a valid filter - allowing request. Checking parameter access for data return.", config.BooleanFieldName, jsonTagName)
				isBasic := slices.Contains(basicKeys, config.ParameterName)
				isAdvanced := slices.Contains(advancedKeys, config.ParameterName)

				if isBasic {
					continue
				} else if isAdvanced {
					if !slices.Contains(allowedAdvancedParameters, config.ParameterName) {
						boolField.SetBool(false)
						log.Printf("User doesn't have access to parameter %s. Setting %s to false (filtering allowed, data not returned).", config.ParameterName, config.BooleanFieldName)
					}
				} else {
					log.Printf("Warning: Parameter %s requested but not found in API configuration", config.ParameterName)
					boolField.SetBool(false)
				}
			} else {
				isBasic := slices.Contains(basicKeys, config.ParameterName)
				isAdvanced := slices.Contains(advancedKeys, config.ParameterName)

				if isBasic {
					continue
				} else if isAdvanced {
					if !slices.Contains(allowedAdvancedParameters, config.ParameterName) {
						unauthorizedParameters = append(unauthorizedParameters, config.ParameterName)
					}
				} else {
					log.Printf("Warning: Parameter %s requested but not found in API configuration", config.ParameterName)
				}
			}
		}
	}

	if len(unauthorizedParameters) > 0 {
		msg := "The requested parameters are not allowed in your current plan, please upgrade your plan to access these advanced parameters: " + strings.Join(unauthorizedParameters, ", ")
		return middleware.NewForbiddenError("Unauthorized advanced parameters", msg)
	}

	return nil
}

func (s *SearchEventService) GetEventDataV2(userId, apiId string, filterFields models.FilterDataDto, pagination models.PaginationDto, responseFields models.ResponseDataDto, showValues string, c *fiber.Ctx) (any, error) {
	startTime := time.Now()
	ipAddress := c.Get("X-Real-IP")
	if ipAddress == "" {
		ipAddress = c.IP()
	}
	statusCode := 200
	var errorMessage *string

	defer func() {
		responseTime := time.Since(startTime).Seconds()

		go func() {
			err := s.sharedFunctionService.logApiUsage(userId, apiId, "search-events", responseTime, ipAddress, statusCode, filterFields, pagination, responseFields, errorMessage)
			if err != nil {
				log.Printf("Error saving API data: %v", err)
			}
			log.Printf("Total API Response Time: %f seconds", responseTime)
		}()
	}()

	var params []struct {
		ParameterName string `json:"parameter_name"`
		ParameterType string `json:"parameter_type"`
	}

	err := s.db.Model(&models.APIParameter{}).
		Select("parameter_name, parameter_type").
		Where("api_id = ? AND is_active = ?", uuid.MustParse(apiId), true).
		Find(&params).Error

	if err != nil {
		log.Printf("Error fetching API parameters: %v", err)
		return nil, err
	}

	var basicKeys []string
	var advancedKeys []string
	for _, param := range params {
		switch param.ParameterType {
		case "BASIC":
			basicKeys = append(basicKeys, param.ParameterName)
		case "ADVANCED":
			advancedKeys = append(advancedKeys, param.ParameterName)
		}
	}

	var allowedFilters []string
	var allowedAdvancedParameters []string

	result, err := s.sharedFunctionService.quotaAndFilterVerification(userId, apiId)
	if err != nil {
		log.Printf("Quota and filter verification failed: %v", err)
		statusCode = http.StatusTooManyRequests
		msg := "Daily API limit exceeded"
		errorMessage = &msg
		return nil, middleware.NewTooManyRequestsError("Daily API limit exceeded", err.Error())
	}

	allowedFilters = result.AllowedFilters
	allowedAdvancedParameters = result.AllowedAdvancedParameters

	byPassAccess := s.sharedFunctionService.ByPassAccess(userId)

	err = s.validateAndProcessParameterAccess(&filterFields, basicKeys, advancedKeys, allowedAdvancedParameters, allowedFilters, byPassAccess)
	if err != nil {
		log.Printf("Parameter access validation failed: %v", err)
		statusCode = http.StatusForbidden
		msg := err.Error()
		errorMessage = &msg
		return nil, err
	}

	var requestedFilters []string
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if len(fieldType.Name) >= 6 && fieldType.Name[:6] == "Parsed" {
			continue
		}
		if fieldType.Name == "View" || fieldType.Name == "ShowCount" || fieldType.Name == "Radius" || fieldType.Name == "Unit" || fieldType.Name == "EventDistanceOrder" || fieldType.Name == "Q" {
			continue
		}
		if field.Kind() == reflect.String && field.String() != "" {
			jsonTag := fieldType.Tag.Get("json")
			if jsonTag != "" && jsonTag != "-" {
				filterName := strings.Split(jsonTag, ",")[0]
				requestedFilters = append(requestedFilters, filterName)
			}
		}
	}

	var unauthorizedFilters []string
	for _, filter := range requestedFilters {
		if !slices.Contains(allowedFilters, filter) {
			unauthorizedFilters = append(unauthorizedFilters, filter)
		}
	}

	if len(unauthorizedFilters) > 0 {
		statusCode = http.StatusForbidden
		msg := "The requested filters are not allowed in your current plan, please upgrade your plan to access these filters: " + strings.Join(unauthorizedFilters, ", ")
		errorMessage = &msg
		return nil, middleware.NewForbiddenError("Unauthorized filters", fmt.Sprintf("The requested filters are not allowed in your current plan, please upgrade your plan to access these filters: %s", strings.Join(unauthorizedFilters, ", ")))
	}

	var requestedAdvancedParameters []string
	for _, filter := range requestedFilters {
		if slices.Contains(advancedKeys, filter) {
			requestedAdvancedParameters = append(requestedAdvancedParameters, filter)
		}
	}
	var unauthorizedAdvancedParameters []string
	for _, parameter := range requestedAdvancedParameters {
		if !slices.Contains(allowedAdvancedParameters, parameter) {
			unauthorizedAdvancedParameters = append(unauthorizedAdvancedParameters, parameter)
		}
	}
	if len(unauthorizedAdvancedParameters) > 0 {
		statusCode = http.StatusForbidden
		msg := "The requested parameters are not allowed in your current plan, please upgrade your plan to access these advanced parameters: " + strings.Join(unauthorizedAdvancedParameters, ", ")
		errorMessage = &msg
		return nil, middleware.NewForbiddenError("Unauthorized advanced parameters", fmt.Sprintf("The requested parameters are not allowed in your current plan, please upgrade your plan to access these advanced parameters: %s", strings.Join(unauthorizedAdvancedParameters, ", ")))
	}

	var selectedAdvancedKeys []string
	for _, parameter := range requestedAdvancedParameters {
		if slices.Contains(allowedAdvancedParameters, parameter) {
			selectedAdvancedKeys = append(selectedAdvancedKeys, parameter)
		}
	}

	var requiredFields []string
	requiredFields = append(requiredFields, basicKeys...)
	requiredFields = append(requiredFields, selectedAdvancedKeys...)

	sortClause, err := s.transformDataService.ParseSortFields(pagination.Sort, &filterFields)
	if err != nil {
		log.Printf("Error parsing sort fields: %v", err)
		statusCode = http.StatusBadRequest
		msg := err.Error()
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid sort fields", err.Error())
	}

	//original api Logic
	validatedParameters, err := s.sharedFunctionService.validateParameters(filterFields)
	if err != nil {
		log.Printf("Error validating parameters: %v", err)
		statusCode = http.StatusBadRequest
		msg := err.Error()
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid parameters", err.Error())
	}

	log.Printf("Validated parameters: %v", validatedParameters)

	eventTypeArray := strings.Split(filterFields.EventTypes, ",")
	if filterFields.EventTypes != "" && len(eventTypeArray) == 1 && eventTypeArray[0] == s.cfg.AlertId {
		baseParams := models.AlertSearchParams{
			EventIds:    validatedParameters.ParsedEventIds,
			StartDate:   &validatedParameters.ActiveGte,
			EndDate:     &validatedParameters.ActiveLte,
			LocationIds: validatedParameters.ParsedLocationIds,
		}

		if validatedParameters.ParsedViewBound != nil {
			if validatedParameters.ParsedViewBound.BoundType == models.BoundTypePoint {
				var geoCoords models.GeoCoordinates
				if err := json.Unmarshal(validatedParameters.ParsedViewBound.Coordinates, &geoCoords); err == nil {
					baseParams.Coordinates = &geoCoords
				}
			}
		}

		type alertCountResult struct {
			Data  interface{}
			Error error
		}

		type alertEventsResult struct {
			Data  interface{}
			Error error
		}

		countChan := make(chan alertCountResult, 1)
		alertsChan := make(chan alertEventsResult, 1)

		countParams := baseParams
		countParams.Required = "count"

		eventsParams := baseParams
		eventsParams.Required = "events"
		limit := pagination.Limit
		offset := pagination.Offset
		eventsParams.Limit = &limit
		eventsParams.Offset = &offset

		go func() {
			countData, err := s.executeAlertQuery(countParams)
			countChan <- alertCountResult{Data: countData, Error: err}
		}()

		go func() {
			alertsData, err := s.executeAlertQuery(eventsParams)
			alertsChan <- alertEventsResult{Data: alertsData, Error: err}
		}()

		countResult := <-countChan
		alertsResult := <-alertsChan

		if countResult.Error != nil || alertsResult.Error != nil {
			var errMsg string
			if countResult.Error != nil {
				errMsg = countResult.Error.Error()
			} else if alertsResult.Error != nil {
				errMsg = alertsResult.Error.Error()
			} else {
				errMsg = "Unknown error"
			}
			statusCode = http.StatusBadRequest
			msg := errMsg
			errorMessage = &msg
			return nil, middleware.NewBadRequestError("Alert request failed", errMsg)
		}

		count := 0
		events := []interface{}{}

		if countMap, ok := countResult.Data.(map[string]interface{}); ok {
			if c, exists := countMap["count"]; exists {
				if cFloat, ok := c.(float64); ok {
					count = int(cFloat)
				} else if cInt, ok := c.(int); ok {
					count = cInt
				}
			}
		}

		if alertsMap, ok := alertsResult.Data.(map[string]interface{}); ok {
			if e, exists := alertsMap["data"]; exists {
				if eventsSlice, ok := e.([]interface{}); ok {
					events = eventsSlice
				} else if eventsMapSlice, ok := e.([]map[string]interface{}); ok {
					events = make([]interface{}, len(eventsMapSlice))
					for i, event := range eventsMapSlice {
						events[i] = event
					}
				}
			}
		}

		if events == nil {
			events = []interface{}{}
		}

		responseData := fiber.Map{
			"count": count,
			"data":  events,
		}

		responseTime := time.Since(startTime).Seconds()
		successResponse := fiber.Map{
			"status":     "success",
			"statusCode": statusCode,
			"meta": fiber.Map{
				"responseTime": responseTime,
				"pagination": fiber.Map{
					"page": (pagination.Offset / pagination.Limit) + 1,
				},
			},
			"data": responseData,
		}
		return successResponse, nil
	}

	if len(filterFields.ParsedGroupBy) > 0 && filterFields.View != "trends" {
		return s.handleGroupByRequest(filterFields, startTime, &statusCode, &errorMessage)
	}

	queryType, err := s.sharedFunctionService.determineQueryType(filterFields)
	if err != nil {
		log.Printf("Error determining query type: %v", err)
		statusCode = http.StatusBadRequest
		msg := err.Error()
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid query type", err.Error())
	}

	var eventData interface{}
	var response interface{}

	switch queryType {
	case "LIST":
		result, err := s.getListData(pagination, sortClause, filterFields, showValues)
		if err != nil {
			log.Printf("Error getting list data: %v", err)
			return nil, err
		}

		if result.StatusCode != 200 {
			statusCode = http.StatusInternalServerError
			msg := result.ErrorMessage
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database query failed", result.ErrorMessage)
		}

		eventData = result.Data

		var eventDataSlice []map[string]interface{}
		if dataSlice, ok := eventData.([]map[string]interface{}); ok {
			eventDataSlice = dataSlice
		} else if dataSlice, ok := eventData.([]interface{}); ok {
			eventDataSlice = make([]map[string]interface{}, len(dataSlice))
			for i, item := range dataSlice {
				if mapItem, ok := item.(map[string]interface{}); ok {
					eventDataSlice[i] = mapItem
				} else {
					eventDataSlice[i] = make(map[string]interface{})
				}
			}
		} else {
			return nil, fmt.Errorf("unexpected data type: %T", eventData)
		}

		response, err = s.transformDataService.BuildClickhouseListViewResponse(eventDataSlice, pagination, result.Count, c)

		if err != nil {
			log.Printf("Error building response: %v", err)
			return nil, err
		}

	case "AGGREGATION":
		result, err := s.getAggregationDataClickHouse(filterFields, pagination)
		if err != nil {
			log.Printf("Error getting default aggregation data: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database aggregation query failed", err.Error())
		}

		if result.StatusCode != 200 {
			statusCode = http.StatusInternalServerError
			msg := result.Error.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database aggregation query failed", result.Error.Error())
		}

		response = result.Response

	case "MAP":
		result, err := s.getMapData(sortClause, filterFields)
		if err != nil {
			log.Printf("Error getting map data: %v", err)
			return nil, err
		}

		if result.StatusCode != 200 {
			statusCode = http.StatusInternalServerError
			msg := result.ErrorMessage
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database query failed", result.ErrorMessage)
		}

		eventData = result.Data

		var eventDataSlice []map[string]interface{}
		if dataSlice, ok := eventData.([]map[string]interface{}); ok {
			eventDataSlice = dataSlice
		} else if dataSlice, ok := eventData.([]interface{}); ok {
			eventDataSlice = make([]map[string]interface{}, len(dataSlice))
			for i, item := range dataSlice {
				if mapItem, ok := item.(map[string]interface{}); ok {
					eventDataSlice[i] = mapItem
				} else {
					eventDataSlice[i] = make(map[string]interface{})
				}
			}
		} else {
			return nil, fmt.Errorf("unexpected data type: %T", eventData)
		}

		response, err = s.transformDataService.BuildClickhouseMapViewResponse(eventDataSlice, pagination, result.Count, c)

		if err != nil {
			log.Printf("Error building map response: %v", err)
			return nil, err
		}

	case "COUNT":
		count, err := s.sharedFunctionService.getCountOnly(filterFields)
		if err != nil {
			log.Printf("Error getting count: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Count query failed", err.Error())
		}

		response = fiber.Map{
			"count": count,
		}

	case "CALENDAR":
		result, err := s.sharedFunctionService.GetCalendarEvents(filterFields)
		if err != nil {
			log.Printf("Error getting calendar events: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Calendar query failed", err.Error())
		}

		if result.StatusCode != 200 {
			statusCode = result.StatusCode
			msg := result.ErrorMessage
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Calendar query failed", result.ErrorMessage)
		}

		response = result.Data

	case "TRENDS":
		result, err := s.sharedFunctionService.GetTrendsEvents(filterFields)
		if err != nil {
			log.Printf("Error getting trends events: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Trends query failed", err.Error())
		}

		if result.StatusCode != 200 {
			statusCode = result.StatusCode
			msg := result.ErrorMessage
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Trends query failed", result.ErrorMessage)
		}

		response = result.Data

	default:
		statusCode = http.StatusBadRequest
		msg := fmt.Sprintf("Invalid query type: %s", queryType)
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid query type", fmt.Sprintf("Invalid query type: %s", queryType))
	}

	responseTime := time.Since(startTime).Seconds()
	successResponse := fiber.Map{
		"status":     "success",
		"statusCode": statusCode,
		"meta": fiber.Map{
			"responseTime": responseTime,
			"pagination": fiber.Map{
				"page": (pagination.Offset / pagination.Limit) + 1,
			},
		},
		"data": response,
	}
	return successResponse, nil
}

func (s *SearchEventService) executeAlertQuery(params models.AlertSearchParams) (interface{}, error) {
	query, err := s.sharedFunctionService.BuildAlertQuery(params)
	if err != nil {
		log.Printf("Error building alert query: %v", err)
		return nil, err
	}

	if query == "" {
		if params.Required == "count" {
			if len(params.GroupBy) > 0 {
				return map[string]interface{}{}, nil
			}
			return map[string]interface{}{"count": 0}, nil
		}
		return map[string]interface{}{"data": []interface{}{}}, nil
	}

	log.Printf("Executing alert query: %s", query)

	ctx := context.Background()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		log.Printf("Error executing alert query: %v", err)
		return nil, err
	}
	defer rows.Close()

	if params.Required == "count" {
		count := s.transformDataService.TransformAlertsCount(params, rows)
		return map[string]interface{}{"count": count}, nil
	} else {
		events := s.transformDataService.TransformAlerts(rows)
		if events == nil {
			events = []interface{}{}
		}
		return map[string]interface{}{"data": events}, nil
	}
}

func (s *SearchEventService) getListDataCount(
	queryResult *ClickHouseQueryResult,
	cteAndJoinResult *CTEAndJoinResult,
	eventFilterSelectStr string,
	eventFilterGroupByStr string,
	hasEndDateFilters bool,
	filterFields models.FilterDataDto,
) (int, error) {
	countQuery := s.sharedFunctionService.buildListDataCountQuery(
		queryResult,
		cteAndJoinResult,
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

func (s *SearchEventService) initializeFieldSelection(showValues string, filterFields models.FilterDataDto) (*FieldSelectionContext, error) {
	processor := NewShowValuesProcessor(showValues)
	fieldsSelector := NewBaseFieldsSelector(processor, filterFields)
	baseFields := fieldsSelector.GetBaseFields()
	baseFieldMap := fieldsSelector.BuildBaseFieldMap(baseFields)
	dbColumnToAliasMap := fieldsSelector.BuildDBColumnToAliasMap(baseFields)

	dbToAliasMap := map[string]string{
		"event_exhibitor":       "exhibitors",
		"event_speaker":         "speakers",
		"event_sponsor":         "sponsors",
		"event_created":         "created",
		"exhibitors_mean":       "estimatedExhibitors",
		"impactScore":           "impactScore",
		"event_economic_value":  "economicImpact",
		"event_score":           "score",
		"inboundEstimate":       "inboundAttendance",
		"internationalEstimate": "internationalAttendance",
		"event_updated":         "updated",
		"start_date":            "start",
		"end_date":              "end",
		"event_followers":       "followers",
		"event_avgRating":       "avgRating",
		"event_name":            "name",
		"event_abbr_name":       "shortName",
		"edition_city_name":     "city",
		"edition_country":       "country",
		"event_description":     "description",
		"event_logo":            "logo",
		"event_format":          "format",
		"yoyGrowth":             "yoyGrowth",
	}

	return &FieldSelectionContext{
		Processor:          processor,
		FieldsSelector:     fieldsSelector,
		BaseFields:         baseFields,
		BaseFieldMap:       baseFieldMap,
		DBColumnToAliasMap: dbColumnToAliasMap,
		DBToAliasMap:       dbToAliasMap,
	}, nil
}

func (s *SearchEventService) buildConditionalFields(ctx *FieldSelectionContext, sortClause []SortClause, filterFields models.FilterDataDto) []string {
	var conditionalFields []string
	conditionalFieldMap := make(map[string]bool)

	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			if sort.Field != "" {
				alias := ctx.DBColumnToAliasMap[sort.Field]
				if alias == "" {
					alias = ctx.DBToAliasMap[sort.Field]
				}
				if alias == "" {
					alias = sort.Field
				}

				fieldAlreadyIncluded := ctx.BaseFieldMap[sort.Field] || ctx.BaseFieldMap[alias]

				if !fieldAlreadyIncluded && !conditionalFieldMap[sort.Field] && !conditionalFieldMap[alias] {
					if sort.Field == "duration" {
						conditionalFields = append(conditionalFields, "(ee.end_date - ee.start_date) as duration")
						conditionalFieldMap["duration"] = true
					} else {
						dbColumn := sort.Field
						sortFieldToDBColumn := map[string]string{
							"start":               "start_date",
							"end":                 "end_date",
							"created":             "event_created",
							"following":           "event_followers",
							"exhibitors":          "event_exhibitor",
							"speakers":            "event_speaker",
							"sponsors":            "event_sponsor",
							"avgRating":           "event_avgRating",
							"title":               "event_name",
							"estimatedExhibitors": "exhibitors_mean",
							"score":               "event_score",
							"updated":             "event_updated",
						}
						if mappedColumn, exists := sortFieldToDBColumn[sort.Field]; exists {
							dbColumn = mappedColumn
						}

						conditionalFields = append(conditionalFields, fmt.Sprintf("ee.%s as %s", dbColumn, alias))
						conditionalFieldMap[sort.Field] = true
						conditionalFieldMap[alias] = true
					}
				}
			}
		}
	}

	if len(filterFields.ParsedAudienceZone) > 0 {
		conditionalFields = append(conditionalFields, "ee.audienceZone as audienceZone")
	}

	if filterFields.ParsedKeywords != nil && len(filterFields.ParsedKeywords.Include) > 0 {
		conditionalFields = append(conditionalFields, "arrayStringConcat(ee.keywords, ' ') as keywords")
	}

	return conditionalFields
}

func (s *SearchEventService) buildOrderByClauses(sortClause []SortClause, queryResult *ClickHouseQueryResult) (orderByClause string, eventFilterOrderBy string, err error) {
	orderByClause, err = s.sharedFunctionService.buildOrderByClause(sortClause, queryResult.NeedsAnyJoin)
	if err != nil {
		log.Printf("Error building order by clause: %v", err)
		return "", "", err
	}

	eventFilterOrderBy, err = s.sharedFunctionService.buildOrderByClause(sortClause, false)
	if err != nil {
		log.Printf("Error building event filter order by clause: %v", err)
		return "", "", err
	}

	if eventFilterOrderBy == "" {
		eventFilterOrderBy = "ORDER BY event_score ASC"
	} else {
		eventFilterOrderBy = strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
		eventFilterOrderBy = strings.ReplaceAll(eventFilterOrderBy, "ee.", "")
		for _, sort := range sortClause {
			switch sort.Field {
			case "duration":
				eventFilterOrderBy = strings.ReplaceAll(eventFilterOrderBy, "(end_date - start_date)", "duration")
			case "event_updated":
				eventFilterOrderBy = strings.ReplaceAll(eventFilterOrderBy, "event_updated", "updated")
			}
		}
		eventFilterOrderBy = "ORDER BY " + eventFilterOrderBy
	}

	return orderByClause, eventFilterOrderBy, nil
}

func (s *SearchEventService) buildEventFilterFields(
	sortClause []SortClause,
	queryResult *ClickHouseQueryResult,
	dbToAliasMap map[string]string,
	conditionalFields []string,
	eventFilterOrderBy string,
) (*EventFilterFields, []string) {
	eventFilterSelectFields := []string{"ee.event_id as event_id", "ee.edition_id as edition_id"}
	eventFilterGroupByFields := []string{"ee.event_id", "ee.edition_id"}

	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			if sort.Field != "" && sort.Field != "event_id" && sort.Field != "edition_id" {
				alreadyAdded := false
				for _, existing := range eventFilterSelectFields {
					if existing == sort.Field || (sort.Field == "duration" && strings.Contains(existing, "end_date - start_date")) {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					if sort.Field == "duration" {
						eventFilterSelectFields = append(eventFilterSelectFields, "(end_date - start_date) as duration")
						eventFilterGroupByFields = append(eventFilterGroupByFields, "duration")
					} else {
						alias := dbToAliasMap[sort.Field]
						if alias == "" {
							alias = sort.Field
						}
						if alias != sort.Field {
							eventFilterSelectFields = append(eventFilterSelectFields, fmt.Sprintf("%s as %s", sort.Field, alias))
							eventFilterGroupByFields = append(eventFilterGroupByFields, alias)
						} else {
							eventFilterSelectFields = append(eventFilterSelectFields, sort.Field)
							eventFilterGroupByFields = append(eventFilterGroupByFields, sort.Field)
						}
					}
				}
			}
		}
	}

	if queryResult.DistanceOrderClause != "" && strings.Contains(queryResult.DistanceOrderClause, "greatCircleDistance") {
		if strings.Contains(queryResult.DistanceOrderClause, "COALESCE") {
			latAdded := false
			lonAdded := false
			for _, field := range eventFilterSelectFields {
				if strings.Contains(field, "COALESCE(ee.venue_lat, ee.edition_city_lat) as lat") {
					latAdded = true
				}
				if strings.Contains(field, "COALESCE(ee.venue_long, ee.edition_city_long) as lon") {
					lonAdded = true
				}
			}
			if !latAdded {
				eventFilterSelectFields = append(eventFilterSelectFields, "COALESCE(ee.venue_lat, ee.edition_city_lat) as lat")
				eventFilterGroupByFields = append(eventFilterGroupByFields, "lat")
			}
			if !lonAdded {
				eventFilterSelectFields = append(eventFilterSelectFields, "COALESCE(ee.venue_long, ee.edition_city_long) as lon")
				eventFilterGroupByFields = append(eventFilterGroupByFields, "lon")
			}
			conditionalFields = append(conditionalFields, "COALESCE(ee.venue_lat, ee.edition_city_lat) as lat")
			conditionalFields = append(conditionalFields, "COALESCE(ee.venue_long, ee.edition_city_long) as lon")
		} else {
			if (strings.Contains(queryResult.DistanceOrderClause, "edition_city_lat") && strings.Contains(queryResult.DistanceOrderClause, "edition_city_long")) ||
				(strings.Contains(queryResult.DistanceOrderClause, " lat") && strings.Contains(queryResult.DistanceOrderClause, " lon")) {
				latAdded := false
				lonAdded := false
				for _, field := range eventFilterSelectFields {
					if strings.Contains(field, "edition_city_lat as lat") {
						latAdded = true
					}
					if strings.Contains(field, "edition_city_long as lon") {
						lonAdded = true
					}
				}
				if !latAdded {
					eventFilterSelectFields = append(eventFilterSelectFields, "ee.edition_city_lat as lat")
					eventFilterGroupByFields = append(eventFilterGroupByFields, "lat")
				}
				if !lonAdded {
					eventFilterSelectFields = append(eventFilterSelectFields, "ee.edition_city_long as lon")
					eventFilterGroupByFields = append(eventFilterGroupByFields, "lon")
				}
				conditionalFields = append(conditionalFields, "ee.edition_city_lat as lat")
				conditionalFields = append(conditionalFields, "ee.edition_city_long as lon")
			}
			if (strings.Contains(queryResult.DistanceOrderClause, "venue_lat") && strings.Contains(queryResult.DistanceOrderClause, "venue_long")) ||
				(strings.Contains(queryResult.DistanceOrderClause, "venueLat") && strings.Contains(queryResult.DistanceOrderClause, "venueLon")) {
				venueLatAdded := false
				venueLonAdded := false
				for _, field := range eventFilterSelectFields {
					if strings.Contains(field, "venue_lat as venueLat") {
						venueLatAdded = true
					}
					if strings.Contains(field, "venue_long as venueLon") {
						venueLonAdded = true
					}
				}
				if !venueLatAdded {
					eventFilterSelectFields = append(eventFilterSelectFields, "ee.venue_lat as venueLat")
					eventFilterGroupByFields = append(eventFilterGroupByFields, "venueLat")
				}
				if !venueLonAdded {
					eventFilterSelectFields = append(eventFilterSelectFields, "ee.venue_long as venueLon")
					eventFilterGroupByFields = append(eventFilterGroupByFields, "venueLon")
				}
				conditionalFields = append(conditionalFields, "ee.venue_lat as venueLat")
				conditionalFields = append(conditionalFields, "ee.venue_long as venueLon")
			}
		}
	}

	// If eventFilterOrderBy requires event_score, ensure it's in the fields
	if eventFilterOrderBy == "ORDER BY event_score ASC" || eventFilterOrderBy == "" {
		hasEventScore := false
		for _, field := range eventFilterSelectFields {
			if strings.Contains(field, "event_score") || strings.Contains(field, "score") {
				hasEventScore = true
				break
			}
		}
		if !hasEventScore {
			eventFilterSelectFields = append(eventFilterSelectFields, "ee.event_score as event_score")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "ee.event_score")
		}
		if eventFilterOrderBy == "" {
			eventFilterOrderBy = "ORDER BY event_score ASC"
		}
	}

	return &EventFilterFields{
		SelectFields:  eventFilterSelectFields,
		GroupByFields: eventFilterGroupByFields,
		OrderBy:       eventFilterOrderBy,
	}, conditionalFields
}

func (s *SearchEventService) getListData(pagination models.PaginationDto, sortClause []SortClause, filterFields models.FilterDataDto, showValues string) (*ListResult, error) {
	fieldCtx, err := s.initializeFieldSelection(showValues, filterFields)
	if err != nil {
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	requestedFieldsSet := fieldCtx.Processor.GetRequestedFieldsSet()
	requestedGroupsSet := fieldCtx.Processor.GetRequestedGroups()

	conditionalFields := s.buildConditionalFields(fieldCtx, sortClause, filterFields)
	requiredFieldsStatic := append(fieldCtx.BaseFields, conditionalFields...)

	queryResult, err := s.sharedFunctionService.buildClickHouseQuery(filterFields)
	if err != nil {
		log.Printf("Error building ClickHouse query: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	log.Printf("Where clause: %s", queryResult.WhereClause)
	log.Printf("Search clause: %s", queryResult.SearchClause)

	orderByClause, eventFilterOrderBy, err := s.buildOrderByClauses(sortClause, queryResult)
	if err != nil {
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	eventFilterFields, conditionalFields := s.buildEventFilterFields(sortClause, queryResult, fieldCtx.DBToAliasMap, conditionalFields, eventFilterOrderBy)
	requiredFieldsStatic = append(fieldCtx.BaseFields, conditionalFields...)

	eventFilterOrderBy = eventFilterFields.OrderBy
	eventFilterSelectStr := strings.Join(eventFilterFields.SelectFields, ", ")
	eventFilterGroupByStr := strings.Join(eventFilterFields.GroupByFields, ", ")

	convertDistanceOrderForEventFilter := func(distanceClause string) string {
		if distanceClause == "" {
			return ""
		}
		if strings.Contains(distanceClause, "COALESCE") {
			converted := strings.ReplaceAll(distanceClause, "COALESCE(ee.venue_lat, ee.edition_city_lat)", "lat")
			converted = strings.ReplaceAll(converted, "COALESCE(ee.venue_long, ee.edition_city_long)", "lon")
			return converted
		}
		converted := strings.ReplaceAll(distanceClause, "ee.edition_city_lat", "lat")
		converted = strings.ReplaceAll(converted, "ee.edition_city_long", "lon")
		converted = strings.ReplaceAll(converted, "ee.venue_lat", "venueLat")
		converted = strings.ReplaceAll(converted, "ee.venue_long", "venueLon")
		return converted
	}

	finalOrderClause := orderByClause
	if queryResult.DistanceOrderClause != "" {
		if orderByClause != "" {
			distanceOrder := strings.TrimPrefix(queryResult.DistanceOrderClause, "ORDER BY ")
			otherOrder := strings.TrimPrefix(orderByClause, "ORDER BY ")
			finalOrderClause = fmt.Sprintf("ORDER BY %s, %s", distanceOrder, otherOrder)
		} else {
			finalOrderClause = queryResult.DistanceOrderClause
		}
		if eventFilterOrderBy == "" || eventFilterOrderBy == "ORDER BY event_score ASC" {
			eventFilterOrderBy = convertDistanceOrderForEventFilter(queryResult.DistanceOrderClause)
		} else {
			distanceOrder := strings.TrimPrefix(convertDistanceOrderForEventFilter(queryResult.DistanceOrderClause), "ORDER BY ")
			eventOrder := strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
			combinedOrder := fmt.Sprintf("ORDER BY %s, %s", distanceOrder, eventOrder)
			eventFilterOrderBy = fieldCtx.FieldsSelector.FixOrderByForFields(combinedOrder, eventFilterFields.SelectFields)
			if eventFilterOrderBy == "" {
				eventFilterOrderBy = convertDistanceOrderForEventFilter(queryResult.DistanceOrderClause)
			}
		}
	} else {
		if eventFilterOrderBy != "" && eventFilterOrderBy != "ORDER BY event_score ASC" {
			eventFilterOrderBy = fieldCtx.FieldsSelector.FixOrderByForFields(eventFilterOrderBy, eventFilterFields.SelectFields)
		} else if eventFilterOrderBy == "ORDER BY event_score ASC" {
			hasEventScore := false
			for _, field := range eventFilterFields.SelectFields {
				if strings.Contains(field, "event_score") || strings.Contains(field, "score") {
					hasEventScore = true
					break
				}
			}
			if !hasEventScore {
				eventFilterOrderBy = ""
			}
		}
	}

	var resolvedCountryIsos, resolvedCategoryNames []string
	if queryResult.NeedsEventRankingJoin {
		resolvedCountryIsos, resolvedCategoryNames, _ = s.sharedFunctionService.GetEventRankingScopeResolved(filterFields)
	}

	cteAndJoinResult := s.sharedFunctionService.buildFilterCTEsAndJoins(
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

	// If no explicit order clause, default to score - ensure score is in fields
	if finalOrderClause == "" {
		hasScore := false
		for _, field := range requiredFieldsStatic {
			if strings.Contains(field, "score") || strings.Contains(field, "event_score") {
				hasScore = true
				break
			}
		}
		if !hasScore {
			requiredFieldsStatic = append(requiredFieldsStatic, "ee.event_score as score")
		}
	}

	fieldsString := strings.Join(requiredFieldsStatic, ", ")
	finalGroupByClause := s.buildGroupByClause(requiredFieldsStatic)

	innerOrderBy := func() string {
		if finalOrderClause != "" {
			fixedOrderBy := fieldCtx.FieldsSelector.FixOrderByForFields(finalOrderClause, requiredFieldsStatic)
			if fixedOrderBy != "" {
				return fixedOrderBy
			}
			return s.sharedFunctionService.fixOrderByForCTE(finalOrderClause, true)
		}
		return "ORDER BY score ASC"
	}()

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
		s.sharedFunctionService.buildPublishedCondition(filterFields),
		s.sharedFunctionService.buildStatusCondition(filterFields),
		s.sharedFunctionService.buildEditionTypeCondition(filterFields, "ee"),
	}

	if !hasEndDateFilters {
		dateCondition := s.sharedFunctionService.buildDefaultDateCondition(filterFields.Forecasted, "ee", today)
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

	eventDataQuery := fmt.Sprintf(`
		WITH %sevent_filter AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			%s
			WHERE %s
			GROUP BY %s
			%s
			LIMIT %d OFFSET %d
		),
		event_data AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			WHERE ee.edition_id in (SELECT edition_id from event_filter)
			GROUP BY
				%s
			%s
		)
		SELECT *
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
		eventFilterGroupByStr,
		eventFilterOrderBy,
		pagination.Limit, pagination.Offset,
		fieldsString, finalGroupByClause, innerOrderBy)

	log.Printf("Event data query: %s", eventDataQuery)

	type dataResult struct {
		Rows driver.Rows
		Err  error
	}

	type countResult struct {
		Count int
		Err   error
	}

	dataChan := make(chan dataResult, 1)
	countChan := make(chan countResult, 1)

	go func() {
		eventDataQueryTime := time.Now()
		eventDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), eventDataQuery)
		eventDataQueryDuration := time.Since(eventDataQueryTime)
		log.Printf("Event data query time: %v", eventDataQueryDuration)
		dataChan <- dataResult{Rows: eventDataResult, Err: err}
	}()

	go func() {
		count, err := s.getListDataCount(
			queryResult,
			&cteAndJoinResult,
			eventFilterSelectStr,
			eventFilterGroupByStr,
			hasEndDateFilters,
			filterFields,
		)
		countChan <- countResult{Count: count, Err: err}
	}()

	dataRes := <-dataChan
	if dataRes.Err != nil {
		log.Printf("ClickHouse query error: %v", dataRes.Err)
		return nil, dataRes.Err
	}
	eventDataResult := dataRes.Rows

	countRes := <-countChan
	if countRes.Err != nil {
		log.Printf("ClickHouse count query error: %v", countRes.Err)
		log.Printf("Warning: Count query failed, continuing without count")
	}
	totalCount := countRes.Count

	var eventIds []uint32
	var eventData []map[string]interface{}
	eventFollowersMap := make(map[uint32]uint32)
	rowCount := 0

	for eventDataResult.Next() {
		rowCount++

		columns := eventDataResult.Columns()

		values := make([]interface{}, len(columns))
		for i, col := range columns {
			switch col {
			case "event_id", "edition_id", "followers", "impactScore", "exhibitors", "speakers", "sponsors", "exhibitors_lower_bound", "exhibitors_upper_bound", "estimatedExhibitors", "inboundScore", "internationalScore", "inboundAttendance", "internationalAttendance", "exhibitorsCount", "speakersCount", "sponsorsCount", "editions", "estimatedAttendanceMean", "estimatedVisitorsMean", "organizer_companyId", "trustScore":
				values[i] = new(uint32)
			case "score", "duration", "futurePredictionScore":
				values[i] = new(int32)
			case "id", "name", "city", "country", "description", "logo", "economicImpactBreakdown", "shortName", "format", "entryType", "website", "10timesEventPageUrl", "estimatedVisitorRangeTag", "maturity", "frequency", "organizer_id", "organizer_name", "organizer_website", "organizer_logoUrl", "organizer_address", "organizer_city", "organizer_state", "organizer_country", "audienceZone":
				values[i] = new(string)
			case "futureExpectedStartDate", "futureExpectedEndDate", "rehostDate":
				values[i] = new(time.Time)
			case "isBranded", "isSeries":
				values[i] = new(string)
			case "publishStatus":
				values[i] = new(int8)
			case "start", "end", "updated", "createdAt", "lastVerifiedOn", "start_date", "end_date", "event_created", "event_updated", "verifiedOn":
				values[i] = new(time.Time)
			case "avgRating", "event_avgRating":
				values[i] = new(*decimal.Decimal)
			case "lat", "lon", "venueLat", "venueLon", "economicImpact", "trustChangePercentage", "reputationChangePercentage", "edition_city_lat", "edition_city_long", "venue_lat", "venue_long", "event_economic_value":
				values[i] = new(float64)
			case "yoyGrowth":
				values[i] = new(uint32)
			case "keywords":
				values[i] = new(string)
			case "tickets", "timings":
				values[i] = new([]string)
			case "isNew":
				values[i] = new(bool)
			default:
				values[i] = new(string)
			}
		}

		if err := eventDataResult.Scan(values...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowData := make(map[string]interface{})
		var currentEventID uint32
		for i, col := range columns {
			val := values[i]

			switch col {
			case "event_id":
				if eventID, ok := val.(*uint32); ok && eventID != nil {
					eventIds = append(eventIds, *eventID)
					currentEventID = *eventID
					rowData["event_id"] = *eventID
				}
			case "followers":
				if followers, ok := val.(*uint32); ok && followers != nil {
					// Store followers in map for percentage calculation, but don't add to response
					if currentEventID != 0 {
						eventFollowersMap[currentEventID] = *followers
					}
				}
			case "id":
				if eventUUID, ok := val.(*string); ok && eventUUID != nil {
					if strings.TrimSpace(*eventUUID) == "" {
						rowData["id"] = nil
					} else {
						rowData["id"] = *eventUUID
					}
				} else {
					rowData["id"] = nil
				}
			case "start", "end", "updated", "createdAt", "start_date", "end_date", "event_created", "event_updated":
				var fieldName string
				switch col {
				case "start_date":
					fieldName = "start"
				case "end_date":
					fieldName = "end"
				case "event_created":
					fieldName = "createdAt"
				case "event_updated":
					fieldName = "updated"
				default:
					fieldName = col
				}

				if dateVal, ok := val.(*time.Time); ok && dateVal != nil {
					rowData[fieldName] = dateVal.Format("2006-01-02")
				}
			case "lastVerifiedOn":
				if dateVal, ok := val.(*time.Time); ok && dateVal != nil {
					if dateVal.IsZero() {
						rowData[col] = nil
					} else {
						rowData[col] = dateVal.Format("2006-01-02")
					}
				} else {
					rowData[col] = nil
				}
			case "futureExpectedStartDate", "futureExpectedEndDate", "rehostDate":
				if dateVal, ok := val.(*time.Time); ok && dateVal != nil {
					if dateVal.IsZero() {
						rowData[col] = nil
					} else {
						rowData[col] = dateVal.Format("2006-01-02")
					}
				} else {
					rowData[col] = nil
				}
			case "isNew":
				if isNewVal, ok := val.(*bool); ok && isNewVal != nil {
					rowData[col] = *isNewVal
				}
			case "avgRating":
				if avgRating, ok := val.(**decimal.Decimal); ok && avgRating != nil && *avgRating != nil {
					rounded := (*avgRating).Round(1)
					rowData[col] = rounded.InexactFloat64()
				} else {
					rowData[col] = 0.0
				}
			case "estimatedExhibitors":
				if estimatedExhibitors, ok := val.(*uint32); ok && estimatedExhibitors != nil {
					rowData[col] = *estimatedExhibitors
				} else {
					rowData[col] = uint32(0)
				}
			case "economicImpact":
				if economicValue, ok := val.(*float64); ok && economicValue != nil {
					formattedValue := s.transformDataService.formatCurrency(*economicValue)
					rowData[col] = formattedValue
				} else {
					rowData[col] = "$0"
				}
			case "yoyGrowth":
				if yoyGrowthVal, ok := val.(*uint32); ok && yoyGrowthVal != nil {
					rowData[col] = float64(*yoyGrowthVal)
				} else if yoyGrowthVal, ok := val.(*float64); ok && yoyGrowthVal != nil {
					rowData[col] = *yoyGrowthVal
				} else {
					rowData[col] = 0.0
				}
			case "economicImpactBreakdown":
				if economicBreakdown, ok := val.(*string); ok && economicBreakdown != nil && strings.TrimSpace(*economicBreakdown) != "" {
					var jsonData interface{}
					if err := json.Unmarshal([]byte(*economicBreakdown), &jsonData); err == nil {
						rowData[col] = jsonData
					} else {
						rowData[col] = *economicBreakdown
					}
				} else {
					rowData[col] = nil
				}
			case "tickets", "timings":
				if arrayPtr, ok := val.(*[]string); ok && arrayPtr != nil {
					parsedArray := make([]interface{}, 0, len(*arrayPtr))
					for _, str := range *arrayPtr {
						var jsonData interface{}
						if err := json.Unmarshal([]byte(str), &jsonData); err == nil {
							parsedArray = append(parsedArray, jsonData)
						} else {
							parsedArray = append(parsedArray, str)
						}
					}
					rowData[col] = parsedArray
				} else {
					rowData[col] = []interface{}{}
				}
			case "keywords":
				var keywordsString string
				var keywordsArray []string

				if keywordsStr, ok := val.(*string); ok && keywordsStr != nil && *keywordsStr != "" {
					keywordsString = *keywordsStr

					if strings.HasPrefix(keywordsString, "[") && strings.HasSuffix(keywordsString, "]") {
						if err := json.Unmarshal([]byte(keywordsString), &keywordsArray); err == nil {
							keywordsString = strings.Join(keywordsArray, " ")
						}
					}

					if len(keywordsArray) == 0 {
						if strings.Contains(keywordsString, ",") {
							parts := strings.Split(keywordsString, ",")
							keywordsArray = make([]string, 0, len(parts))
							for _, part := range parts {
								trimmed := strings.TrimSpace(part)
								if trimmed != "" {
									keywordsArray = append(keywordsArray, trimmed)
								}
							}
							keywordsString = strings.Join(keywordsArray, " ")
						} else {
							keywordsArray = strings.Fields(keywordsString)
						}
					}
				}

				if keywordsString != "" && filterFields.ParsedKeywords != nil && len(filterFields.ParsedKeywords.Include) > 0 {
					matchedKeywordsData, err := getMatchedKeywords(keywordsString, filterFields.ParsedKeywords.Include)
					if err == nil {
						rowData["matchedKeywords"] = matchedKeywordsData["matchedKeywords"]
						rowData["matchedKeywordsPercentage"] = matchedKeywordsData["matchedKeywordsPercentage"]
					}
				}
				if len(keywordsArray) > 0 {
					rowData["keywords"] = keywordsArray
				}
			case "isBranded", "isSeries":
				if uuidVal, ok := val.(*string); ok && uuidVal != nil && *uuidVal != "" {
					// If ID exists, return true
					rowData[col] = true
				} else {
					// If null/empty, return false
					rowData[col] = false
				}
			case "publishStatus":
				if publishStatusVal, ok := val.(*int8); ok && publishStatusVal != nil {
					rowData[col] = fmt.Sprintf("%d", *publishStatusVal)
				} else if publishStatusStr, ok := val.(*string); ok && publishStatusStr != nil {
					rowData[col] = *publishStatusStr
				}
			case "PrimaryEventType":
				if uuidVal, ok := val.(*string); ok && uuidVal != nil && *uuidVal != "" {
					if eventTypeName, exists := models.EventTypeById[*uuidVal]; exists {
						rowData[col] = eventTypeName
					} else {
						rowData[col] = nil
					}
				} else {
					rowData[col] = nil
				}
			case "format":
				if formatVal, ok := val.(*string); ok && formatVal != nil {
					formatStr := strings.ToLower(strings.TrimSpace(*formatVal))
					if formatStr == "offline" {
						rowData[col] = "in-person"
					} else if formatStr != "" {
						result := strings.ToLower(*formatVal)
						rowData[col] = result
					} else {
						rowData[col] = nil
					}
				} else {
					rowData[col] = nil
				}
			default:
				if ptr, ok := val.(*string); ok && ptr != nil {
					if strings.TrimSpace(*ptr) == "" {
						rowData[col] = nil
					} else {
						rowData[col] = *ptr
					}
				} else if ptr, ok := val.(*uint32); ok && ptr != nil {
					rowData[col] = *ptr
				} else if ptr, ok := val.(*int32); ok && ptr != nil {
					rowData[col] = *ptr
				} else if ptr, ok := val.(*float64); ok && ptr != nil {
					rowData[col] = *ptr
				} else {
					rowData[col] = val
				}
			}
		}

		eventData = append(eventData, rowData)
	}

	if len(eventIds) == 0 {
		return &ListResult{
			StatusCode: 200,
			Data:       []interface{}{},
			Count:      totalCount,
		}, nil
	}

	var eventToDesignationMatchInfo map[string]interface{}
	if filterFields.ParsedJobCompositeFilter != nil && filterFields.ParsedJobCompositeFilter.Property != nil {
		eventToDesignationMatchInfo = make(map[string]interface{})
	}

	var eventIdsStr []string
	for _, id := range eventIds {
		eventIdsStr = append(eventIdsStr, fmt.Sprintf("%d", id))
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")

	rankingScopeConditions := ""
	if scope, err := s.sharedFunctionService.BuildEventRankingScopeConditions(filterFields, resolvedCountryIsos, resolvedCategoryNames); err == nil {
		rankingScopeConditions = scope
	}

	queryBuilder := NewRelatedDataQueryBuilder(fieldCtx.Processor, filterFields, queryResult, eventIdsStrJoined, rankingScopeConditions)
	relatedDataQuery := queryBuilder.BuildQuery()

	log.Printf("Related data query: %s", relatedDataQuery)

	parallelQueryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type relatedDataResult struct {
		Rows     driver.Rows
		Err      error
		Duration time.Duration
	}

	type locationDataResult struct {
		Locations map[string]map[string]interface{}
		Err       error
		Duration  time.Duration
	}

	type audienceMatchInfoResult struct {
		MatchInfo map[string]interface{}
		Err       error
		Duration  time.Duration
	}

	type entityQualificationsResult struct {
		Qualifications map[uint32][]string
		Err            error
		Duration       time.Duration
	}

	relatedDataChan := make(chan relatedDataResult, 1)
	locationDataChan := make(chan locationDataResult, 1)
	audienceMatchInfoChan := make(chan audienceMatchInfoResult, 1)

	hasSearchByEntity := len(filterFields.ParsedSearchByEntity) > 0
	var entityQualificationsChan chan entityQualificationsResult
	if hasSearchByEntity {
		entityQualificationsChan = make(chan entityQualificationsResult, 1)
	}

	go func() {
		queryStartTime := time.Now()
		rows, err := s.clickhouseService.ExecuteQuery(parallelQueryCtx, relatedDataQuery)
		duration := time.Since(queryStartTime)

		relatedDataChan <- relatedDataResult{
			Rows:     rows,
			Err:      err,
			Duration: duration,
		}
	}()

	go func() {
		queryStartTime := time.Now()
		var locations map[string]map[string]interface{}
		var err error

		if len(eventIds) > 0 {
			locations, err = s.sharedFunctionService.GetEventLocations(eventIds, filterFields)
		} else {
			locations = make(map[string]map[string]interface{})
		}

		duration := time.Since(queryStartTime)

		locationDataChan <- locationDataResult{
			Locations: locations,
			Err:       err,
			Duration:  duration,
		}
	}()

	go func() {
		queryStartTime := time.Now()
		var matchInfo map[string]interface{}
		var err error

		if filterFields.ParsedJobCompositeFilter != nil && filterFields.ParsedJobCompositeFilter.Property != nil && len(eventIds) > 0 {
			matchInfo, err = s.sharedFunctionService.audienceTrackerMatchInfo(
				eventIds,
				filterFields.ParsedJobCompositeFilter.Property,
			)
		} else {
			matchInfo = make(map[string]interface{})
		}

		duration := time.Since(queryStartTime)

		audienceMatchInfoChan <- audienceMatchInfoResult{
			MatchInfo: matchInfo,
			Err:       err,
			Duration:  duration,
		}
	}()

	if hasSearchByEntity {
		go func() {
			queryStartTime := time.Now()
			var qualifications map[uint32][]string
			var err error

			if len(eventIds) > 0 {
				qualifications, err = s.sharedFunctionService.getEntityQualificationsForCompanyName(
					parallelQueryCtx,
					eventIds,
					filterFields,
				)
			} else {
				qualifications = make(map[uint32][]string)
			}

			duration := time.Since(queryStartTime)

			entityQualificationsChan <- entityQualificationsResult{
				Qualifications: qualifications,
				Err:            err,
				Duration:       duration,
			}
		}()
	}

	relatedDataRes := <-relatedDataChan
	locationDataRes := <-locationDataChan
	audienceMatchInfoRes := <-audienceMatchInfoChan

	var entityQualificationsRes entityQualificationsResult
	if hasSearchByEntity {
		entityQualificationsRes = <-entityQualificationsChan
	} else {
		entityQualificationsRes = entityQualificationsResult{
			Qualifications: make(map[uint32][]string),
			Err:            nil,
			Duration:       0,
		}
	}

	log.Printf("Related data query time: %v", relatedDataRes.Duration)
	log.Printf("Event location query time: %v", locationDataRes.Duration)
	log.Printf("Audience match info query time: %v", audienceMatchInfoRes.Duration)

	var eventTrackerMatchInfo map[uint32]map[string]interface{}

	if hasSearchByEntity {
		log.Printf("Entity qualifications query time: %v", entityQualificationsRes.Duration)

		if entityQualificationsRes.Err != nil {
			log.Printf("Error fetching entity qualifications: %v", entityQualificationsRes.Err)
			eventTrackerMatchInfo = make(map[uint32]map[string]interface{})
		} else {
			if len(entityQualificationsRes.Qualifications) > 0 {
				searchByEntity := filterFields.ParsedSearchByEntity[0]
				trackerMatchInfo, err := s.sharedFunctionService.getTrackerMatchInfo(
					entityQualificationsRes.Qualifications,
					searchByEntity,
					filterFields,
				)
				if err != nil {
					log.Printf("Error processing tracker match info: %v", err)
					eventTrackerMatchInfo = make(map[uint32]map[string]interface{})
				} else {
					eventTrackerMatchInfo = trackerMatchInfo
				}
			} else {
				eventTrackerMatchInfo = make(map[uint32]map[string]interface{})
			}
		}

		fmt.Println("eventTrackerMatchInfo:", eventTrackerMatchInfo)
	} else {
		eventTrackerMatchInfo = make(map[uint32]map[string]interface{})
	}

	if audienceMatchInfoRes.Err != nil {
		log.Printf("Error fetching audience match info: %v", audienceMatchInfoRes.Err)
	} else {
		eventToDesignationMatchInfo = audienceMatchInfoRes.MatchInfo
	}

	if relatedDataRes.Err != nil {
		log.Printf("Related data query error: %v", relatedDataRes.Err)
		if relatedDataRes.Rows != nil {
			relatedDataRes.Rows.Close()
		}
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: relatedDataRes.Err.Error(),
		}, nil
	}

	if locationDataRes.Err != nil {
		log.Printf("Error fetching event locations: %v", locationDataRes.Err)
		if relatedDataRes.Rows != nil {
			relatedDataRes.Rows.Close()
		}
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: locationDataRes.Err.Error(),
		}, nil
	}

	relatedDataRows := relatedDataRes.Rows
	eventLocationsMap := locationDataRes.Locations

	defer func() {
		if relatedDataRows != nil {
			relatedDataRows.Close()
		}
	}()

	categoriesMap := make(map[string][]map[string]string)
	tagsMap := make(map[string][]map[string]string)
	typesMap := make(map[string][]map[string]string)
	jobCompositeMap := make(map[string][]map[string]string)
	countrySpreadMap := make(map[string][]map[string]interface{})
	designationSpreadMap := make(map[string][]map[string]interface{})
	rankingsMap := make(map[string]string)

	// Helper function to convert interface{} to float64
	convertToFloat64 := func(value interface{}) (float64, bool) {
		switch v := value.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int:
			return float64(v), true
		case int8:
			return float64(v), true
		case int16:
			return float64(v), true
		case int32:
			return float64(v), true
		case int64:
			return float64(v), true
		case uint:
			return float64(v), true
		case uint8:
			return float64(v), true
		case uint16:
			return float64(v), true
		case uint32:
			return float64(v), true
		case uint64:
			return float64(v), true
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				return parsed, true
			}
		}
		return 0, false
	}

	for relatedDataRows.Next() {
		var eventID uint32
		var dataType, value, uuidValue, slugValue, eventGroupTypeValue string

		if err := relatedDataRows.Scan(&eventID, &dataType, &value, &uuidValue, &slugValue, &eventGroupTypeValue); err != nil {
			log.Printf("Error scanning related data row: %v", err)
			continue
		}

		eventIDStr := fmt.Sprintf("%d", eventID)

		switch dataType {
		case "category":
			names := strings.Split(value, "<sep>")
			uuids := strings.Split(uuidValue, ", ")
			slugs := strings.Split(slugValue, ", ")

			var items []map[string]string
			for i, name := range names {
				if i < len(uuids) && name != "" {
					items = append(items, map[string]string{
						"id":   strings.TrimSpace(uuids[i]),
						"name": strings.TrimSpace(name),
						"slug": strings.TrimSpace(slugs[i]),
					})
				}
			}
			categoriesMap[eventIDStr] = items
		case "tags":
			var items []map[string]string
			if value != "" && uuidValue != "" {
				names := strings.Split(value, "<sep>")
				uuids := strings.Split(uuidValue, ", ")
				slugs := strings.Split(slugValue, ", ")

				for i, name := range names {
					if i < len(uuids) && name != "" {
						items = append(items, map[string]string{
							"id":   strings.TrimSpace(uuids[i]),
							"name": strings.TrimSpace(name),
							"slug": strings.TrimSpace(slugs[i]),
						})
					}
				}
			}
			tagsMap[eventIDStr] = items
		case "types":
			names := strings.Split(value, ", ")
			uuids := strings.Split(uuidValue, ", ")
			slugs := strings.Split(slugValue, ", ")
			eventGroupTypes := strings.Split(eventGroupTypeValue, ", ")

			var items []map[string]string
			for i, name := range names {
				if i < len(uuids) && name != "" {
					items = append(items, map[string]string{
						"id":             strings.TrimSpace(uuids[i]),
						"name":           strings.TrimSpace(name),
						"slug":           strings.TrimSpace(slugs[i]),
						"eventGroupType": strings.TrimSpace(eventGroupTypes[i]),
					})
				}
			}
			typesMap[eventIDStr] = items
		case "jobComposite":
			parts := strings.Split(value, "|||")
			if len(parts) >= 3 {
				jobCompositeMap[eventIDStr] = append(jobCompositeMap[eventIDStr], map[string]string{
					"name":       parts[0],
					"role":       parts[1],
					"department": parts[2],
				})
			}
		case "audienceSpread":
			var countryArray []map[string]interface{}
			if err := json.Unmarshal([]byte(value), &countryArray); err != nil {
				log.Printf("Error unmarshaling audienceSpread for event %d: %v", eventID, err)
			} else {
				followers := eventFollowersMap[eventID]
				sort.Slice(countryArray, func(i, j int) bool {
					iCount, _ := convertToFloat64(countryArray[i]["total_count"])
					jCount, _ := convertToFloat64(countryArray[j]["total_count"])
					return iCount > jCount
				})
				for _, countryData := range countryArray {
					transformedCountryData := make(map[string]interface{})
					if cntryId, ok := countryData["cntry_id"]; ok {
						transformedCountryData["iso"] = strings.ToUpper(cntryId.(string))

						if isoStr, ok := cntryId.(string); ok {
							countryInfo := s.sharedFunctionService.GetCountryDataByISO(strings.ToUpper(isoStr))
							if countryInfo != nil {
								if name, ok := countryInfo["name"].(string); ok {
									transformedCountryData["name"] = name
								}
								if geoLat, ok := countryInfo["geoLat"].(float64); ok {
									transformedCountryData["latitude"] = geoLat
								}
								if geoLong, ok := countryInfo["geoLong"].(float64); ok {
									transformedCountryData["longitude"] = geoLong
								}
							}
						}
					}
					if totalCount, ok := countryData["total_count"]; ok {
						transformedCountryData["count"] = totalCount
						percentage := 0.0
						if userCount, ok := convertToFloat64(totalCount); ok && userCount > 0 {
							percentage = math.Round((userCount/float64(followers))*100*100) / 100
						}
						transformedCountryData["percentage"] = percentage
					}
					countrySpreadMap[eventIDStr] = append(countrySpreadMap[eventIDStr], transformedCountryData)
				}
			}
		case "designationSpread":
			var designationArray []map[string]interface{}
			if err := json.Unmarshal([]byte(value), &designationArray); err != nil {
				log.Printf("Error unmarshaling designationSpread for event %d: %v", eventID, err)
			} else {
				followers, exists := eventFollowersMap[eventID]
				shouldLimit := len(designationArray) > 5 && (fieldCtx.Processor.GetShowValues() == "" ||
					(strings.ToLower(strings.TrimSpace(filterFields.View)) == "list" ||
						strings.ToLower(strings.TrimSpace(filterFields.View)) == "tracker"))

				sort.Slice(designationArray, func(i, j int) bool {
					iCount, _ := convertToFloat64(designationArray[i]["total_count"])
					jCount, _ := convertToFloat64(designationArray[j]["total_count"])
					return iCount > jCount
				})

				if shouldLimit && len(designationArray) > 5 {
					designationArray = designationArray[:5]
				}

				for _, designationData := range designationArray {
					transformedDesignationData := make(map[string]interface{})
					if designation, ok := designationData["designation"]; ok {
						transformedDesignationData["designation"] = designation
					}
					if totalCount, ok := designationData["total_count"]; ok {
						transformedDesignationData["count"] = totalCount
						percentage := 0.0
						if exists && followers > 0 {
							var userCount float64
							if countFloat, ok := totalCount.(float64); ok {
								userCount = countFloat
							} else if countStr, ok := totalCount.(string); ok {
								if parsedCount, err := strconv.ParseFloat(countStr, 64); err == nil {
									userCount = parsedCount
								}
							}

							if userCount > 0 {
								percentage = math.Round((userCount/float64(followers))*100*100) / 100
							}
						}
						transformedDesignationData["percentage"] = percentage
					}
					designationSpreadMap[eventIDStr] = append(designationSpreadMap[eventIDStr], transformedDesignationData)
				}
			}
		case "rankings":
			rankingsMap[eventIDStr] = value
		}
	}

	var combinedData []map[string]interface{}
	for _, event := range eventData {
		eventID := fmt.Sprintf("%d", event["event_id"])

		grouper := NewFieldGrouper(fieldCtx.Processor)
		grouper.ProcessEventFields(event)

		var combinedEvent map[string]interface{}
		var groupedFields map[ResponseGroups]map[string]interface{}

		if fieldCtx.Processor.IsGroupedStructure() {
			groupedFields = grouper.GetGroupedFields()
			combinedEvent = make(map[string]interface{})
		} else {
			combinedEvent = grouper.GetFlatEvent()
		}

		if len(requestedFieldsSet) == 0 || requestedFieldsSet["isNew"] {
			var createdTime, updatedTime time.Time
			var hasCreated, hasUpdated bool

			if createdAtStr, ok := event["createdAt"].(string); ok && createdAtStr != "" {
				if t, err := time.Parse("2006-01-02", createdAtStr); err == nil {
					createdTime = t
					hasCreated = true
				}
			} else if createdAtTime, ok := event["createdAt"].(time.Time); ok {
				createdTime = createdAtTime
				hasCreated = true
			}

			if updatedStr, ok := event["updated"].(string); ok && updatedStr != "" {
				if t, err := time.Parse("2006-01-02", updatedStr); err == nil {
					updatedTime = t
					hasUpdated = true
				}
			} else if updatedTimeVal, ok := event["updated"].(time.Time); ok {
				updatedTime = updatedTimeVal
				hasUpdated = true
			}

			var isNewValue bool
			if hasCreated && hasUpdated {
				isNewValue = updatedTime.After(createdTime) || updatedTime.Equal(createdTime)
			} else if hasCreated {
				isNewValue = false
			}

			grouper.AddField("isNew", isNewValue)
		}

		if len(requestedFieldsSet) == 0 || requestedFieldsSet["organizer"] {
			organizer := make(map[string]interface{})
			hasOrganizerData := false

			if id, ok := event["organizer_id"].(string); ok && id != "" {
				organizer["id"] = id
				hasOrganizerData = true
			}
			if name, ok := event["organizer_name"].(string); ok && name != "" {
				organizer["name"] = name
				hasOrganizerData = true
			}
			if website, ok := event["organizer_website"].(string); ok && website != "" {
				organizer["website"] = website
				hasOrganizerData = true
			}
			if logoUrl, ok := event["organizer_logoUrl"].(string); ok && logoUrl != "" {
				organizer["logoUrl"] = logoUrl
				hasOrganizerData = true
			}

			var companyId uint32
			if idUint, ok := event["organizer_companyId"].(uint32); ok {
				companyId = idUint
			} else if idStr, ok := event["organizer_companyId"].(string); ok && idStr != "" {
				if parsedId, err := strconv.ParseUint(idStr, 10, 32); err == nil {
					companyId = uint32(parsedId)
				}
			}
			if companyId != 0 {
				organizer["prospectId"] = companyId
				hasOrganizerData = true
			}

			location := make(map[string]interface{})
			if address, ok := event["organizer_address"].(string); ok && strings.TrimSpace(address) != "" {
				location["address"] = strings.TrimSpace(address)
				hasOrganizerData = true
			}
			if city, ok := event["organizer_city"].(string); ok && strings.TrimSpace(city) != "" {
				location["city"] = strings.TrimSpace(city)
				hasOrganizerData = true
			}
			if state, ok := event["organizer_state"].(string); ok && strings.TrimSpace(state) != "" {
				location["state"] = strings.TrimSpace(state)
				hasOrganizerData = true
			}
			if country, ok := event["organizer_country"].(string); ok && strings.TrimSpace(country) != "" {
				location["country"] = strings.TrimSpace(country)
				hasOrganizerData = true
			}
			if len(location) > 0 {
				organizer["location"] = location
			}

			if hasOrganizerData {
				grouper.AddNestedField("organizer", organizer)
			}
		}

		categories := categoriesMap[eventID]
		if categories == nil {
			categories = []map[string]string{}
		}
		tags := tagsMap[eventID]
		if tags == nil {
			tags = []map[string]string{}
		}
		types := typesMap[eventID]
		grouper.AddField("categories", categories)
		grouper.AddField("tags", tags)
		grouper.AddField("eventTypes", types)

		if fieldCtx.Processor.GetRequestedGroups()[ResponseGroupBasic] {
			grouper.AddField("designations", []interface{}{})
		}

		if fieldCtx.Processor.GetRequestedGroups()[ResponseGroupBasic] {
			grouper.AddField("bannerUrl", nil)
		}

		if fieldCtx.Processor.GetRequestedGroups()[ResponseGroupBasic] || len(requestedFieldsSet) == 0 || requestedFieldsSet["matchedKeywords"] || requestedFieldsSet["matchedKeywordsPercentage"] {
			if matchInfo, ok := eventToDesignationMatchInfo[eventID]; ok {
				if matchInfoMap, ok := matchInfo.(map[string]interface{}); ok {
					if matchedKeywords, exists := matchInfoMap["matchedKeywords"]; exists {
						grouper.AddField("matchedKeywords", matchedKeywords)
					}
					if matchedKeywordsPercentage, exists := matchInfoMap["matchedKeywordsPercentage"]; exists {
						grouper.AddField("matchedKeywordsPercentage", matchedKeywordsPercentage)
					}
				}
			}
		}

		if fieldCtx.Processor.GetRequestedGroups()[ResponseGroupBasic] || len(requestedFieldsSet) == 0 || requestedFieldsSet["eventLocation"] {
			if eventLocation, ok := eventLocationsMap[eventID]; ok && eventLocation != nil {
				grouper.AddField("eventLocation", eventLocation)
			} else {
				grouper.AddField("eventLocation", nil)
			}
		}

		if len(requestedFieldsSet) == 0 || requestedFieldsSet["jobComposite"] {
			if jobCompositeData, ok := jobCompositeMap[eventID]; ok && len(jobCompositeData) > 0 {
				var jobCompositeArray []map[string]interface{}
				for _, designation := range jobCompositeData {
					jobCompositeArray = append(jobCompositeArray, map[string]interface{}{
						"name":       designation["name"],
						"role":       designation["role"],
						"department": designation["department"],
					})
				}
				grouper.AddField("jobComposite", jobCompositeArray)
			}
		}

		var audienceZone interface{}
		if zone, ok := event["audienceZone"].(string); ok && strings.TrimSpace(zone) != "" {
			audienceZone = zone
		} else {
			audienceZone = nil
		}
		countrySpreadData, _ := countrySpreadMap[eventID]
		designationSpreadData, _ := designationSpreadMap[eventID]

		// Calculate total count from all country spread (audienceSpread) entries and recalculate percentages
		totalCountryCount := 0.0
		eventIDUint, _ := event["event_id"].(uint32)
		followers, exists := eventFollowersMap[eventIDUint]

		// Recalculate percentages for each country entry using followers
		for i, countryItem := range countrySpreadData {
			if count, ok := countryItem["count"]; ok {
				if countFloat, ok := convertToFloat64(count); ok {
					totalCountryCount += countFloat
					if exists && followers > 0 && countFloat > 0 {
						percentage := math.Round((countFloat/float64(followers))*100*100) / 100
						countrySpreadData[i]["percentage"] = percentage
					} else {
						countrySpreadData[i]["percentage"] = 0.0
					}
				}
			}
		}

		grouper.AddAudienceData(countrySpreadData, designationSpreadData, audienceZone)

		var estimatedExhibitorsValue string
		if lowerBound, ok := event["exhibitors_lower_bound"].(uint32); ok {
			if upperBound, ok := event["exhibitors_upper_bound"].(uint32); ok {
				if lowerBound > 0 || upperBound > 0 {
					estimatedExhibitorsValue = fmt.Sprintf("%d-%d", lowerBound, upperBound)
				} else {
					estimatedExhibitorsValue = "0-0"
				}
			}
		} else {
			estimatedExhibitorsValue = "0-0"
		}
		grouper.AddField("estimatedExhibitors", estimatedExhibitorsValue)

		if len(requestedFieldsSet) == 0 || requestedFieldsSet["trustChangeTag"] {
			var trustChangeTagValue interface{}
			if trustChangePercentageVal, ok := event["trustChangePercentage"].(float64); ok {
				if trustChangePercentageVal > 10 {
					trustChangeTagValue = "improved"
				} else if trustChangePercentageVal < -10 {
					trustChangeTagValue = "declined"
				} else {
					trustChangeTagValue = "neutral"
				}
			} else if trustChangePercentageStr, ok := event["trustChangePercentage"].(string); ok {
				if trustChangePercentageStr == "null" {
					trustChangeTagValue = nil
				} else {
					if num, err := strconv.ParseFloat(trustChangePercentageStr, 64); err == nil {
						if num > 10 {
							trustChangeTagValue = "improved"
						} else if num < -10 {
							trustChangeTagValue = "declined"
						} else {
							trustChangeTagValue = "neutral"
						}
					} else {
						trustChangeTagValue = nil
					}
				}
			} else {
				trustChangeTagValue = nil
			}

			grouper.AddField("trustChangeTag", trustChangeTagValue)
		}

		if len(requestedFieldsSet) == 0 || requestedFieldsSet["reputationChangeTag"] {
			var reputationChangeTagValue interface{}
			if reputationChangePercentageVal, ok := event["reputationChangePercentage"].(float64); ok {
				if reputationChangePercentageVal > 10 {
					reputationChangeTagValue = "improved"
				} else if reputationChangePercentageVal < -10 {
					reputationChangeTagValue = "declined"
				} else {
					reputationChangeTagValue = "neutral"
				}
			} else if reputationChangePercentageStr, ok := event["reputationChangePercentage"].(string); ok {
				if reputationChangePercentageStr == "null" {
					reputationChangeTagValue = nil
				} else {
					if num, err := strconv.ParseFloat(reputationChangePercentageStr, 64); err == nil {
						if num > 10 {
							reputationChangeTagValue = "improved"
						} else if num < -10 {
							reputationChangeTagValue = "declined"
						} else {
							reputationChangeTagValue = "neutral"
						}
					} else {
						reputationChangeTagValue = nil
					}
				}
			} else {
				reputationChangeTagValue = nil
			}

			grouper.AddField("reputationChangeTag", reputationChangeTagValue)
		}

		shouldIncludeRankings := false
		if filterFields.View != "" {
			viewLower := strings.ToLower(strings.TrimSpace(filterFields.View))
			if viewLower == "list" || viewLower == "tracker" {
				shouldIncludeRankings = len(requestedFieldsSet) == 0 || requestedFieldsSet["rankings"]
			}
		}
		if !shouldIncludeRankings {
			shouldIncludeRankings = requestedGroupsSet[ResponseGroupInsights] || requestedFieldsSet["rankings"]
		}
		if shouldIncludeRankings {
			rankingsValue, hasRankings := rankingsMap[eventID]
			if hasRankings && rankingsValue != "" {
				parsedRankings := s.transformDataService.TransformRankings(rankingsValue, filterFields)
				grouper.AddField("rankings", parsedRankings)
			} else {
				grouper.AddField("rankings", nil)
			}
		}

		if fieldCtx.Processor.IsGroupedStructure() {

			if len(requestedFieldsSet) == 0 || requestedFieldsSet["impactScore"] || requestedFieldsSet["inboundScore"] || requestedFieldsSet["internationalScore"] {
				scoresData := make(map[string]interface{})
				hasScores := false

				extractScore := func(source map[string]interface{}, key string) (interface{}, bool) {
					if val, exists := source[key]; exists && val != nil {
						if score, ok := val.(float64); ok {
							return score, true
						}
						if score, ok := val.(uint32); ok {
							return float64(score), true
						}
					}
					return nil, false
				}

				if insightsGroup, ok := groupedFields[ResponseGroupInsights]; ok {
					if score, ok := extractScore(insightsGroup, "impactScore"); ok {
						scoresData["impactScore"] = score
						hasScores = true
						delete(insightsGroup, "impactScore")
					}
					if score, ok := extractScore(insightsGroup, "inboundScore"); ok {
						scoresData["inboundScore"] = score
						hasScores = true
						delete(insightsGroup, "inboundScore")
					}
					if score, ok := extractScore(insightsGroup, "internationalScore"); ok {
						scoresData["internationalScore"] = score
						hasScores = true
						delete(insightsGroup, "internationalScore")
					}
					if hasScores {
						insightsGroup["scores"] = scoresData
					}
				}
			}
		}

		combinedEvent = grouper.GetFinalEvent()
		delete(combinedEvent, "event_id")

		if len(eventTrackerMatchInfo) > 0 {
			if eventIDUint, ok := event["event_id"].(uint32); ok {
				if trackerInfo, exists := eventTrackerMatchInfo[eventIDUint]; exists && trackerInfo != nil && len(trackerInfo) > 0 {
					combinedEvent["trackerMatchInfo"] = trackerInfo
				}
			}
		}

		combinedData = append(combinedData, combinedEvent)
	}

	viewLower := strings.ToLower(strings.TrimSpace(filterFields.View))
	if viewLower == "promote" {
		transformedData := s.getPromoteEventListingResponse(combinedData)
		return &ListResult{
			StatusCode: 200,
			Data:       transformedData,
			Count:      totalCount,
		}, nil
	}
	return &ListResult{
		StatusCode: 200,
		Data:       combinedData,
		Count:      totalCount,
	}, nil
}

func (s *SearchEventService) getMapData(sortClause []SortClause, filterFields models.FilterDataDto) (*ListResult, error) {
	queryResult, err := s.sharedFunctionService.buildClickHouseQuery(filterFields)
	if err != nil {
		log.Printf("Error building ClickHouse query: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	log.Printf("Map view - Where clause: %s", queryResult.WhereClause)
	log.Printf("Map view - Search clause: %s", queryResult.SearchClause)

	_, eventFilterOrderBy, err := s.buildOrderByClauses(sortClause, queryResult)
	if err != nil {
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	eventFilterSelectFields := []string{"ee.event_id as event_id", "ee.edition_id as edition_id"}
	eventFilterGroupByFields := []string{"ee.event_id", "ee.edition_id"}

	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			if sort.Field != "" && sort.Field != "event_id" && sort.Field != "edition_id" {
				alreadyAdded := false
				for _, existing := range eventFilterSelectFields {
					if existing == sort.Field {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					if sort.Field == "duration" {
						eventFilterSelectFields = append(eventFilterSelectFields, "(end_date - start_date) as duration")
						eventFilterGroupByFields = append(eventFilterGroupByFields, "duration")
					} else {
						eventFilterSelectFields = append(eventFilterSelectFields, sort.Field)
						eventFilterGroupByFields = append(eventFilterGroupByFields, sort.Field)
					}
				}
			}
		}
	}

	if queryResult.DistanceOrderClause != "" {
		if strings.Contains(queryResult.DistanceOrderClause, "COALESCE") {
			eventFilterSelectFields = append(eventFilterSelectFields,
				"COALESCE(ee.venue_lat, ee.edition_city_lat) as lat",
				"COALESCE(ee.venue_long, ee.edition_city_long) as lon")
			eventFilterGroupByFields = append(eventFilterGroupByFields, "lat", "lon")
			convertedDistanceOrder := strings.ReplaceAll(queryResult.DistanceOrderClause, "COALESCE(ee.venue_lat, ee.edition_city_lat)", "lat")
			convertedDistanceOrder = strings.ReplaceAll(convertedDistanceOrder, "COALESCE(ee.venue_long, ee.edition_city_long)", "lon")
			if eventFilterOrderBy != "" && eventFilterOrderBy != "ORDER BY event_score ASC" {
				distanceOrder := strings.TrimPrefix(convertedDistanceOrder, "ORDER BY ")
				eventOrder := strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
				eventFilterOrderBy = fmt.Sprintf("ORDER BY %s, %s", distanceOrder, eventOrder)
			} else {
				eventFilterOrderBy = convertedDistanceOrder
			}
		} else {
			if (strings.Contains(queryResult.DistanceOrderClause, "edition_city_lat") && strings.Contains(queryResult.DistanceOrderClause, "edition_city_long")) ||
				(strings.Contains(queryResult.DistanceOrderClause, " lat") && strings.Contains(queryResult.DistanceOrderClause, " lon")) {
				eventFilterSelectFields = append(eventFilterSelectFields, "ee.edition_city_lat as lat", "ee.edition_city_long as lon")
				eventFilterGroupByFields = append(eventFilterGroupByFields, "lat", "lon")
			}
			if (strings.Contains(queryResult.DistanceOrderClause, "venue_lat") && strings.Contains(queryResult.DistanceOrderClause, "venue_long")) ||
				(strings.Contains(queryResult.DistanceOrderClause, "venueLat") && strings.Contains(queryResult.DistanceOrderClause, "venueLon")) {
				eventFilterSelectFields = append(eventFilterSelectFields, "ee.venue_lat as venueLat", "ee.venue_long as venueLon")
				eventFilterGroupByFields = append(eventFilterGroupByFields, "venueLat", "venueLon")
			}
			convertedDistanceOrder := strings.ReplaceAll(queryResult.DistanceOrderClause, "ee.edition_city_lat", "lat")
			convertedDistanceOrder = strings.ReplaceAll(convertedDistanceOrder, "ee.edition_city_long", "lon")
			convertedDistanceOrder = strings.ReplaceAll(convertedDistanceOrder, "ee.venue_lat", "venueLat")
			convertedDistanceOrder = strings.ReplaceAll(convertedDistanceOrder, "ee.venue_long", "venueLon")
			if eventFilterOrderBy != "" && eventFilterOrderBy != "ORDER BY event_score ASC" {
				distanceOrder := strings.TrimPrefix(convertedDistanceOrder, "ORDER BY ")
				eventOrder := strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
				eventFilterOrderBy = fmt.Sprintf("ORDER BY %s, %s", distanceOrder, eventOrder)
			} else {
				eventFilterOrderBy = convertedDistanceOrder
			}
		}
	}

	eventFilterSelectStr := strings.Join(eventFilterSelectFields, ", ")
	eventFilterGroupByStr := strings.Join(eventFilterGroupByFields, ", ")

	if eventFilterOrderBy == "" {
		eventFilterOrderBy = "ORDER BY event_score ASC"
	} else {
		eventFilterOrderBy = strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
		eventFilterOrderBy = strings.ReplaceAll(eventFilterOrderBy, "ee.", "")
		eventFilterOrderBy = "ORDER BY " + eventFilterOrderBy
	}

	cteAndJoinResult := s.sharedFunctionService.buildFilterCTEsAndJoins(
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

	hasEndDateFilters := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
		filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != "" ||
		filterFields.CreatedAt != "" || len(filterFields.ParsedEventIds) > 0 || len(filterFields.ParsedNotEventIds) > 0 || len(filterFields.ParsedSourceEventIds) > 0 || len(filterFields.ParsedDates) > 0 || filterFields.ParsedPastBetween != nil || filterFields.ParsedActiveBetween != nil

	mapFields := []string{
		"ee.event_uuid as id",
		"ee.impactScore as impactScore",
		"ee.PrimaryEventType as PrimaryEventType",
		"ee.venue_lat as venueLat",
		"ee.venue_long as venueLon",
		"ee.edition_city_lat as cityLat",
		"ee.edition_city_long as cityLon",
		"ee.edition_country as code",
	}
	mapFieldsStr := strings.Join(mapFields, ", ")
	mapGroupByClause := strings.Join([]string{"id", "impactScore", "PrimaryEventType", "venueLat", "venueLon", "cityLat", "cityLon", "code"}, ", ")

	today := time.Now().Format("2006-01-02")

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinClauses := ""
	if cteAndJoinResult.JoinClausesStr != "" {
		joinClauses = cteAndJoinResult.JoinClausesStr
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	whereConditions := []string{
		s.sharedFunctionService.buildPublishedCondition(filterFields),
		s.sharedFunctionService.buildStatusCondition(filterFields),
		s.sharedFunctionService.buildEditionTypeCondition(filterFields, "ee"),
	}
	if !hasEndDateFilters {
		dateCondition := s.sharedFunctionService.buildDefaultDateCondition(filterFields.Forecasted, "ee", today)
		whereConditions = append(whereConditions, dateCondition)
	}
	if queryResult.WhereClause != "" {
		whereConditions = append(whereConditions, queryResult.WhereClause)
	}
	if queryResult.SearchClause != "" {
		whereConditions = append(whereConditions, queryResult.SearchClause)
	}
	if joinConditionsStr != "" {
		whereConditions = append(whereConditions, strings.TrimPrefix(joinConditionsStr, "AND "))
	}
	whereClause := strings.Join(whereConditions, "\n                        AND ")

	mapDataQuery := fmt.Sprintf(`
		WITH %sevent_filter AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			%s
			WHERE %s
			GROUP BY %s
			%s
		),
		event_data AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			WHERE ee.edition_id IN (SELECT edition_id FROM event_filter)
			GROUP BY %s
		)
		SELECT *
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
		eventFilterGroupByStr,
		eventFilterOrderBy,
		mapFieldsStr,
		mapGroupByClause)

	log.Printf("Map data query: %s", mapDataQuery)

	type dataResult struct {
		Rows driver.Rows
		Err  error
	}

	type countResult struct {
		Count int
		Err   error
	}

	dataChan := make(chan dataResult, 1)
	countChan := make(chan countResult, 1)

	go func() {
		eventDataQueryTime := time.Now()
		eventDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), mapDataQuery)
		eventDataQueryDuration := time.Since(eventDataQueryTime)
		log.Printf("Map data query time: %v", eventDataQueryDuration)
		dataChan <- dataResult{Rows: eventDataResult, Err: err}
	}()

	go func() {
		count, err := s.getListDataCount(
			queryResult,
			&cteAndJoinResult,
			eventFilterSelectStr,
			eventFilterGroupByStr,
			hasEndDateFilters,
			filterFields,
		)
		countChan <- countResult{Count: count, Err: err}
	}()

	dataRes := <-dataChan
	if dataRes.Err != nil {
		log.Printf("ClickHouse map query error: %v", dataRes.Err)
		return nil, dataRes.Err
	}
	eventDataResult := dataRes.Rows

	countRes := <-countChan
	if countRes.Err != nil {
		log.Printf("ClickHouse count query error: %v", countRes.Err)
		log.Printf("Warning: Count query failed, continuing without count")
	}
	totalCount := countRes.Count

	var mapData []map[string]interface{}

	for eventDataResult.Next() {
		columns := eventDataResult.Columns()

		values := make([]interface{}, len(columns))
		for i, col := range columns {
			switch col {
			case "id":
				values[i] = new(string)
			case "impactScore":
				values[i] = new(uint32)
			case "PrimaryEventType":
				values[i] = new(string)
			case "venueLat", "venueLon", "cityLat", "cityLon":
				values[i] = new(float64)
			default:
				values[i] = new(string)
			}
		}

		if err := eventDataResult.Scan(values...); err != nil {
			log.Printf("Error scanning map row: %v", err)
			continue
		}

		rowData := make(map[string]interface{})
		var venueLat, venueLon, cityLat, cityLon *float64

		for i, col := range columns {
			val := values[i]

			switch col {
			case "id":
				if id, ok := val.(*string); ok && id != nil {
					rowData["id"] = *id
				}
			case "impactScore":
				if score, ok := val.(*uint32); ok && score != nil {
					rowData["eventImpactScore"] = *score
				} else {
					rowData["eventImpactScore"] = uint32(0)
				}
			case "PrimaryEventType":
				if uuid, ok := val.(*string); ok && uuid != nil {
					// Convert UUID to slug using EventTypeById map
					if slug, exists := models.EventTypeById[*uuid]; exists {
						rowData["eventType"] = slug
					} else {
						rowData["eventType"] = nil
					}
				} else {
					rowData["eventType"] = nil
				}
			case "venueLat":
				if lat, ok := val.(*float64); ok && lat != nil {
					venueLat = lat
				}
			case "venueLon":
				if lon, ok := val.(*float64); ok && lon != nil {
					venueLon = lon
				}
			case "cityLat":
				if lat, ok := val.(*float64); ok && lat != nil {
					cityLat = lat
				}
			case "cityLon":
				if lon, ok := val.(*float64); ok && lon != nil {
					cityLon = lon
				}
			default:
				if ptr, ok := val.(*string); ok && ptr != nil {
					if strings.TrimSpace(*ptr) == "" {
						rowData[col] = nil
					} else {
						rowData[col] = *ptr
					}
				}
			}
		}

		// Coordinate fallback: venue coordinates → city coordinates
		var latitude, longitude *float64
		if venueLat != nil && venueLon != nil && *venueLat != 0 && *venueLon != 0 {
			latitude = venueLat
			longitude = venueLon
		} else if cityLat != nil && cityLon != nil && *cityLat != 0 && *cityLon != 0 {
			latitude = cityLat
			longitude = cityLon
		}

		if latitude != nil && longitude != nil {
			rowData["latitude"] = *latitude
			rowData["longitude"] = *longitude
		} else {
			rowData["latitude"] = nil
			rowData["longitude"] = nil
		}

		mapData = append(mapData, rowData)
	}

	if len(mapData) == 0 {
		return &ListResult{
			StatusCode: 200,
			Data:       []interface{}{},
			Count:      totalCount,
		}, nil
	}

	return &ListResult{
		StatusCode: 200,
		Data:       mapData,
		Count:      totalCount,
	}, nil
}

func (s *SearchEventService) getPromoteEventListingResponse(events []map[string]interface{}) interface{} {
	var transformedEvents []map[string]interface{}

	for _, event := range events {
		var basic map[string]interface{}
		if basicVal, ok := event["basic"].(map[string]interface{}); ok {
			basic = basicVal
		} else {
			basic = event
		}

		var id, name interface{}
		var startDate, endDate interface{}
		var eventLocation map[string]interface{}
		var description, format, primaryEventType interface{}

		if val, ok := basic["id"]; ok {
			id = val
		}
		if val, ok := basic["name"]; ok {
			name = val
		}
		if val, ok := basic["startDateTime"]; ok {
			startDate = val
		} else if val, ok := basic["start"]; ok {
			startDate = val
		}
		if val, ok := basic["endDateTime"]; ok {
			endDate = val
		} else if val, ok := basic["end"]; ok {
			endDate = val
		}
		if val, ok := basic["eventLocation"].(map[string]interface{}); ok {
			eventLocation = val
		}
		if val, ok := basic["description"]; ok {
			description = val
		}
		if val, ok := basic["format"]; ok {
			if val == "OFFLINE" {
				format = "in-person"
			} else {
				format = strings.ToLower(val.(string))
			}
		}
		if val, ok := basic["primaryEventType"]; ok {
			primaryEventType = val
		}

		data := make(map[string]interface{})
		if startDate != nil {
			data["start_date"] = startDate
		} else {
			data["start_date"] = nil
		}
		if endDate != nil {
			data["end_date"] = endDate
		} else {
			data["end_date"] = nil
		}

		if eventLocation != nil {
			locationType, _ := eventLocation["locationType"].(string)

			if locationType == "VENUE" {
				if cityId, ok := eventLocation["cityId"]; ok {
					data["location_city_id"] = cityId
				} else {
					data["location_city_id"] = nil
				}
				if cityName, ok := eventLocation["cityName"]; ok {
					data["location_city"] = cityName
				} else {
					data["location_city"] = nil
				}
			} else {
				if locationId, ok := eventLocation["id"]; ok {
					data["location_city_id"] = locationId
				} else {
					data["location_city_id"] = nil
				}
				if locationName, ok := eventLocation["name"]; ok {
					data["location_city"] = locationName
				} else {
					data["location_city"] = nil
				}
			}

			if countryName, ok := eventLocation["countryName"]; ok {
				data["location_country"] = countryName
			} else {
				data["location_country"] = nil
			}
			if countryId, ok := eventLocation["countryId"]; ok {
				data["location_country_id"] = countryId
			} else {
				data["location_country_id"] = nil
			}
			if countryIso, ok := eventLocation["countryIso"]; ok {
				data["location_country_iso_code"] = countryIso
			} else {
				data["location_country_iso_code"] = nil
			}

			if stateName, ok := eventLocation["stateName"]; ok {
				data["location_state"] = stateName
			} else {
				data["location_state"] = nil
			}
			if stateId, ok := eventLocation["stateId"]; ok {
				data["location_state_id"] = stateId
			} else {
				data["location_state_id"] = nil
			}

			if locationType == "VENUE" {
				if venueName, ok := eventLocation["name"]; ok {
					data["venue_name"] = venueName
				} else {
					data["venue_name"] = nil
				}
				if address, ok := eventLocation["address"]; ok {
					data["venue_address"] = address
				} else {
					data["venue_address"] = nil
				}
				if venueLatitude, ok := eventLocation["latitude"]; ok {
					data["venue_latitude"] = venueLatitude
				} else {
					data["venue_latitude"] = nil
				}
				if venueLongitude, ok := eventLocation["longitude"]; ok {
					data["venue_longitude"] = venueLongitude
				} else {
					data["venue_longitude"] = nil
				}
				if venueId, ok := eventLocation["id"]; ok {
					data["venue_id"] = venueId
				} else {
					data["venue_id"] = nil
				}
			} else {
				data["venue_name"] = nil
				data["venue_address"] = nil
				data["venue_latitude"] = nil
				data["venue_longitude"] = nil
				data["venue_id"] = nil
			}

			if locationType == "VENUE" {
				if cityLatitude, ok := eventLocation["cityLatitude"]; ok {
					data["latitude"] = cityLatitude
				} else {
					data["latitude"] = nil
				}
				if cityLongitude, ok := eventLocation["cityLongitude"]; ok {
					data["longitude"] = cityLongitude
				} else {
					data["longitude"] = nil
				}
			} else {
				if latitude, ok := eventLocation["latitude"]; ok {
					data["latitude"] = latitude
				} else {
					data["latitude"] = nil
				}
				if longitude, ok := eventLocation["longitude"]; ok {
					data["longitude"] = longitude
				} else {
					data["longitude"] = nil
				}
			}
		} else {
			data["location_city_id"] = nil
			data["location_city"] = nil
			data["location_country"] = nil
			data["location_country_id"] = nil
			data["location_country_iso_code"] = nil
			data["location_state"] = nil
			data["location_state_id"] = nil
			data["venue_name"] = nil
			data["venue_address"] = nil
			data["venue_latitude"] = nil
			data["venue_longitude"] = nil
			data["venue_id"] = nil
			data["latitude"] = nil
			data["longitude"] = nil
		}

		if description != nil {
			data["description"] = description
		} else {
			data["description"] = nil
		}
		if format != nil {
			data["format"] = format
		} else {
			data["format"] = nil
		}
		if primaryEventType != nil {
			data["event_type"] = primaryEventType
		} else {
			data["event_type"] = nil
		}

		data["isMicroServiceData"] = true

		transformedEvent := map[string]interface{}{
			"id":   id,
			"name": name,
			"data": data,
		}

		transformedEvents = append(transformedEvents, transformedEvent)
	}

	return transformedEvents
}

func (s *SearchEventService) buildGroupByClause(fields []string) string {
	var groupByFields []string
	for _, field := range fields {
		fieldName := field
		if strings.Contains(field, " as ") {
			parts := strings.Split(field, " as ")
			fieldName = parts[1]
		} else {
			fieldName = strings.Replace(field, "ee.", "", 1)
		}
		groupByFields = append(groupByFields, fieldName)
	}
	return strings.Join(groupByFields, ",\n                        ")
}

func (s *SearchEventService) getAggregationDataClickHouse(filterFields models.FilterDataDto, pagination models.PaginationDto) (*AggregationResult, error) {
	ctx := context.Background()

	nestedQuery, err := s.sharedFunctionService.HandleNestedAggregation(filterFields, pagination)
	if err != nil {
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}

	log.Printf("Nested Aggregation Query: %s", nestedQuery)

	startTime := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, nestedQuery)
	if err != nil {
		log.Printf("ClickHouse aggregation query error: %v", err)
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}
	defer rows.Close()

	log.Printf("Default Aggregation query: %v", time.Since(startTime))

	var nestedData []map[string]interface{}

	for rows.Next() {
		columns := rows.Columns()
		values := make([]interface{}, len(columns))

		for i, col := range columns {
			if strings.HasSuffix(col, "Count") {
				values[i] = new(uint64)
			} else if strings.HasSuffix(col, "Data") {
				values[i] = new(interface{})
			} else {
				values[i] = new(string)
			}
		}

		if err := rows.Scan(values...); err != nil {
			log.Printf("Error scanning aggregation row: %v", err)
			continue
		}

		rowData := make(map[string]interface{})

		for i, col := range columns {
			if strings.HasSuffix(col, "Count") {
				if ptr, ok := values[i].(*uint64); ok && ptr != nil {
					rowData[col] = *ptr
				}
			} else if strings.HasSuffix(col, "Data") {
				if ptr, ok := values[i].(*interface{}); ok && ptr != nil {
					parsedData := s.parseClickHouseGroupArrayInterface(*ptr)
					rowData[col] = parsedData
				}
			} else {
				if ptr, ok := values[i].(*string); ok && ptr != nil {
					rowData[col] = *ptr
				}
			}
		}

		nestedData = append(nestedData, rowData)
	}

	if err := rows.Err(); err != nil {
		log.Printf("ClickHouse row iteration error: %v", err)
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}

	aggregationFields := s.extractAggregationFields(filterFields.ToAggregate)
	log.Printf("Aggregation fields: %v", aggregationFields)

	transformedData, err := s.transformDataService.transformAggregationDataToNested(nestedData, aggregationFields)
	if err != nil {
		log.Printf("Error transforming aggregation data: %v", err)
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}

	return &AggregationResult{StatusCode: 200, Response: transformedData}, nil
}

func (s *SearchEventService) extractAggregationFields(toAggregate string) []string {
	if toAggregate == "" {
		return []string{}
	}

	fields := strings.Split(toAggregate, ",")
	var result []string
	for _, field := range fields {
		trimmed := strings.TrimSpace(field)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func (s *SearchEventService) parseGenericEntry(entryString string, result *[]interface{}) {
	parts := strings.Split(entryString, "|||")
	if len(parts) >= 3 {
		fieldName := parts[0]
		if fieldCount, err := strconv.Atoi(parts[1]); err == nil {
			nestedData := parts[2]

			var parsedNestedData []interface{}
			nestedParts := strings.Fields(nestedData)
			for _, nestedPart := range nestedParts {
				if strings.Contains(nestedPart, "|") {
					nestedPartsArray := strings.Split(nestedPart, "|")
					if len(nestedPartsArray) >= 2 {
						nestedName := nestedPartsArray[0]
						if nestedCount, err := strconv.Atoi(nestedPartsArray[1]); err == nil {
							parsedNestedData = append(parsedNestedData, map[string]interface{}{
								"value": nestedName,
								"count": nestedCount,
							})
						}
					}
				}
			}

			*result = append(*result, map[string]interface{}{
				"field2Name":  fieldName,
				"field2Count": fieldCount,
				"field3Data":  parsedNestedData,
			})
		}
	}
}

func (s *SearchEventService) parseClickHouseGroupArrayInterface(data interface{}) []interface{} {
	if data == nil {
		return []interface{}{}
	}

	switch v := data.(type) {
	case []map[string]interface{}:
		var result []interface{}

		for i, item := range v {

			if field1Name, hasField1Name := item["field1Name"]; hasField1Name {
				field1Count := s.transformDataService.parseIntFromInterface(item["field1Count"])
				field2Data := item["field2Data"]
				var actualFieldName string
				for key, value := range item {
					if key != "field1Name" && key != "field1Count" && key != "field2Data" && key != "" {
						actualFieldName = fmt.Sprintf("%v", value)
						break
					}
				}

				if actualFieldName == "" {
					actualFieldName = fmt.Sprintf("%v", field1Name)
				}

				parsedField2Data := s.parseNestedFieldDataFromInterface(field2Data)

				result = append(result, map[string]interface{}{
					"field1Name":  actualFieldName,
					"field1Count": field1Count,
					"field2Data":  parsedField2Data,
				})
			} else if monthData, exists := item[""]; exists {
				parsedMonthData := s.parseNestedDataFromString(monthData)

				totalCount := 0
				for _, monthItem := range parsedMonthData {
					if monthMap, ok := monthItem.(map[string]interface{}); ok {
						if count, ok := monthMap["count"].(int); ok {
							totalCount += count
						}
					}
				}

				result = append(result, map[string]interface{}{
					"field1Name":  fmt.Sprintf("City_%d", i),
					"field1Count": totalCount,
					"field2Data":  parsedMonthData,
				})
			} else {
				var fieldName string
				var fieldCount int

				for key, value := range item {
					if key == "" {
						continue
					}
					if strings.HasSuffix(key, "Count") {
						fieldCount = s.transformDataService.parseIntFromInterface(value)
					} else if !strings.HasSuffix(key, "Data") {
						fieldName = fmt.Sprintf("%v", value)
					}
				}

				if fieldName != "" {
					result = append(result, map[string]interface{}{
						"field1Name":  fieldName,
						"field1Count": fieldCount,
						"field2Data":  []interface{}{},
					})
				}
			}
		}
		return result

	case []interface{}:
		var result []interface{}

		for _, item := range v {
			if tupleArray, ok := item.([]interface{}); ok {
				if len(tupleArray) >= 3 {
					field1Name := fmt.Sprintf("%v", tupleArray[0])
					field1Count := s.transformDataService.parseIntFromInterface(tupleArray[1])
					field2Data := tupleArray[2]
					field2DataStr := fmt.Sprintf("%v", field2Data)
					parsedField2Data := s.parseNestedDataFromString(field2DataStr)

					result = append(result, map[string]interface{}{
						"field1Name":  field1Name,
						"field1Count": field1Count,
						"field2Data":  parsedField2Data,
					})
				} else if len(tupleArray) >= 2 {
					field1Name := fmt.Sprintf("%v", tupleArray[0])
					field1Count := s.transformDataService.parseIntFromInterface(tupleArray[1])

					result = append(result, map[string]interface{}{
						"field1Name":  field1Name,
						"field1Count": field1Count,
						"field2Data":  []interface{}{},
					})
				} else {
					result = append(result, tupleArray)
				}
			} else {
				unwrapped := s.transformDataService.unwrapNestedArrays(item)
				if tupleArray, ok := unwrapped.([]interface{}); ok {
					if len(tupleArray) >= 3 {
						field1Name := fmt.Sprintf("%v", tupleArray[0])
						field1Count := s.transformDataService.parseIntFromInterface(tupleArray[1])
						field2Data := tupleArray[2]
						var parsedField2Data []interface{}
						if field2DataStr, ok := field2Data.(string); ok && strings.Contains(field2DataStr, "|||") {
							parsedField2Data = s.parseNestedDataFromString(field2DataStr)
						} else {
							parsedField2Data = s.parseNestedFieldDataFromInterface(field2Data)
						}

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": field1Count,
							"field2Data":  parsedField2Data,
						})
					} else if len(tupleArray) >= 2 {
						field1Name := fmt.Sprintf("%v", tupleArray[0])
						field1Count := s.transformDataService.parseIntFromInterface(tupleArray[1])

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": field1Count,
							"field2Data":  []interface{}{},
						})
					} else {
						result = append(result, tupleArray)
					}
				} else {
					result = append(result, item)
				}
			}
		}
		return result

	case string:
		return s.parseClickHouseStringArray(v)

	case []string:
		var result []interface{}
		for _, str := range v {
			if strings.Contains(str, "|||||") {
				parts := strings.Split(str, "|||||")
				if len(parts) >= 3 {
					cityName := parts[0]
					if count, err := strconv.Atoi(parts[1]); err == nil {
						monthData := parts[2]
						parsedMonthData := s.parseNestedDataFromString(monthData)

						result = append(result, map[string]interface{}{
							"field1Name":  cityName,
							"field1Count": count,
							"field2Data":  parsedMonthData,
						})
					}
				}
			} else if strings.Contains(str, "|||") {
				parts := strings.Split(str, "|||")
				if len(parts) >= 3 {
					cityName := parts[0]
					if count, err := strconv.Atoi(parts[1]); err == nil {
						monthData := parts[2]
						parsedMonthData := s.parseNestedDataFromString(monthData)

						result = append(result, map[string]interface{}{
							"field1Name":  cityName,
							"field1Count": count,
							"field2Data":  parsedMonthData,
						})
					}
				}
			} else {
				parts := strings.Split(str, "|")
				if len(parts) >= 2 {
					name := parts[0]
					if count, err := strconv.Atoi(parts[1]); err == nil {
						result = append(result, []interface{}{name, count})
					}
				}
			}
		}
		return result

	default:
		if str := fmt.Sprintf("%v", data); str != "" && str != "<nil>" {
			return s.parseClickHouseStringArray(str)
		}
		return []interface{}{}
	}
}

func (s *SearchEventService) parseNestedDataFromString(data interface{}) []interface{} {
	var result []interface{}

	switch v := data.(type) {
	case string:
		if strings.Contains(v, "|||") {
			parts := strings.Fields(v)
			var currentEntry strings.Builder

			for i, part := range parts {
				if strings.Contains(part, "|||") && strings.Count(part, "|||") >= 2 {
					if currentEntry.Len() > 0 {
						entryStr := strings.TrimSpace(currentEntry.String())
						s.parseGenericEntry(entryStr, &result)
						currentEntry.Reset()
					}
					currentEntry.WriteString(part)
				} else {
					if currentEntry.Len() > 0 {
						currentEntry.WriteString(" " + part)
					}
				}

				if i == len(parts)-1 && currentEntry.Len() > 0 {
					entryStr := strings.TrimSpace(currentEntry.String())
					s.parseGenericEntry(entryStr, &result)
				}
			}
		} else {
			parts := strings.Fields(v)
			for _, part := range parts {
				if strings.Contains(part, "|") {
					fieldParts := strings.Split(part, "|")
					if len(fieldParts) >= 2 {
						fieldName := fieldParts[0]
						if count, err := strconv.Atoi(fieldParts[1]); err == nil {
							result = append(result, map[string]interface{}{
								"value": fieldName,
								"count": count,
							})
						}
					}
				}
			}
		}
	case []string:
		for _, itemStr := range v {
			if strings.Contains(itemStr, "|") {
				fieldParts := strings.Split(itemStr, "|")
				if len(fieldParts) >= 2 {
					fieldName := fieldParts[0]
					if count, err := strconv.Atoi(fieldParts[1]); err == nil {
						result = append(result, map[string]interface{}{
							"value": fieldName,
							"count": count,
						})
					}
				}
			}
		}
	case []interface{}:
		for _, item := range v {
			if itemStr, ok := item.(string); ok {
				if strings.Contains(itemStr, "|") {
					fieldParts := strings.Split(itemStr, "|")
					if len(fieldParts) >= 2 {
						fieldName := fieldParts[0]
						if count, err := strconv.Atoi(fieldParts[1]); err == nil {
							result = append(result, map[string]interface{}{
								"value": fieldName,
								"count": count,
							})
						}
					}
				}
			}
		}
	}

	return result
}

func (s *SearchEventService) parseNestedFieldDataFromInterface(data interface{}) []interface{} {
	if data == nil {
		return []interface{}{}
	}
	if dataMap, ok := data.(map[string]interface{}); ok {
		if emptyKeyValue, exists := dataMap[""]; exists {
			return s.parseNestedFieldArrayFromInterface(emptyKeyValue)
		}
	}

	if dataArray, ok := data.([]interface{}); ok {
		return s.parseNestedFieldArrayFromInterface(dataArray)
	}

	if stringArray, ok := data.([]string); ok {
		var result []interface{}
		for _, str := range stringArray {
			parts := strings.Split(str, "|")
			if len(parts) >= 2 {
				fieldValue := strings.TrimSpace(parts[0])
				if fieldCount, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					result = append(result, map[string]interface{}{
						"value": fieldValue,
						"count": fieldCount,
					})
				}
			}
		}
		return result
	}

	return []interface{}{}
}

func (s *SearchEventService) parseNestedFieldArrayFromInterface(data interface{}) []interface{} {
	if data == nil {
		return []interface{}{}
	}

	var result []interface{}

	if dataArray, ok := data.([]interface{}); ok {
		for _, item := range dataArray {
			if itemStr, ok := item.(string); ok {
				var parts []string
				if strings.Contains(itemStr, "|") {
					parts = strings.Split(itemStr, "|")
				} else {
					parts = strings.Fields(itemStr)
				}

				if len(parts) >= 2 {
					fieldValue := strings.TrimSpace(parts[0])
					if fieldCount, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						result = append(result, map[string]interface{}{
							"value": fieldValue,
							"count": fieldCount,
						})
					}
				}
			}
		}
	} else if dataStr, ok := data.(string); ok {
		var parts []string
		if strings.Contains(dataStr, "|") {
			parts = strings.Split(dataStr, "|")
		} else {
			parts = strings.Fields(dataStr)
		}

		if len(parts) >= 2 {
			fieldValue := strings.TrimSpace(parts[0])
			if fieldCount, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				result = append(result, map[string]interface{}{
					"value": fieldValue,
					"count": fieldCount,
				})
			}
		}
	}

	return result
}

func (s *SearchEventService) parseClickHouseStringArray(str string) []interface{} {
	var result []interface{}

	if strings.Contains(str, ",") {
		pairs := strings.Split(str, ",")
		for _, pair := range pairs {
			trimmedPair := strings.TrimSpace(pair)
			if strings.Contains(trimmedPair, "|||||") {
				parts := strings.Split(trimmedPair, "|||||")
				if len(parts) >= 3 {
					field1Name := strings.TrimSpace(parts[0])
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						field2Data := strings.TrimSpace(parts[2])
						parsedField2Data := s.parseNestedDataFromString(field2Data)

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": count,
							"field2Data":  parsedField2Data,
						})
					}
				}
			} else if strings.Contains(trimmedPair, "|||") {
				parts := strings.Split(trimmedPair, "|||")
				if len(parts) >= 3 {
					field1Name := strings.TrimSpace(parts[0])
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						field2Data := strings.TrimSpace(parts[2])
						parsedField2Data := s.parseNestedDataFromString(field2Data)

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": count,
							"field2Data":  parsedField2Data,
						})
					}
				}
			} else {
				parts := strings.Split(trimmedPair, "|")
				if len(parts) >= 2 {
					name := strings.TrimSpace(parts[0])
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						result = append(result, []interface{}{name, count})
					}
				}
			}
		}
	} else if strings.Contains(str, " ") {
		pairs := strings.Fields(str)
		for _, pair := range pairs {
			if strings.Contains(pair, "|||") {
				parts := strings.Split(pair, "|||")
				if len(parts) >= 3 {
					field1Name := strings.TrimSpace(parts[0])
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						field2Data := strings.TrimSpace(parts[2])
						parsedField2Data := s.parseNestedDataFromString(field2Data)

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": count,
							"field2Data":  parsedField2Data,
						})
					}
				}
			} else {
				parts := strings.Split(pair, "|")
				if len(parts) >= 2 {
					name := strings.TrimSpace(parts[0])
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						result = append(result, []interface{}{name, count})
					}
				}
			}
		}
	} else if strings.Contains(str, "|||") {
		parts := strings.Split(str, "|||")
		if len(parts) >= 3 {
			field1Name := strings.TrimSpace(parts[0])
			if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				field2Data := strings.TrimSpace(parts[2])
				parsedField2Data := s.parseNestedDataFromString(field2Data)

				result = append(result, map[string]interface{}{
					"field1Name":  field1Name,
					"field1Count": count,
					"field2Data":  parsedField2Data,
				})
			}
		}
	} else if strings.Contains(str, "|") {
		parts := strings.Split(str, "|")
		if len(parts) >= 2 {
			name := strings.TrimSpace(parts[0])
			if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				result = append(result, []interface{}{name, count})
			}
		}
	}

	return result
}

type groupByHandler func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error)

func (s *SearchEventService) handleGroupByRequest(
	filterFields models.FilterDataDto,
	startTime time.Time,
	statusCode *int,
	errorMessage **string,
) (interface{}, error) {

	queryResult, err := s.sharedFunctionService.buildClickHouseQuery(filterFields)
	if err != nil {
		log.Printf("Error building ClickHouse query for groupBy: %v", err)
		*statusCode = http.StatusInternalServerError
		msg := err.Error()
		*errorMessage = &msg
		return nil, middleware.NewInternalServerError("Database query failed", err.Error())
	}

	cteAndJoinResult := s.sharedFunctionService.buildFilterCTEsAndJoins(
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

	groupByType := filterFields.ParsedGroupBy[0]

	if groupByType == models.CountGroupEventTypeGroup {
		if err := s.validateEventTypeGroupSearchByEntity(filterFields); err != nil {
			*statusCode = http.StatusBadRequest
			msg := err.Error()
			*errorMessage = &msg
			return nil, middleware.NewBadRequestError("Invalid searchByEntity for eventTypeGroup", msg)
		}
	}

	handler, err := s.getGroupByHandler(groupByType)
	if err != nil {
		*statusCode = http.StatusBadRequest
		msg := err.Error()
		*errorMessage = &msg
		return nil, middleware.NewBadRequestError("Unsupported groupBy option", msg)
	}

	count, err := handler(queryResult, &cteAndJoinResult, filterFields)
	if err != nil {
		log.Printf("Error executing groupBy handler for %s: %v", groupByType, err)
		*statusCode = http.StatusInternalServerError
		msg := err.Error()
		*errorMessage = &msg
		return nil, middleware.NewInternalServerError(fmt.Sprintf("Error getting event count by %s", groupByType), err.Error())
	}

	return s.sharedFunctionService.BuildGroupByResponse(count, startTime), nil
}

func (s *SearchEventService) validateEventTypeGroupSearchByEntity(filterFields models.FilterDataDto) error {
	searchByEntity := strings.ToLower(strings.TrimSpace(filterFields.SearchByEntity))
	validValues := map[string]bool{
		"":                             true,
		"event":                        true,
		"keywords":                     true,
		"eventestimatecount":           true,
		"economicimpactbreakdowncount": true,
		"company":                      true,
		"user":                         true,
		"speaker":                      true,
		"audience":                     true,
	}
	if searchByEntity != "" && !validValues[searchByEntity] {
		return fmt.Errorf("eventTypeGroup groupBy is only supported when searchByEntity is empty, 'event', 'keywords', 'eventEstimateCount', 'economicImpactBreakdownCount', 'company', 'user', 'speaker', or 'audience'")
	}
	return nil
}

func (s *SearchEventService) getGroupByHandler(groupByType models.CountGroup) (groupByHandler, error) {
	handlers := map[models.CountGroup]groupByHandler{
		models.CountGroupStatus: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByStatus(
				queryResult,
				cteAndJoinResult,
				filterFields,
			)
		},
		models.CountGroupEventTypeGroup: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByEventTypeGroup(
				queryResult,
				cteAndJoinResult,
				filterFields,
			)
		},
		models.CountGroupCountry: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByLocation(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"country",
			)
		},
		models.CountGroupState: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByLocation(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"state",
			)
		},
		models.CountGroupCity: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByLocation(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"city",
			)
		},
		models.CountGroupMonth: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByDate(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"month",
			)
		},
		models.CountGroupYear: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByDate(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"year",
			)
		},
		models.CountGroupDay: func(queryResult *ClickHouseQueryResult, cteAndJoinResult *CTEAndJoinResult, filterFields models.FilterDataDto) (interface{}, error) {
			return s.sharedFunctionService.GetEventCountByDate(
				queryResult,
				cteAndJoinResult,
				filterFields,
				"day",
			)
		},
	}

	handler, exists := handlers[groupByType]
	if !exists {
		return nil, fmt.Errorf("Unsupported groupBy option: %s", groupByType)
	}

	return handler, nil
}
