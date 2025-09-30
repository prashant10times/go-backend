package services

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"search-event-go/middleware"
	"search-event-go/models"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type SearchEventService struct {
	db                    *gorm.DB
	sharedFunctionService *SharedFunctionService
	clickhouseService     *ClickHouseService
}

func NewSearchEventService(db *gorm.DB, sharedFunctionService *SharedFunctionService, clickhouseService *ClickHouseService) *SearchEventService {
	return &SearchEventService{
		db:                    db,
		sharedFunctionService: sharedFunctionService,
		clickhouseService:     clickhouseService,
	}
}

func (s *SearchEventService) GetEventDataV2(userId, apiId string, filterFields models.FilterDataDto, pagination models.PaginationDto, responseFields models.ResponseDataDto, c *fiber.Ctx) (interface{}, error) {
	startTime := time.Now()
	ipAddress := c.IP()
	statusCode := 200
	var errorMessage *string

	log.Printf("Starting GetEventDataV2 for user %s, api %s", userId, apiId)

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

	// Handle panic as internal server error
	defer func() {
		if r := recover(); r != nil {
			responseTime := time.Since(startTime).Seconds()
			log.Printf("Panic recovered: %v", r)
			errorResponse := fiber.Map{
				"statusCode": http.StatusInternalServerError,
				"message":    "INTERNAL_SERVER_ERROR",
				"data": fiber.Map{
					"message": "An unexpected error occurred",
				},
				"meta": fiber.Map{
					"responseTime": responseTime,
				},
			}
			log.Printf("Error response: %v", errorResponse)
		}
	}()

	log.Printf("User ID: %v", userId)

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

	log.Printf("Starting quota and filter verification")
	result, err := s.sharedFunctionService.quotaAndFilterVerification(userId, apiId)
	if err != nil {
		log.Printf("Quota and filter verification failed: %v", err)
		statusCode = http.StatusTooManyRequests
		msg := "Daily API limit exceeded"
		errorMessage = &msg
		return nil, middleware.NewTooManyRequestsError("Daily API limit exceeded", err.Error())
	}
	log.Printf("Quota and filter verification successful")

	allowedFilters = result.AllowedFilters
	allowedAdvancedParameters = result.AllowedAdvancedParameters

	var requestedFilters []string
	v := reflect.ValueOf(filterFields)
	t := reflect.TypeOf(filterFields)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		if len(fieldType.Name) >= 6 && fieldType.Name[:6] == "Parsed" {
			continue
		}
		if fieldType.Name == "View" || fieldType.Name == "Radius" || fieldType.Name == "Unit" || fieldType.Name == "EventDistanceOrder" || fieldType.Name == "Q" {
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
		found := false
		for _, allowed := range allowedFilters {
			if filter == allowed {
				found = true
				break
			}
		}
		if !found {
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
		for _, advancedKey := range advancedKeys {
			if filter == advancedKey {
				requestedAdvancedParameters = append(requestedAdvancedParameters, filter)
				break
			}
		}
	}
	var unauthorizedAdvancedParameters []string
	for _, parameter := range requestedAdvancedParameters {
		found := false
		for _, allowed := range allowedAdvancedParameters {
			if parameter == allowed {
				found = true
				break
			}
		}
		if !found {
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
		for _, allowed := range allowedAdvancedParameters {
			if parameter == allowed {
				selectedAdvancedKeys = append(selectedAdvancedKeys, parameter)
				break
			}
		}
	}

	var requiredFields []string
	requiredFields = append(requiredFields, basicKeys...)
	requiredFields = append(requiredFields, selectedAdvancedKeys...)

	// Parse sort fields
	sortClause, err := s.sharedFunctionService.parseSortFields(pagination.Sort, filterFields)
	if err != nil {
		log.Printf("Error parsing sort fields: %v", err)
		statusCode = http.StatusBadRequest
		msg := err.Error()
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid sort fields", err.Error())
	}

	// Determine query type
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
	case "DEFAULT_LIST":
		result, err := s.getDefaultListData(pagination, sortClause)
		if err != nil {
			log.Printf("Error getting default list data: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database query failed", err.Error())
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

		response, err = s.sharedFunctionService.BuildClickhouseListViewResponse(eventDataSlice, pagination, c)
		if err != nil {
			log.Printf("Error building response: %v", err)
			return nil, err
		}

	case "FILTERED_LIST":
		result, err := s.getFilteredListData(pagination, sortClause, filterFields)
		if err != nil {
			log.Printf("Error getting filtered list data: %v", err)
			statusCode = http.StatusInternalServerError
			msg := err.Error()
			errorMessage = &msg
			return nil, middleware.NewInternalServerError("Database query failed", err.Error())
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

		response, err = s.sharedFunctionService.BuildClickhouseListViewResponse(eventDataSlice, pagination, c)
		if err != nil {
			log.Printf("Error building response: %v", err)
			return nil, err
		}

	case "DEFAULT_AGGREGATION":
		result, err := s.getDefaultAggregationDataClickHouse(filterFields, pagination)
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

type ListResult struct {
	Data         interface{}
	StatusCode   int
	ErrorMessage string
}

type AggregationResult struct {
	StatusCode int
	Response   interface{}
	Error      error
}

func (s *SearchEventService) getDefaultListData(pagination models.PaginationDto, sortClause []SortClause) (*ListResult, error) {

	baseFields := []string{
		"ee.event_id",
		"ee.start_date",
		"ee.end_date",
		"ee.event_name",
		"ee.edition_city_name",
		"ee.edition_country",
		"ee.event_description",
		"ee.event_followers",
		"ee.event_logo",
		"ee.event_avgRating",
	}

	sortFields := make(map[string]bool)
	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			sortFields[sort.Field] = true
		}
	}

	mappedFields := make(map[string]bool)
	for field := range sortFields {
		switch field {
		case "exhibitors":
			mappedFields["event_exhibitor"] = true
		case "speakers":
			mappedFields["event_speaker"] = true
		case "sponsors":
			mappedFields["event_sponsor"] = true
		case "created":
			mappedFields["event_created"] = true
		case "following":
			mappedFields["event_followers"] = true
		}
	}

	var conditionalFields []string
	if sortFields["exhibitors"] || mappedFields["event_exhibitor"] || sortFields["event_exhibitor"] {
		conditionalFields = append(conditionalFields, "ee.event_exhibitor")
	}
	if sortFields["speakers"] || mappedFields["event_speaker"] || sortFields["event_speaker"] {
		conditionalFields = append(conditionalFields, "ee.event_speaker")
	}
	if sortFields["sponsors"] || mappedFields["event_sponsor"] || sortFields["event_sponsor"] {
		conditionalFields = append(conditionalFields, "ee.event_sponsor")
	}
	if sortFields["created"] || mappedFields["event_created"] || sortFields["event_created"] {
		conditionalFields = append(conditionalFields, "ee.event_created")
	}

	requiredFieldsStatic := append(baseFields, conditionalFields...)

	// Build group by clause - simple replacement of 'ee.' prefix
	var groupByFields []string
	for _, field := range requiredFieldsStatic {
		groupByFields = append(groupByFields, strings.Replace(field, "ee.", "", 1))
	}
	groupByClause := strings.Join(groupByFields, ",\n                        ")

	orderByClause, err := s.sharedFunctionService.buildOrderByClause(sortClause, false)
	if err != nil {
		return nil, err
	}

	today := time.Now().Format("2006-01-02")

	fieldsString := strings.Join(requiredFieldsStatic, ", ")
	eventDataQuery := fmt.Sprintf(`
		WITH event_filter AS (
			SELECT event_id, edition_id
			FROM testing_db.event_edition_ch
			WHERE published = '1' 
			AND status != 'U'
			AND edition_type = 'current_edition'
			AND end_date >= '%s'
			GROUP BY event_id, edition_id
			ORDER BY event_id ASC
			LIMIT %d OFFSET %d
		),
		event_data AS (
			SELECT %s
			FROM testing_db.event_edition_ch AS ee
			WHERE ee.edition_id in (SELECT edition_id from event_filter)
			GROUP BY
				%s
			ORDER BY ee.event_id ASC
		)
		SELECT %s
		FROM event_data
		GROUP BY
			%s
		%s
	`, today, pagination.Limit, pagination.Offset, fieldsString, groupByClause, groupByClause, groupByClause, s.sharedFunctionService.fixOrderByForCTE(orderByClause, false))

	log.Printf("Event data query: %s", eventDataQuery)

	eventDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), eventDataQuery)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	var eventIds []uint32
	var eventData []map[string]interface{}
	rowCount := 0

	for eventDataResult.Next() {
		rowCount++

		columns := eventDataResult.Columns()

		values := make([]interface{}, len(columns))
		for i, col := range columns {
			switch col {
			case "event_id", "event_followers":
				values[i] = new(uint32)
			case "start_date", "end_date":
				values[i] = new(time.Time)
			case "event_name", "edition_city_name", "edition_country", "event_description", "event_logo":
				values[i] = new(string)
			case "event_avgRating":
				values[i] = new(*decimal.Decimal)
			default:
				values[i] = new(string)
			}
		}

		if err := eventDataResult.Scan(values...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowData := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			switch col {
			case "event_id":
				if eventID, ok := val.(*uint32); ok && eventID != nil {
					eventIds = append(eventIds, *eventID)
					rowData["event_id"] = *eventID
				}
			case "start_date", "end_date":
				if dateVal, ok := val.(*time.Time); ok && dateVal != nil {
					rowData[col] = dateVal.Format("2006-01-02")
				}
			case "event_avgRating":
				if avgRating, ok := val.(**decimal.Decimal); ok && avgRating != nil && *avgRating != nil {
					rowData[col] = *avgRating
				} else {
					rowData[col] = 0.0
				}
			default:
				if ptr, ok := val.(*string); ok && ptr != nil {
					rowData[col] = *ptr
				} else if ptr, ok := val.(*uint32); ok && ptr != nil {
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
		}, nil
	}

	var eventIdsStr []string
	for _, id := range eventIds {
		eventIdsStr = append(eventIdsStr, fmt.Sprintf("%d", id))
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")
	relatedDataQuery := fmt.Sprintf(`
		SELECT 
			event AS event_id,
			'category' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 1
		GROUP BY event

		UNION ALL

		SELECT 
			event AS event_id,
			'tags' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 0
		GROUP BY event

		UNION ALL

		SELECT 
			event_id,
			'types' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_type_ch
		WHERE event_id IN (%s)
		GROUP BY event_id
	`, eventIdsStrJoined, eventIdsStrJoined, eventIdsStrJoined)

	// Execute related data query
	relatedDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), relatedDataQuery)
	if err != nil {
		log.Printf("Related data query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Process related data
	categoriesMap := make(map[string]string)
	tagsMap := make(map[string]string)
	typesMap := make(map[string]string)

	for relatedDataResult.Next() {
		var eventID uint32
		var dataType, value string

		if err := relatedDataResult.Scan(&eventID, &dataType, &value); err != nil {
			log.Printf("Error scanning related data row: %v", err)
			continue
		}

		eventIDStr := fmt.Sprintf("%d", eventID)

		switch dataType {
		case "category":
			categoriesMap[eventIDStr] = value
		case "tags":
			tagsMap[eventIDStr] = value
		case "types":
			typesMap[eventIDStr] = value
		}
	}

	// Combine event data with related data
	var combinedData []map[string]interface{}
	for _, event := range eventData {
		eventID := fmt.Sprintf("%d", event["event_id"])
		combinedEvent := make(map[string]interface{})
		for k, v := range event {
			combinedEvent[k] = v
		}
		combinedEvent["category"] = categoriesMap[eventID]
		combinedEvent["tags"] = tagsMap[eventID]
		combinedEvent["type"] = typesMap[eventID]

		combinedData = append(combinedData, combinedEvent)
	}

	renamedData, err := s.sharedFunctionService.clickHouseResponseNameChange(combinedData)
	if err != nil {
		log.Printf("Error renaming response data: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &ListResult{
		StatusCode: 200,
		Data:       renamedData,
	}, nil
}

func (s *SearchEventService) getFilteredListData(pagination models.PaginationDto, sortClause []SortClause, filterFields models.FilterDataDto) (*ListResult, error) {
	baseFields := []string{
		"ee.event_id as id",
		"ee.start_date as start",
		"ee.end_date as end",
		"ee.event_name as name",
		"ee.edition_city_name as city",
		"ee.edition_country as country",
		"ee.event_description as description",
		"ee.event_followers as followers",
		"ee.event_logo as logo",
		"ee.event_avgRating as avgRating",
	}

	sortFields := make(map[string]bool)
	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			sortFields[sort.Field] = true
		}
	}

	mappedFields := make(map[string]bool)
	for field := range sortFields {
		switch field {
		case "exhibitors":
			mappedFields["event_exhibitor"] = true
		case "speakers":
			mappedFields["event_speaker"] = true
		case "sponsors":
			mappedFields["event_sponsor"] = true
		case "created":
			mappedFields["event_created"] = true
		case "following":
			mappedFields["event_followers"] = true
		}
	}

	var conditionalFields []string
	if sortFields["exhibitors"] || mappedFields["event_exhibitor"] || sortFields["event_exhibitor"] {
		conditionalFields = append(conditionalFields, "ee.event_exhibitor as exhibitors")
	}
	if sortFields["speakers"] || mappedFields["event_speaker"] || sortFields["event_speaker"] {
		conditionalFields = append(conditionalFields, "ee.event_speaker as speakers")
	}
	if sortFields["sponsors"] || mappedFields["event_sponsor"] || sortFields["event_sponsor"] {
		conditionalFields = append(conditionalFields, "ee.event_sponsor as sponsors")
	}
	if sortFields["created"] || mappedFields["event_created"] || sortFields["event_created"] {
		conditionalFields = append(conditionalFields, "ee.event_created as created")
	}

	requiredFieldsStatic := append(baseFields, conditionalFields...)

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

	orderByClause, err := s.sharedFunctionService.buildOrderByClause(sortClause, queryResult.NeedsAnyJoin)
	if err != nil {
		log.Printf("Error building order by clause: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	finalOrderClause := queryResult.DistanceOrderClause
	if finalOrderClause == "" {
		finalOrderClause = orderByClause
	}

	if queryResult.DistanceOrderClause != "" && strings.Contains(queryResult.DistanceOrderClause, "greatCircleDistance") {
		if strings.Contains(queryResult.DistanceOrderClause, "lat") && strings.Contains(queryResult.DistanceOrderClause, "lon") {
			conditionalFields = append(conditionalFields, "ee.edition_city_lat as lat")
			conditionalFields = append(conditionalFields, "ee.edition_city_long as lon")
		}
		if strings.Contains(queryResult.DistanceOrderClause, "venueLat") && strings.Contains(queryResult.DistanceOrderClause, "venueLon") {
			conditionalFields = append(conditionalFields, "ee.venue_lat as venueLat")
			conditionalFields = append(conditionalFields, "ee.venue_long as venueLon")
		}
	}

	requiredFieldsStatic = append(baseFields, conditionalFields...)

	// Build CTEs and joins
	cteAndJoinResult := s.sharedFunctionService.buildFilterCTEsAndJoins(
		queryResult.NeedsVisitorJoin,
		queryResult.NeedsSpeakerJoin,
		queryResult.NeedsExhibitorJoin,
		queryResult.NeedsSponsorJoin,
		queryResult.NeedsCategoryJoin,
		queryResult.NeedsTypeJoin,
		queryResult.VisitorWhereConditions,
		queryResult.SpeakerWhereConditions,
		queryResult.ExhibitorWhereConditions,
		queryResult.SponsorWhereConditions,
		queryResult.CategoryWhereConditions,
		queryResult.TypeWhereConditions,
	)

	// Check for end date filters
	hasEndDateFilters := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
		filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != ""

	fieldsString := strings.Join(requiredFieldsStatic, ", ")
	finalGroupByClause := s.buildGroupByClause(requiredFieldsStatic)

	today := time.Now().Format("2006-01-02")

	// Build CTE clauses string
	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	// Build join conditions string
	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	eventDataQuery := fmt.Sprintf(`
		WITH %sevent_filter AS (
			SELECT event_id, edition_id
			FROM testing_db.event_edition_ch AS ee
			WHERE published = '1' 
			AND status != 'U'
			AND edition_type = 'current_edition'
			%s
			%s
			%s
			%s
			GROUP BY event_id, edition_id
			ORDER BY event_id ASC
			LIMIT %d OFFSET %d
		),
		event_data AS (
			SELECT %s
			FROM testing_db.event_edition_ch AS ee
			WHERE ee.edition_id in (SELECT edition_id from event_filter)
			GROUP BY
				%s
			ORDER BY ee.event_id ASC
		)
		SELECT %s
		FROM event_data
		GROUP BY
			%s
		%s
	`,
		cteClausesStr,
		func() string {
			if !hasEndDateFilters {
				return fmt.Sprintf("AND end_date >= '%s'", today)
			}
			return ""
		}(),
		func() string {
			if queryResult.WhereClause != "" {
				return fmt.Sprintf("AND %s", queryResult.WhereClause)
			}
			return ""
		}(),
		func() string {
			if queryResult.SearchClause != "" {
				return fmt.Sprintf("AND %s", queryResult.SearchClause)
			}
			return ""
		}(),
		joinConditionsStr,
		pagination.Limit, pagination.Offset,
		fieldsString, finalGroupByClause, finalGroupByClause, finalGroupByClause,
		s.sharedFunctionService.fixOrderByForCTE(finalOrderClause, true))

	eventDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), eventDataQuery)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	var eventIds []uint32
	var eventData []map[string]interface{}
	rowCount := 0

	for eventDataResult.Next() {
		rowCount++

		columns := eventDataResult.Columns()

		// Create appropriate data type containers for each column
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			switch col {
			case "id", "followers":
				values[i] = new(uint32)
			case "start", "end":
				values[i] = new(time.Time)
			case "name", "city", "country", "description", "logo":
				values[i] = new(string)
			case "avgRating":
				values[i] = new(*decimal.Decimal)
			case "lat", "lon", "venueLat", "venueLon":
				values[i] = new(float64)
			case "exhibitors", "speakers", "sponsors":
				values[i] = new(uint32)
			default:
				values[i] = new(string)
			}
		}

		if err := eventDataResult.Scan(values...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowData := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			switch col {
			case "id":
				if eventID, ok := val.(*uint32); ok && eventID != nil {
					eventIds = append(eventIds, *eventID)
					rowData["id"] = *eventID
				}
			case "start", "end":
				if dateVal, ok := val.(*time.Time); ok && dateVal != nil {
					rowData[col] = dateVal.Format("2006-01-02")
				}
			case "avgRating":
				if avgRating, ok := val.(**decimal.Decimal); ok && avgRating != nil && *avgRating != nil {
					rowData[col] = *avgRating
				} else {
					rowData[col] = 0.0
				}
			default:
				if ptr, ok := val.(*string); ok && ptr != nil {
					rowData[col] = *ptr
				} else if ptr, ok := val.(*uint32); ok && ptr != nil {
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
		}, nil
	}

	// Build related data query
	var eventIdsStr []string
	for _, id := range eventIds {
		eventIdsStr = append(eventIdsStr, fmt.Sprintf("%d", id))
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")

	relatedDataQuery := fmt.Sprintf(`
		SELECT 
			event AS event_id,
			'category' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 1
		GROUP BY event

		UNION ALL

		SELECT 
			event AS event_id,
			'tags' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 0
		GROUP BY event

		UNION ALL

		SELECT 
			event_id,
			'types' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value
		FROM testing_db.event_type_ch
		WHERE event_id IN (%s)
		GROUP BY event_id
	`, eventIdsStrJoined, eventIdsStrJoined, eventIdsStrJoined)

	relatedDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), relatedDataQuery)
	if err != nil {
		log.Printf("Related data query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Process related data
	categoriesMap := make(map[string]string)
	tagsMap := make(map[string]string)
	typesMap := make(map[string]string)

	for relatedDataResult.Next() {
		var eventID uint32
		var dataType, value string

		if err := relatedDataResult.Scan(&eventID, &dataType, &value); err != nil {
			log.Printf("Error scanning related data row: %v", err)
			continue
		}

		eventIDStr := fmt.Sprintf("%d", eventID)

		switch dataType {
		case "category":
			categoriesMap[eventIDStr] = value
		case "tags":
			tagsMap[eventIDStr] = value
		case "types":
			typesMap[eventIDStr] = value
		}
	}

	// Combine event data with related data
	var combinedData []map[string]interface{}
	for _, event := range eventData {
		eventID := fmt.Sprintf("%d", event["id"])
		combinedEvent := make(map[string]interface{})
		for k, v := range event {
			combinedEvent[k] = v
		}
		combinedEvent["category"] = categoriesMap[eventID]
		combinedEvent["tags"] = tagsMap[eventID]
		combinedEvent["type"] = typesMap[eventID]

		combinedData = append(combinedData, combinedEvent)
	}

	return &ListResult{
		StatusCode: 200,
		Data:       combinedData,
	}, nil
}

// buildGroupByClause builds the GROUP BY clause from the field list
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

func (s *SearchEventService) getDefaultAggregationDataClickHouse(filterFields models.FilterDataDto, pagination models.PaginationDto) (*AggregationResult, error) {
	ctx := context.Background()

	nestedQuery, err := s.sharedFunctionService.HandleNestedAggregation(filterFields, pagination)
	if err != nil {
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}

	log.Printf("Nested Aggregation Query: %s", nestedQuery)
	log.Printf("Query length: %d characters", len(nestedQuery))

	startTime := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, nestedQuery)
	if err != nil {
		log.Printf("ClickHouse aggregation query error: %v", err)
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}
	defer rows.Close()

	log.Printf("Default Aggregation query executed in: %v", time.Since(startTime))

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
		log.Printf("Processed row %d: %+v", len(nestedData), rowData)
	}

	log.Printf("Finished processing all rows. Total rows: %d", len(nestedData))
	// Check for any errors that occurred during row iteration
	if err := rows.Err(); err != nil {
		log.Printf("ClickHouse row iteration error: %v", err)
		return &AggregationResult{StatusCode: 500, Error: err}, err
	}

	log.Printf("No row iteration errors found. Processed %d rows", len(nestedData))
	aggregationFields := s.extractAggregationFields(filterFields.ToAggregate)
	log.Printf("Aggregation fields: %v", aggregationFields)

	transformedData, err := s.sharedFunctionService.transformAggregationDataToNested(nestedData, aggregationFields)
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
	// Parse format: "field1|||field1Count|||field2Data"
	parts := strings.Split(entryString, "|||")
	if len(parts) >= 3 {
		fieldName := parts[0]
		if fieldCount, err := strconv.Atoi(parts[1]); err == nil {
			nestedData := parts[2]

			// Parse the nested field data
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
		// Handle ClickHouse groupArray format where tuples are converted to maps
		var result []interface{}

		for i, item := range v {

			// Check if this is a tuple converted to map format (field1Name, field1Count, field2Data)
			if field1Name, hasField1Name := item["field1Name"]; hasField1Name {
				field1Count := s.parseIntFromInterface(item["field1Count"])
				field2Data := item["field2Data"]

				// The issue is that field1Name is coming as "City_0" instead of the actual city name
				// This suggests the ClickHouse driver is not properly parsing the tuple
				// Let's try to extract the actual field name from the map keys
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

				// Parse the nested field data
				parsedField2Data := s.parseNestedFieldDataFromInterface(field2Data)

				result = append(result, map[string]interface{}{
					"field1Name":  actualFieldName,
					"field1Count": field1Count,
					"field2Data":  parsedField2Data,
				})
			} else if monthData, exists := item[""]; exists {
				// Parse the month data directly from the string format
				parsedMonthData := s.parseNestedDataFromString(monthData)

				// Calculate total count from month data
				totalCount := 0
				for _, monthItem := range parsedMonthData {
					if monthMap, ok := monthItem.(map[string]interface{}); ok {
						if count, ok := monthMap["count"].(int); ok {
							totalCount += count
						}
					}
				}

				// For this case, we need to extract city name from somewhere else
				// The city name is missing from this structure, so we'll use a placeholder
				result = append(result, map[string]interface{}{
					"field1Name":  fmt.Sprintf("City_%d", i),
					"field1Count": totalCount,
					"field2Data":  parsedMonthData,
				})
			} else {
				// Try to extract field name and count from the map keys
				var fieldName string
				var fieldCount int

				for key, value := range item {
					if key == "" {
						continue // Skip empty key
					}
					if strings.HasSuffix(key, "Count") {
						fieldCount = s.parseIntFromInterface(value)
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
		// Handle deeply nested arrays like [[[tuple]]]
		var result []interface{}

		for _, item := range v {
			// Handle tuple format directly: ('Online',8935,['2025-10|4013','2025-11|1956',...])
			if tupleArray, ok := item.([]interface{}); ok {
				// Handle ClickHouse tuple format: [field1, field1Count, field2Data]
				if len(tupleArray) >= 3 {
					field1Name := fmt.Sprintf("%v", tupleArray[0])
					field1Count := s.parseIntFromInterface(tupleArray[1])
					field2Data := tupleArray[2]

					// For 4-level data, use parseNestedDataFromString to handle the complex nested structure
					field2DataStr := fmt.Sprintf("%v", field2Data)
					parsedField2Data := s.parseNestedDataFromString(field2DataStr)

					result = append(result, map[string]interface{}{
						"field1Name":  field1Name,
						"field1Count": field1Count,
						"field2Data":  parsedField2Data,
					})
				} else if len(tupleArray) >= 2 {
					// Handle simpler tuple format: [field1, field1Count]
					field1Name := fmt.Sprintf("%v", tupleArray[0])
					field1Count := s.parseIntFromInterface(tupleArray[1])

					result = append(result, map[string]interface{}{
						"field1Name":  field1Name,
						"field1Count": field1Count,
						"field2Data":  []interface{}{},
					})
				} else {
					result = append(result, tupleArray)
				}
			} else {
				// Unwrap nested arrays to get to the actual tuple
				unwrapped := s.unwrapNestedArrays(item)

				if tupleArray, ok := unwrapped.([]interface{}); ok {
					// Handle ClickHouse tuple format: [field1, field1Count, field2Data]
					if len(tupleArray) >= 3 {
						field1Name := fmt.Sprintf("%v", tupleArray[0])
						field1Count := s.parseIntFromInterface(tupleArray[1])
						field2Data := tupleArray[2]

						// For 4-level data, use parseNestedDataFromString to handle the complex "|||||" format
						var parsedField2Data []interface{}
						if field2DataStr, ok := field2Data.(string); ok && strings.Contains(field2DataStr, "|||") {
							parsedField2Data = s.parseNestedDataFromString(field2DataStr)
						} else {
							// Fallback to the original parsing for simpler formats
							parsedField2Data = s.parseNestedFieldDataFromInterface(field2Data)
						}

						result = append(result, map[string]interface{}{
							"field1Name":  field1Name,
							"field1Count": field1Count,
							"field2Data":  parsedField2Data,
						})
					} else if len(tupleArray) >= 2 {
						// Handle simpler tuple format: [field1, field1Count]
						field1Name := fmt.Sprintf("%v", tupleArray[0])
						field1Count := s.parseIntFromInterface(tupleArray[1])

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
			// Handle the 4-level format: "cityName|||||cityCount|||||monthData"
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
				// Handle the 3-level format: "cityName|||count|||monthData"
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
				// Handle the old format: "name|count"
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

	// Handle different data types that might contain nested field data
	switch v := data.(type) {
	case string:
		if strings.Contains(v, "|||") {
			// Parse entries separated by spaces, each entry in format: "field1|||count1|||field2Data"
			parts := strings.Fields(v)
			var currentEntry strings.Builder

			for i, part := range parts {
				if strings.Contains(part, "|||") && strings.Count(part, "|||") >= 2 {
					// This is the start of a new entry
					if currentEntry.Len() > 0 {
						// Process the previous entry
						entryStr := strings.TrimSpace(currentEntry.String())
						s.parseGenericEntry(entryStr, &result)
						currentEntry.Reset()
					}
					currentEntry.WriteString(part)
				} else {
					// This is part of the current entry's nested data
					if currentEntry.Len() > 0 {
						currentEntry.WriteString(" " + part)
					}
				}

				// If this is the last part, process the current entry
				if i == len(parts)-1 && currentEntry.Len() > 0 {
					entryStr := strings.TrimSpace(currentEntry.String())
					s.parseGenericEntry(entryStr, &result)
				}
			}
		} else {
			// Parse 3-level format like "field1|count1 field2|count2 field3|count3"
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
		// Handle []string format - this is what ClickHouse is actually sending
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
		// Handle array format
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

func (s *SearchEventService) unwrapNestedArrays(data interface{}) interface{} {
	current := data

	// Keep unwrapping while we have arrays
	for {
		if arr, ok := current.([]interface{}); ok && len(arr) > 0 {
			current = arr[0]
		} else {
			break
		}
	}

	return current
}

func (s *SearchEventService) parseIntFromInterface(value interface{}) int {
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

func (s *SearchEventService) parseNestedFieldDataFromInterface(data interface{}) []interface{} {
	if data == nil {
		return []interface{}{}
	}

	// Handle map with empty key containing nested field data like map[:[value count] [value count]]
	if dataMap, ok := data.(map[string]interface{}); ok {
		if emptyKeyValue, exists := dataMap[""]; exists {
			return s.parseNestedFieldArrayFromInterface(emptyKeyValue)
		}
	}

	// Handle direct array format - this is the main case for tuple data
	if dataArray, ok := data.([]interface{}); ok {
		return s.parseNestedFieldArrayFromInterface(dataArray)
	}

	// Handle string array format
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
				// Parse format like "value|count" (pipe-separated) or "value count" (space-separated)
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
		// Handle single string format
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
			// Handle the 4-level format: "field1Name|||||field1Count|||||field2Data"
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
				// Handle the 3-level format: "field1Name|||count|||field2Data"
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
				// Handle the old format: "name|count"
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
			// Handle the new format: "field1Name|||count|||field2Data"
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
				// Handle the old format: "name|count"
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
		// Handle the new format: "field1Name|||count|||field2Data"
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
		// Handle the old format: "name|count"
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
