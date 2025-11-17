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
	cfg                   *config.Config
}

func NewSearchEventService(db *gorm.DB, sharedFunctionService *SharedFunctionService, clickhouseService *ClickHouseService, cfg *config.Config) *SearchEventService {
	return &SearchEventService{
		db:                    db,
		sharedFunctionService: sharedFunctionService,
		clickhouseService:     clickhouseService,
		cfg:                   cfg,
	}
}

func (s *SearchEventService) GetEventDataV2(userId, apiId string, filterFields models.FilterDataDto, pagination models.PaginationDto, responseFields models.ResponseDataDto, c *fiber.Ctx) (any, error) {
	startTime := time.Now()
	ipAddress := c.IP()
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

	// Parse sort fields
	sortClause, err := s.sharedFunctionService.parseSortFields(pagination.Sort, filterFields)
	if err != nil {
		log.Printf("Error parsing sort fields: %v", err)
		statusCode = http.StatusBadRequest
		msg := err.Error()
		errorMessage = &msg
		return nil, middleware.NewBadRequestError("Invalid sort fields", err.Error())
	}

	// // original api Logic
	// validatedParameters, err := s.sharedFunctionService.validateParameters(filterFields)
	// if err != nil {
	// 	log.Printf("Error validating parameters: %v", err)
	// 	statusCode = http.StatusBadRequest
	// 	msg := err.Error()
	// 	errorMessage = &msg
	// 	return nil, middleware.NewBadRequestError("Invalid parameters", err.Error())
	// }

	// log.Printf("Validated parameters: %v", validatedParameters)

	// typeArray := strings.Split(filterFields.Type, ",")
	// if filterFields.Type != "" && len(typeArray) == 1 && typeArray[0] == s.cfg.AlertId {
	// 	// Build base alert params
	// 	baseParams := models.AlertSearchParams{
	// 		EventIds:  validatedParameters.ParsedEventIds,
	// 		StartDate: &validatedParameters.ActiveGte,
	// 		EndDate:   &validatedParameters.ActiveLte,
	// 	}

	// 	if validatedParameters.ParsedViewBound != nil {
	// 		if validatedParameters.ParsedViewBound.BoundType == models.BoundTypePoint {
	// 			var geoCoords models.GeoCoordinates
	// 			if err := json.Unmarshal(validatedParameters.ParsedViewBound.Coordinates, &geoCoords); err == nil {
	// 				baseParams.Coordinates = &geoCoords
	// 			}
	// 		}
	// 	}

	// 	type alertResult struct {
	// 		Data  interface{}
	// 		Error error
	// 	}

	// 	countChan := make(chan alertResult, 1)
	// 	alertsChan := make(chan alertResult, 1)

	// 	countParams := baseParams
	// 	countParams.Required = "count"
	// 	go func() {
	// 		countData, err := s.getAlerts(countParams)
	// 		countChan <- alertResult{Data: countData, Error: err}
	// 	}()

	// 	eventsParams := baseParams
	// 	eventsParams.Required = "events"
	// 	limit := pagination.Limit
	// 	offset := pagination.Offset
	// 	eventsParams.Limit = &limit
	// 	eventsParams.Offset = &offset

	// 	go func() {
	// 		alertsData, err := s.getAlerts(eventsParams)
	// 		alertsChan <- alertResult{Data: alertsData, Error: err}
	// 	}()

	// 	countResult := <-countChan
	// 	alertsResult := <-alertsChan

	// 	if countResult.Error != nil || alertsResult.Error != nil {
	// 		var errMsg string
	// 		if countResult.Error != nil {
	// 			errMsg = countResult.Error.Error()
	// 		} else if alertsResult.Error != nil {
	// 			errMsg = alertsResult.Error.Error()
	// 		} else {
	// 			errMsg = "Unknown error"
	// 		}
	// 		statusCode = http.StatusBadRequest
	// 		msg := errMsg
	// 		errorMessage = &msg
	// 		return nil, middleware.NewBadRequestError("Alert request failed", errMsg)
	// 	}

	// 	count := 0
	// 	events := []interface{}{}

	// 	if countMap, ok := countResult.Data.(map[string]interface{}); ok {
	// 		if c, exists := countMap["count"]; exists {
	// 			if cFloat, ok := c.(float64); ok {
	// 				count = int(cFloat)
	// 			} else if cInt, ok := c.(int); ok {
	// 				count = cInt
	// 			}
	// 		}
	// 	}

	// 	if alertsMap, ok := alertsResult.Data.(map[string]interface{}); ok {
	// 		if e, exists := alertsMap["events"]; exists {
	// 			if eventsSlice, ok := e.([]interface{}); ok {
	// 				events = eventsSlice
	// 			}
	// 		}
	// 	}

	// 	responseData := fiber.Map{
	// 		"count":  count,
	// 		"events": events,
	// 	}

	// 	responseTime := time.Since(startTime).Seconds()
	// 	successResponse := fiber.Map{
	// 		"status":     "success",
	// 		"statusCode": statusCode,
	// 		"meta": fiber.Map{
	// 			"responseTime": responseTime,
	// 			"pagination": fiber.Map{
	// 				"page": (pagination.Offset / pagination.Limit) + 1,
	// 			},
	// 		},
	// 		"data": responseData,
	// 	}
	// 	return successResponse, nil
	// }

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
		result, err := s.getListData(pagination, sortClause, filterFields)
		if err != nil {
			log.Printf("Error getting list data: %v", err)
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

// getAlerts calls the alerts API with the given parameters
func (s *SearchEventService) getAlerts(params models.AlertSearchParams) (interface{}, error) {
	// TODO: Implement the actual HTTP request to the alerts API
	// This is a placeholder - replace with actual implementation
	// Example structure:
	//
	// reqBody := map[string]interface{}{
	//     "eventIds": alertParams.EventIds,
	//     "startDate": alertParams.StartDate,
	//     "endDate": alertParams.EndDate,
	//     "required": required,
	// }
	// if required == "events" {
	//     reqBody["sortBy"] = sortBy
	//     reqBody["limit"] = limit
	//     reqBody["offset"] = offset
	// }
	//
	// Make HTTP request and return response

	// Placeholder return
	if params.Required == "count" {
		return map[string]interface{}{
			"count": 0,
		}, nil
	}
	return map[string]interface{}{
		"events": []interface{}{},
	}, nil
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

func (s *SearchEventService) getListData(pagination models.PaginationDto, sortClause []SortClause, filterFields models.FilterDataDto) (*ListResult, error) {
	baseFields := []string{
		"ee.event_uuid as id",
		"ee.event_id",
		"ee.start_date as start",
		"ee.end_date as end",
		"ee.event_name as name",
		"ee.edition_city_name as city",
		"ee.edition_country as country",
		"ee.event_description as description",
		"ee.event_followers as followers",
		"ee.event_logo as logo",
		"ee.event_avgRating as avgRating",
		"ee.exhibitors_lower_bound as exhibitors_lower_bound",
		"ee.exhibitors_upper_bound as exhibitors_upper_bound",
	}

	isEconomicImpactRangeFilter := false
	if filterFields.EconomicImpactGte != "" || filterFields.EconomicImpactLte != "" {
		isEconomicImpactRangeFilter = true
	}

	if filterFields.EventEstimate || !!isEconomicImpactRangeFilter {
		baseFields = append(baseFields, "ee.event_economic_value as economicImpact")
		baseFields = append(baseFields, "ee.event_economic_breakdown as economicImpactBreakdown")
	}

	if filterFields.ImpactScoreGte != "" || filterFields.ImpactScoreLte != "" {
		baseFields = append(baseFields, "ee.impactScore as impactScore")
	}

	baseFieldMap := make(map[string]bool)
	for _, field := range baseFields {
		fieldName := strings.Replace(field, "ee.", "", 1)
		if strings.Contains(fieldName, " as ") {
			parts := strings.Split(fieldName, " as ")
			fieldName = strings.TrimSpace(parts[0])
		}
		baseFieldMap[fieldName] = true
	}

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
	}

	var conditionalFields []string
	conditionalFieldMap := make(map[string]bool)
	if len(sortClause) > 0 {
		for _, sort := range sortClause {
			if sort.Field != "" {
				if !baseFieldMap[sort.Field] && !conditionalFieldMap[sort.Field] {
					alias := dbToAliasMap[sort.Field]
					if alias == "" {
						alias = sort.Field
					}
					conditionalFields = append(conditionalFields, fmt.Sprintf("ee.%s as %s", sort.Field, alias))
					conditionalFieldMap[sort.Field] = true
				}
			}
		}
	}
	if len(filterFields.ParsedAudienceZone) > 0 {
		conditionalFields = append(conditionalFields, "ee.audienceZone as audienceZone")
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

	eventFilterOrderBy, err := s.sharedFunctionService.buildOrderByClause(sortClause, false)
	if err != nil {
		log.Printf("Error building event filter order by clause: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}
	if eventFilterOrderBy == "" {
		eventFilterOrderBy = "ORDER BY event_score ASC"
	} else {
		eventFilterOrderBy = strings.TrimPrefix(eventFilterOrderBy, "ORDER BY ")
		eventFilterOrderBy = strings.ReplaceAll(eventFilterOrderBy, "ee.", "")
		eventFilterOrderBy = "ORDER BY " + eventFilterOrderBy
	}

	eventFilterSelectFields := []string{"event_id", "edition_id"}
	eventFilterGroupByFields := []string{"event_id", "edition_id"}
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
					eventFilterSelectFields = append(eventFilterSelectFields, sort.Field)
					eventFilterGroupByFields = append(eventFilterGroupByFields, sort.Field)
				}
			}
		}
	}
	eventFilterSelectStr := strings.Join(eventFilterSelectFields, ", ")
	eventFilterGroupByStr := strings.Join(eventFilterGroupByFields, ", ")

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
		queryResult.VisitorWhereConditions,
		queryResult.SpeakerWhereConditions,
		queryResult.ExhibitorWhereConditions,
		queryResult.SponsorWhereConditions,
		queryResult.CategoryWhereConditions,
		queryResult.TypeWhereConditions,
		queryResult.EventRankingWhereConditions,
		queryResult.JobCompositeWhereConditions,
		queryResult.AudienceSpreadWhereConditions,
		filterFields,
	)

	hasEndDateFilters := filterFields.EndGte != "" || filterFields.EndLte != "" || filterFields.EndGt != "" || filterFields.EndLt != "" ||
		filterFields.ActiveGte != "" || filterFields.ActiveLte != "" || filterFields.ActiveGt != "" || filterFields.ActiveLt != "" ||
		filterFields.CreatedAt != "" || len(filterFields.ParsedEventIds) > 0 || len(filterFields.ParsedNotEventIds) > 0 || len(filterFields.ParsedSourceEventIds) > 0 || len(filterFields.ParsedDates) > 0 || filterFields.ParsedPastBetween != nil || filterFields.ParsedActiveBetween != nil

	fieldsString := strings.Join(requiredFieldsStatic, ", ")
	finalGroupByClause := s.buildGroupByClause(requiredFieldsStatic)

	innerOrderBy := func() string {
		if finalOrderClause != "" {
			return s.sharedFunctionService.fixOrderByForCTE(finalOrderClause, true)
		}
		return "ORDER BY score ASC"
	}()

	today := time.Now().Format("2006-01-02")

	cteClausesStr := ""
	if len(cteAndJoinResult.CTEClauses) > 0 {
		cteClausesStr = strings.Join(cteAndJoinResult.CTEClauses, ",\n                ") + ",\n                "
	}

	joinConditionsStr := ""
	if len(cteAndJoinResult.JoinConditions) > 0 {
		joinConditionsStr = fmt.Sprintf("AND %s", strings.Join(cteAndJoinResult.JoinConditions, " AND "))
	}

	eventDataQuery := fmt.Sprintf(`
		WITH %sevent_filter AS (
			SELECT %s
			FROM testing_db.allevent_ch AS ee
			WHERE %s 
			AND %s
			AND edition_type = 'current_edition'
			%s
			%s
			%s
			%s
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
		SELECT %s
		FROM event_data
		GROUP BY
			%s
		%s
	`,
		cteClausesStr,
		eventFilterSelectStr,
		s.sharedFunctionService.buildPublishedCondition(filterFields),
		s.sharedFunctionService.buildStatusCondition(filterFields),
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
		eventFilterGroupByStr,
		eventFilterOrderBy,
		pagination.Limit, pagination.Offset,
		fieldsString, finalGroupByClause, innerOrderBy,
		finalGroupByClause, finalGroupByClause, s.sharedFunctionService.fixOrderByForCTE(finalOrderClause, true))

	log.Printf("Event data query: %s", eventDataQuery)

	eventDataQueryTime := time.Now()
	eventDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), eventDataQuery)
	if err != nil {
		log.Printf("ClickHouse query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	eventDataQueryDuration := time.Since(eventDataQueryTime)
	log.Printf("Event data query time: %v", eventDataQueryDuration)

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
			case "event_id", "followers", "impactScore", "exhibitors", "speakers", "sponsors", "exhibitors_lower_bound", "exhibitors_upper_bound", "estimatedExhibitors", "inboundScore", "internationalScore", "inboundAttendance", "internationalAttendance":
				values[i] = new(uint32)
			case "score":
				values[i] = new(int32)
			case "id", "name", "city", "country", "description", "logo", "economicImpactBreakdown":
				values[i] = new(string)
			case "start", "end":
				values[i] = new(time.Time)
			case "avgRating":
				values[i] = new(*decimal.Decimal)
			case "lat", "lon", "venueLat", "venueLon", "economicImpact":
				values[i] = new(float64)
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
					rowData[col] = *followers
					if currentEventID != 0 {
						eventFollowersMap[currentEventID] = *followers
					}
				}
			case "id":
				if eventUUID, ok := val.(*string); ok && eventUUID != nil {
					rowData["id"] = *eventUUID
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
			case "estimatedExhibitors":
				if estimatedExhibitors, ok := val.(*uint32); ok && estimatedExhibitors != nil {
					rowData[col] = *estimatedExhibitors
				} else {
					rowData[col] = uint32(0)
				}
			case "economicImpact":
				if economicValue, ok := val.(*float64); ok && economicValue != nil {
					formattedValue := s.sharedFunctionService.formatCurrency(*economicValue)
					rowData[col] = formattedValue
				} else {
					rowData[col] = "$0"
				}
			case "economicImpactBreakdown":
				if economicBreakdown, ok := val.(*string); ok && economicBreakdown != nil {
					var jsonData interface{}
					if err := json.Unmarshal([]byte(*economicBreakdown), &jsonData); err == nil {
						rowData[col] = jsonData
					} else {
						rowData[col] = *economicBreakdown
					}
				} else {
					rowData[col] = "{}"
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

	var eventIdsStr []string
	for _, id := range eventIds {
		eventIdsStr = append(eventIdsStr, fmt.Sprintf("%d", id))
	}
	eventIdsStrJoined := strings.Join(eventIdsStr, ",")

	relatedDataQuery := fmt.Sprintf(`
		WITH current_events AS (
    		SELECT edition_id
    		FROM testing_db.allevent_ch
    		WHERE event_id IN (%s)
			AND edition_type = 'current_edition'
		)	
		SELECT 
			event AS event_id,
			'category' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(category_uuid), ', ') AS uuid_value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 1
		GROUP BY event

		UNION ALL

		SELECT 
			event AS event_id,
			'tags' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(category_uuid), ', ') AS uuid_value
		FROM testing_db.event_category_ch
		WHERE event IN (%s) 
		  AND is_group = 0
		GROUP BY event

		UNION ALL

		SELECT 
			event_id,
			'types' AS data_type,
			arrayStringConcat(groupArray(name), ', ') AS value,
			arrayStringConcat(groupArray(eventtype_uuid), ', ') AS uuid_value
		FROM testing_db.event_type_ch
		WHERE event_id IN (%s)
		GROUP BY event_id
	`, eventIdsStrJoined, eventIdsStrJoined, eventIdsStrJoined, eventIdsStrJoined)

	if len(filterFields.ParsedJobComposite) > 0 && queryResult.needsDesignationJoin {
		escapedDesignations := make([]string, len(filterFields.ParsedJobComposite))
		for i, designation := range filterFields.ParsedJobComposite {
			escapedDesignations[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(designation, "'", "''"))
		}
		designationsStr := strings.Join(escapedDesignations, ",")

		relatedDataQuery += fmt.Sprintf(`
		UNION ALL

		SELECT 
			event_id,
			'jobComposite' AS data_type,
			concat(display_name, '|||', role, '|||', department) AS value,
			'' AS uuid_value
		FROM testing_db.event_designation_ch
		WHERE edition_id IN (SELECT edition_id FROM current_events) 
			AND display_name IN (%s)
			AND total_visitors >= 5
		ORDER BY event_id, total_visitors DESC
		`, designationsStr)
	}
	if len(filterFields.ParsedAudienceSpread) > 0 && queryResult.needsAudienceSpreadJoin {
		relatedDataQuery += `
		UNION ALL

		SELECT 
			event_id,
			'countrySpread' AS data_type,
			CAST(country_data as String) AS value,
			'' AS uuid_value
		FROM testing_db.event_visitorSpread_ch
		ARRAY JOIN user_by_cntry AS country_data
		WHERE event_id IN (` + eventIdsStrJoined + `)
		ORDER BY event_id, JSONExtractInt(CAST(country_data as String), 'total_count') DESC
		LIMIT 5 BY event_id
		`
	}

	if len(filterFields.ParsedAudienceSpread) > 0 && queryResult.needsAudienceSpreadJoin {
		relatedDataQuery += `
		UNION ALL

		SELECT 
			event_id,
			'designationSpread' AS data_type,
			CAST(designation_data as String) AS value,
			'' AS uuid_value
		FROM testing_db.event_visitorSpread_ch
		ARRAY JOIN user_by_designation AS designation_data
		WHERE event_id IN (` + eventIdsStrJoined + `)
		ORDER BY event_id, JSONExtractInt(CAST(designation_data as String), 'total_count') DESC
		`
	}

	log.Printf("Related data query: %s", relatedDataQuery)
	relatedDataQueryTime := time.Now()
	relatedDataResult, err := s.clickhouseService.ExecuteQuery(context.Background(), relatedDataQuery)
	if err != nil {
		log.Printf("Related data query error: %v", err)
		return &ListResult{
			StatusCode:   500,
			ErrorMessage: err.Error(),
		}, nil
	}

	relatedDataQueryDuration := time.Since(relatedDataQueryTime)
	log.Printf("Related data query time: %v", relatedDataQueryDuration)

	categoriesMap := make(map[string][]map[string]string)
	tagsMap := make(map[string][]map[string]string)
	typesMap := make(map[string][]map[string]string)
	jobCompositeMap := make(map[string][]map[string]string)
	countrySpreadMap := make(map[string][]map[string]interface{})
	designationSpreadMap := make(map[string][]map[string]interface{})

	for relatedDataResult.Next() {
		var eventID uint32
		var dataType, value, uuidValue string

		if err := relatedDataResult.Scan(&eventID, &dataType, &value, &uuidValue); err != nil {
			log.Printf("Error scanning related data row: %v", err)
			continue
		}

		eventIDStr := fmt.Sprintf("%d", eventID)

		switch dataType {
		case "category":
			names := strings.Split(value, ", ")
			uuids := strings.Split(uuidValue, ", ")

			var items []map[string]string
			for i, name := range names {
				if i < len(uuids) && name != "" {
					items = append(items, map[string]string{
						"id":   strings.TrimSpace(uuids[i]),
						"name": strings.TrimSpace(name),
					})
				}
			}
			categoriesMap[eventIDStr] = items
		case "tags":
			names := strings.Split(value, ", ")
			uuids := strings.Split(uuidValue, ", ")

			var items []map[string]string
			for i, name := range names {
				if i < len(uuids) && name != "" {
					items = append(items, map[string]string{
						"id":   strings.TrimSpace(uuids[i]),
						"name": strings.TrimSpace(name),
					})
				}
			}
			tagsMap[eventIDStr] = items
		case "types":
			names := strings.Split(value, ", ")
			uuids := strings.Split(uuidValue, ", ")

			var items []map[string]string
			for i, name := range names {
				if i < len(uuids) && name != "" {
					items = append(items, map[string]string{
						"id":   strings.TrimSpace(uuids[i]),
						"name": strings.TrimSpace(name),
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
		case "countrySpread":
			var countryData map[string]interface{}
			if err := json.Unmarshal([]byte(value), &countryData); err == nil {
				transformedCountryData := make(map[string]interface{})
				if cntryId, ok := countryData["cntry_id"]; ok {
					transformedCountryData["iso"] = cntryId

					if isoStr, ok := cntryId.(string); ok {
						countryInfo := s.sharedFunctionService.GetCountryDataByISO(isoStr)
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
					if followers, exists := eventFollowersMap[eventID]; exists && followers > 0 {
						if userCount, ok := totalCount.(float64); ok && userCount > 0 {
							percentage = math.Round((userCount/float64(followers))*100*100) / 100
						}
					}
					transformedCountryData["percentage"] = percentage
				}
				countrySpreadMap[eventIDStr] = append(countrySpreadMap[eventIDStr], transformedCountryData)
			}
		case "designationSpread":
			var designationData map[string]interface{}
			if err := json.Unmarshal([]byte(value), &designationData); err == nil {
				transformedDesignationData := make(map[string]interface{})
				if designation, ok := designationData["designation"]; ok {
					transformedDesignationData["designation"] = designation
				}
				if totalCount, ok := designationData["total_count"]; ok {
					transformedDesignationData["count"] = totalCount
					percentage := 0.0
					if followers, exists := eventFollowersMap[eventID]; exists && followers > 0 {
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
	}

	var combinedData []map[string]interface{}
	for _, event := range eventData {
		eventID := fmt.Sprintf("%d", event["event_id"])
		combinedEvent := make(map[string]interface{})
		for k, v := range event {
			combinedEvent[k] = v
		}
		combinedEvent["categories"] = categoriesMap[eventID]
		combinedEvent["tags"] = tagsMap[eventID]
		combinedEvent["types"] = typesMap[eventID]

		if jobCompositeData, ok := jobCompositeMap[eventID]; ok && len(jobCompositeData) > 0 {
			var jobCompositeArray []map[string]interface{}
			for _, designation := range jobCompositeData {
				jobCompositeArray = append(jobCompositeArray, map[string]interface{}{
					"name":       designation["name"],
					"role":       designation["role"],
					"department": designation["department"],
				})
			}
			combinedEvent["jobComposite"] = jobCompositeArray
		}

		audienceData := make(map[string]interface{})
		if countrySpreadData, ok := countrySpreadMap[eventID]; ok && len(countrySpreadData) > 0 {
			audienceData["countrySpread"] = countrySpreadData
		}
		if designationSpreadData, ok := designationSpreadMap[eventID]; ok && len(designationSpreadData) > 0 {
			audienceData["designationSpread"] = designationSpreadData
		}

		if len(audienceData) > 0 {
			combinedEvent["audience"] = audienceData
		}

		if lowerBound, ok := event["exhibitors_lower_bound"].(uint32); ok {
			if upperBound, ok := event["exhibitors_upper_bound"].(uint32); ok {
				if lowerBound > 0 || upperBound > 0 {
					combinedEvent["estimatedExhibitors"] = fmt.Sprintf("%d-%d", lowerBound, upperBound)
				} else {
					combinedEvent["estimatedExhibitors"] = "0-0"
				}
				delete(combinedEvent, "exhibitors_lower_bound")
				delete(combinedEvent, "exhibitors_upper_bound")
			}
		} else {
			combinedEvent["estimatedExhibitors"] = "0-0"
		}
		delete(combinedEvent, "event_id")
		combinedData = append(combinedData, combinedEvent)
	}

	return &ListResult{
		StatusCode: 200,
		Data:       combinedData,
	}, nil
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
				field1Count := s.sharedFunctionService.parseIntFromInterface(item["field1Count"])
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
						fieldCount = s.sharedFunctionService.parseIntFromInterface(value)
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
					field1Count := s.sharedFunctionService.parseIntFromInterface(tupleArray[1])
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
					field1Count := s.sharedFunctionService.parseIntFromInterface(tupleArray[1])

					result = append(result, map[string]interface{}{
						"field1Name":  field1Name,
						"field1Count": field1Count,
						"field2Data":  []interface{}{},
					})
				} else {
					result = append(result, tupleArray)
				}
			} else {
				unwrapped := s.sharedFunctionService.unwrapNestedArrays(item)
				if tupleArray, ok := unwrapped.([]interface{}); ok {
					if len(tupleArray) >= 3 {
						field1Name := fmt.Sprintf("%v", tupleArray[0])
						field1Count := s.sharedFunctionService.parseIntFromInterface(tupleArray[1])
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
						field1Count := s.sharedFunctionService.parseIntFromInterface(tupleArray[1])

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
