package services

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"search-event-go/models"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const alleventTable = "testing_db.allevent_ch"

type PastEditionsService struct {
	clickhouseService     *ClickHouseService
	sharedFunctionService *SharedFunctionService
}

func NewPastEditionsService(clickhouseService *ClickHouseService, sharedFunctionService *SharedFunctionService) *PastEditionsService {
	return &PastEditionsService{
		clickhouseService:     clickhouseService,
		sharedFunctionService: sharedFunctionService,
	}
}

type PastEditionsResult struct {
	Data  []models.PastEditionsBasic `json:"data"`
	Count int                        `json:"count"`
}

func buildPastEditionsCTE(editionTypes []string, locationSelectSQL string) string {
	if len(editionTypes) == 0 {
		return ""
	}
	inPlaceholders := strings.Repeat("?, ", len(editionTypes))
	inPlaceholders = strings.TrimSuffix(inPlaceholders, ", ")
	locPart := ""
	if strings.TrimSpace(locationSelectSQL) != "" {
		locPart = ",\n\t\t\t" + locationSelectSQL
	}
	return fmt.Sprintf(`past_editions_cte AS (
		SELECT
			ee.edition_uuid,
			ee.edition_id,
			ee.start_date,
			ee.end_date,
			ee.status,
			ee.event_format%s
		FROM %s AS ee
		WHERE ee.edition_type IN (%s)
		  AND ee.event_id IN (SELECT event_id FROM %s WHERE event_uuid = ?)
	)`, locPart, alleventTable, inPlaceholders, alleventTable)
}

func buildCountQuery(editionTypes []string) string {
	if len(editionTypes) == 0 {
		return fmt.Sprintf(`SELECT count(*) FROM %s WHERE event_id IN (SELECT event_id FROM %s WHERE event_uuid = ?)`, alleventTable, alleventTable)
	}
	inPlaceholders := strings.Repeat("?, ", len(editionTypes))
	inPlaceholders = strings.TrimSuffix(inPlaceholders, ", ")
	return fmt.Sprintf(`SELECT count(*) FROM %s WHERE edition_type IN (%s) AND event_id IN (SELECT event_id FROM %s WHERE event_uuid = ?)`, alleventTable, inPlaceholders, alleventTable)
}

func compactQuery(q string) string {
	q = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(q), " ")
	return q
}

func queryForLog(query string, args ...interface{}) string {
	for _, a := range args {
		var s string
		switch v := a.(type) {
		case string:
			s = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case int:
			s = fmt.Sprintf("%d", v)
		default:
			s = fmt.Sprint(a)
		}
		query = strings.Replace(query, "?", s, 1)
	}
	return query
}

func (s *PastEditionsService) GetPastEditions(eventId string, limit, offset int, editionTypes []string) (*PastEditionsResult, error) {
	ctx := context.Background()
	if len(editionTypes) == 0 {
		editionTypes = []string{"past_edition"}
	}
	locExprs := s.sharedFunctionService.AlleventLocationDenormalizedSelectExprs()
	locationSelectSQL := strings.Join(locExprs, ",\n\t\t\t")
	cteSQL := buildPastEditionsCTE(editionTypes, locationSelectSQL)

	countQuery := buildCountQuery(editionTypes)
	listQuery := fmt.Sprintf("WITH %s SELECT * FROM past_editions_cte LIMIT ? OFFSET ?", cteSQL)

	countArgs := make([]interface{}, 0, len(editionTypes)+1)
	for _, et := range editionTypes {
		countArgs = append(countArgs, et)
	}
	countArgs = append(countArgs, eventId)

	listArgs := make([]interface{}, 0, len(editionTypes)+1+2)
	for _, et := range editionTypes {
		listArgs = append(listArgs, et)
	}
	listArgs = append(listArgs, eventId, limit, offset)

	log.Printf("Past editions count query: %s", queryForLog(countQuery, countArgs...))
	log.Printf("Past editions list query: %s", queryForLog(listQuery, listArgs...))

	var count int
	var listRows []map[string]interface{}
	var countErr, listErr error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		count, countErr = s.runCountQuery(ctx, countQuery, countArgs...)
	}()

	go func() {
		defer wg.Done()
		listRows, listErr = s.runListQuery(ctx, listQuery, listArgs...)
	}()

	wg.Wait()

	if countErr != nil {
		log.Printf("Past editions count query error: %v", countErr)
		return nil, countErr
	}
	if listErr != nil {
		log.Printf("Past editions list query error: %v", listErr)
		return nil, listErr
	}

	listWithLocations, err := s.attachEventLocations(listRows)
	if err != nil {
		log.Printf("Past editions attach event locations error: %v", err)
		return nil, err
	}

	return &PastEditionsResult{
		Data:  listWithLocations,
		Count: count,
	}, nil
}

func (s *PastEditionsService) runCountQuery(ctx context.Context, query string, args ...interface{}) (int, error) {
	start := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	log.Printf("Past editions count duration: %v", time.Since(start))

	var total uint64
	if rows.Next() {
		if err := rows.Scan(&total); err != nil {
			return 0, err
		}
	}
	return int(total), nil
}

func (s *PastEditionsService) runListQuery(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	start := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	log.Printf("Past editions list duration: %v", time.Since(start))

	return scanPastEditionsRows(rows)
}

func pastEditionsScanTargets(columns []string) []interface{} {
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		switch col {
		case "edition_id", "loc_venue_id", "loc_venue_city", "loc_edition_city", "loc_edition_city_state_id":
			values[i] = new(uint32)
		case "start_date", "end_date":
			values[i] = new(time.Time)
		case "event_format":
			values[i] = new(*string)
		case "loc_region":
			values[i] = new([]string)
		case "loc_venue_lat", "loc_venue_long", "loc_edition_city_lat", "loc_edition_city_long", "loc_edition_country_latitude", "loc_edition_country_longitude", "loc_edition_city_state_latitude", "loc_edition_city_state_longitude":
			values[i] = new(*float64)
		default:
			values[i] = new(string)
		}
	}
	return values
}

func pastEditionsRowToMap(columns []string, values []interface{}) map[string]interface{} {
	row := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		val := values[i]
		switch col {
		case "edition_uuid":
			if p, ok := val.(*string); ok && p != nil {
				row[col] = strings.TrimSpace(*p)
			}
		case "edition_id":
			if p, ok := val.(*uint32); ok && p != nil {
				row["edition_id"] = *p
			}
		case "start_date", "end_date":
			if p, ok := val.(*time.Time); ok && p != nil {
				row[col] = *p
			}
		case "status":
			if p, ok := val.(*string); ok && p != nil {
				row[col] = *p
			}
		case "event_format":
			if p, ok := val.(**string); ok && p != nil {
				row[col] = *p
			}
		case "loc_venue_lat", "loc_venue_long", "loc_edition_city_lat", "loc_edition_city_long", "loc_edition_country_latitude", "loc_edition_country_longitude", "loc_edition_city_state_latitude", "loc_edition_city_state_longitude":
			if pp, ok := val.(**float64); ok && pp != nil && *pp != nil {
				row[col] = **pp
			} else {
				row[col] = nil
			}
		case "loc_region":
			if arrPtr, ok := val.(*[]string); ok && arrPtr != nil && *arrPtr != nil {
				row[col] = *arrPtr
			} else {
				row[col] = nil
			}
		case "loc_venue_id", "loc_venue_city", "loc_edition_city", "loc_edition_city_state_id":
			if p, ok := val.(*uint32); ok && p != nil {
				row[col] = *p
			}
		default:
			if ptr, ok := val.(*string); ok && ptr != nil {
				if strings.TrimSpace(*ptr) == "" {
					row[col] = nil
				} else {
					row[col] = *ptr
				}
			}
		}
	}
	return row
}

func scanPastEditionsRows(rows driver.Rows) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	for rows.Next() {
		columns := rows.Columns()
		values := pastEditionsScanTargets(columns)
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		result = append(result, pastEditionsRowToMap(columns, values))
	}
	return result, rows.Err()
}

func (s *PastEditionsService) attachEventLocations(rows []map[string]interface{}) ([]models.PastEditionsBasic, error) {
	list := make([]models.PastEditionsBasic, 0, len(rows))
	for _, row := range rows {
		eventLocation := s.sharedFunctionService.BuildEventLocationFromAlleventRow(row)
		var editionUUID string
		if u, ok := row["edition_uuid"].(string); ok {
			editionUUID = u
		}
		var startStr, endStr string
		if t, ok := row["start_date"].(time.Time); ok {
			startStr = t.Format("2006-01-02")
		}
		if t, ok := row["end_date"].(time.Time); ok {
			endStr = t.Format("2006-01-02")
		}
		var status string
		if st, ok := row["status"].(string); ok {
			status = st
		}
		var formatPtr *string
		if fp, ok := row["event_format"].(*string); ok {
			formatPtr = fp
		}
		item := models.PastEditionsBasic{
			Id:            editionUUID,
			EventLocation: eventLocation,
			Status:        status,
			Format:        formatForResponse(formatPtr),
			Start:         startStr,
			End:           endStr,
		}
		list = append(list, item)
	}
	return list, nil
}

func formatForResponse(dbFormat *string) *string {
	if dbFormat == nil || strings.TrimSpace(*dbFormat) == "" {
		return nil
	}
	s := strings.ToLower(strings.TrimSpace(*dbFormat))
	if s == "offline" {
		out := "in-person"
		return &out
	}
	return &s
}
