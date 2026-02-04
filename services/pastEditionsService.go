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

func buildPastEditionsCTE(editionTypes []string) string {
	if len(editionTypes) == 0 {
		return ""
	}
	inPlaceholders := strings.Repeat("?, ", len(editionTypes))
	inPlaceholders = strings.TrimSuffix(inPlaceholders, ", ")
	return fmt.Sprintf(`past_editions_cte AS (
		SELECT
			ee.edition_uuid,
			ee.edition_id,
			ee.start_date,
			ee.end_date,
			ee.status,
			ee.event_format
		FROM %s AS ee
		WHERE ee.edition_type IN (%s)
		  AND ee.event_id IN (SELECT event_id FROM %s WHERE event_uuid = ?)
	)`, alleventTable, inPlaceholders, alleventTable)
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
	cteSQL := buildPastEditionsCTE(editionTypes)

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
	var listRows []pastEditionRow
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

	listWithLocations, err := s.attachEventLocations(listRows, editionTypes)
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

func (s *PastEditionsService) runListQuery(ctx context.Context, query string, args ...interface{}) ([]pastEditionRow, error) {
	start := time.Now()
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	log.Printf("Past editions list duration: %v", time.Since(start))

	return scanPastEditionsRows(rows)
}

type pastEditionRow struct {
	EditionUUID string
	EditionID   uint32
	StartDate   time.Time
	EndDate     time.Time
	Status      string
	EventFormat *string
}

func scanPastEditionsRows(rows driver.Rows) ([]pastEditionRow, error) {
	var result []pastEditionRow
	for rows.Next() {
		var (
			editionUUID string
			editionID   uint32
			startDate   time.Time
			endDate     time.Time
			status      string
			eventFormat *string
		)
		if err := rows.Scan(&editionUUID, &editionID, &startDate, &endDate, &status, &eventFormat); err != nil {
			return nil, err
		}
		result = append(result, pastEditionRow{
			EditionUUID: editionUUID,
			EditionID:   editionID,
			StartDate:   startDate,
			EndDate:     endDate,
			Status:      status,
			EventFormat: eventFormat,
		})
	}
	return result, rows.Err()
}

func (s *PastEditionsService) attachEventLocations(rows []pastEditionRow, editionTypes []string) ([]models.PastEditionsBasic, error) {
	list := make([]models.PastEditionsBasic, 0, len(rows))
	if len(rows) == 0 {
		return list, nil
	}

	editionIDs := make([]uint32, 0, len(rows))
	for _, r := range rows {
		editionIDs = append(editionIDs, r.EditionID)
	}
	if len(editionTypes) == 0 {
		editionTypes = []string{"past_edition"}
	}
	filterFields := models.FilterDataDto{ParsedEditionType: editionTypes}
	locations, err := s.sharedFunctionService.GetEventLocations(editionIDs, filterFields, true)
	if err != nil {
		return nil, err
	}

	for _, r := range rows {
		eventLocation := locations[fmt.Sprintf("%d", r.EditionID)]
		item := models.PastEditionsBasic{
			Id:            r.EditionUUID,
			EventLocation: eventLocation,
			Status:        r.Status,
			Format:        formatForResponse(r.EventFormat),
			Start:         r.StartDate.Format("2006-01-02"),
			End:           r.EndDate.Format("2006-01-02"),
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
