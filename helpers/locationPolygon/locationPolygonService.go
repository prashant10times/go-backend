package locationpolygon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/services"
	"strings"
)

type LocationPolygonService struct {
	clickhouseService *services.ClickHouseService
}

func NewLocationPolygonService(clickhouseService *services.ClickHouseService) *LocationPolygonService {
	return &LocationPolygonService{clickhouseService: clickhouseService}
}

func (s *LocationPolygonService) GetLocationPolygons(eventId string) (interface{}, error) {
	ctx := context.Background()
	escapedEventId := strings.ReplaceAll(eventId, "'", "''")
	selectQuery := fmt.Sprintf(`
		SELECT polygon
		FROM testing_db.location_polygons_ch
		WHERE tableId = '%s' AND tableName = 'Alerts'
		LIMIT 1
	`, escapedEventId)

	log.Printf("LocationPolygon query: %s", selectQuery)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
	if err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}
	defer rows.Close()

	var polygon *string
	if rows.Next() {
		if err := rows.Scan(&polygon); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
	}

	if err := rows.Err(); err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	if polygon == nil {
		return nil, middleware.NewNotFoundError("No polygon found for the given eventId", "")
	}

	// Parse the polygon string as JSON
	var polygonJSON interface{}
	if err := json.Unmarshal([]byte(*polygon), &polygonJSON); err != nil {
		return nil, middleware.NewInternalServerError("Failed to parse polygon JSON", err.Error())
	}

	return map[string]interface{}{
		"data": polygonJSON,
	}, nil
}
