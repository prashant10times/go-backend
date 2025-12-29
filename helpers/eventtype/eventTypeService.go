package eventtype

import (
	"context"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
)

type EventTypeService struct {
	clickhouseService *services.ClickHouseService
}

func NewEventTypeService(clickhouseService *services.ClickHouseService) *EventTypeService {
	return &EventTypeService{clickhouseService: clickhouseService}
}

type EventType struct {
	ID       string   `json:"id"`
	ID10x    uint32   `json:"id_10x"`
	Priority int8     `json:"priority"`
	Name     string   `json:"name"`
	Slug     string   `json:"slug,omitempty"`
	Groups   []string `json:"groups,omitempty"`
}

func (s *EventTypeService) GetAll(query models.SearchEventTypeDto) (interface{}, error) {
	ctx := context.Background()

	whereConditions := []string{}
	hasSlugs := len(query.ParsedSlugs) > 0
	hasIDs := len(query.ParsedIDs) > 0
	hasEventTypeGroup := query.ParsedEventTypeGroup != nil
	hasQuery := query.Query != ""

	if hasSlugs {
		escapedSlugs := make([]string, len(query.ParsedSlugs))
		for i, slug := range query.ParsedSlugs {
			escapedSlugs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(slug, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("et.slug IN (%s)", strings.Join(escapedSlugs, ",")))
	} else if hasIDs {
		escapedIDs := make([]string, len(query.ParsedIDs))
		for i, id := range query.ParsedIDs {
			escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("et.eventtype_uuid IN (%s)", strings.Join(escapedIDs, ",")))
	} else if hasEventTypeGroup {
		group := *query.ParsedEventTypeGroup
		escapedGroup := strings.ReplaceAll(group, "'", "''")
		whereConditions = append(whereConditions, fmt.Sprintf("has(et.groups, '%s')", escapedGroup))
	} else if hasQuery {
		queryParts := strings.Split(query.Query, ",")
		nameConditions := []string{}
		for _, part := range queryParts {
			part = strings.TrimSpace(part)
			if part != "" {
				partLower := strings.ToLower(part)
				escapedPart := strings.ReplaceAll(partLower, "'", "''")
				nameConditions = append(nameConditions, fmt.Sprintf("lower(et.name) LIKE lower('%%%s%%')", escapedPart))
			}
		}
		if len(nameConditions) > 0 {
			whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(nameConditions, " OR ")))
		}
	}

	whereClause := strings.Join(whereConditions, " AND ")
	if whereClause == "" {
		whereClause = "1=1"
	}

	if query.ParsedGroupBy != nil && *query.ParsedGroupBy == "typeGroup" {
		baseWhereClause := strings.Join(whereConditions, " AND ")
		if baseWhereClause == "" {
			baseWhereClause = "1=1"
		}

		baseWhereConditions := []string{}
		for _, condition := range whereConditions {
			if !strings.Contains(condition, "has(et.groups") {
				baseWhereConditions = append(baseWhereConditions, condition)
			}
		}
		if len(baseWhereConditions) == 0 {
			baseWhereConditions = append(baseWhereConditions, "1=1")
		}
		baseWhereClause = strings.Join(baseWhereConditions, " AND ")

		groupByQuery := fmt.Sprintf(`
			SELECT 
				group_name,
				count(*) as count
			FROM testing_db.event_type_ch AS et
			ARRAY JOIN et.groups AS group_name
			WHERE %s
				AND group_name IN ('business', 'social', 'unattended')
			GROUP BY group_name
			ORDER BY group_name ASC
		`, baseWhereClause)

		log.Printf("Event type query (groupBy): %s", groupByQuery)

		rows, err := s.clickhouseService.ExecuteQuery(ctx, groupByQuery)
		if err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		defer rows.Close()

		result := make(map[string]interface{})
		for rows.Next() {
			var groupName string
			var count uint64
			if err := rows.Scan(&groupName, &count); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			result[groupName] = count
		}

		if err := rows.Err(); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}

		return result, nil
	}

	selectClause := "DISTINCT et.eventtype_uuid, et.eventtype_id, et.priority, et.name, et.slug, toString(et.groups) as groups"
	selectQuery := fmt.Sprintf(`
		SELECT %s
		FROM testing_db.event_type_ch AS et
		WHERE %s
		ORDER BY et.name ASC
	`, selectClause, whereClause)

	selectQuery += fmt.Sprintf(" LIMIT %d", query.ParsedTake)
	if query.ParsedSkip > 0 {
		selectQuery += fmt.Sprintf(" OFFSET %d", query.ParsedSkip)
	}

	log.Printf("Event type query: %s", selectQuery)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
	if err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}
	defer rows.Close()

	var eventTypes []EventType
	for rows.Next() {
		var eventType EventType
		var groupsStr string
		if err := rows.Scan(&eventType.ID, &eventType.ID10x, &eventType.Priority, &eventType.Name, &eventType.Slug, &groupsStr); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}

		if groupsStr != "" {
			groupsStr = strings.Trim(groupsStr, "[]")
			if groupsStr != "" {
				groups := strings.Split(groupsStr, ",")
				eventType.Groups = make([]string, 0, len(groups))
				for _, group := range groups {
					group = strings.TrimSpace(group)
					group = strings.Trim(group, `"'`)
					if group != "" {
						eventType.Groups = append(eventType.Groups, group)
					}
				}
			}
		}

		eventTypes = append(eventTypes, eventType)
	}

	if err := rows.Err(); err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	if len(eventTypes) == 0 {
		return nil, middleware.NewNotFoundError("No record found", "")
	}

	if hasSlugs {
		return eventTypes, nil
	}

	if hasEventTypeGroup {
		group := *query.ParsedEventTypeGroup
		filteredEventTypes := []EventType{}
		for _, eventType := range eventTypes {
			hasGroup := false
			for _, g := range eventType.Groups {
				if strings.ToLower(g) == group {
					hasGroup = true
					break
				}
			}
			if hasGroup {
				filteredEventTypes = append(filteredEventTypes, eventType)
			}
		}
		return filteredEventTypes, nil
	}

	return eventTypes, nil
}
