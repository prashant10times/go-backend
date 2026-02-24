package category

import (
	"context"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
	"time"
)

type CategoryService struct {
	clickhouseService *services.ClickHouseService
}

func NewCategoryService(clickhouseService *services.ClickHouseService) *CategoryService {
	return &CategoryService{clickhouseService: clickhouseService}
}

type Category struct {
	Name         string `json:"name"`
	CategoryUUID string `json:"category_uuid"`
	Slug         string `json:"slug"`
	IsGroup      uint8  `json:"is_group"`
}

func (s *CategoryService) GetCategory(query models.SearchCategoryDto) (interface{}, error) {
	ctx := context.Background()

	// Build WHERE conditions with priority: id > id_10x > slugs > name
	whereConditions := []string{}

	// Priority 1: IDs
	if len(query.ParsedID) > 0 {
		escapedIDs := make([]string, len(query.ParsedID))
		for i, id := range query.ParsedID {
			escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("category_uuid IN (%s)", strings.Join(escapedIDs, ",")))
	} else if len(query.ParsedID10x) > 0 {
		// Priority 2: id_10x
		id10xStrs := make([]string, len(query.ParsedID10x))
		for i, id := range query.ParsedID10x {
			id10xStrs[i] = fmt.Sprintf("%d", id)
		}
		whereConditions = append(whereConditions, fmt.Sprintf("category IN (%s)", strings.Join(id10xStrs, ",")))
	} else {
		// Priority 3: slugs
		if len(query.ParsedSlugs) > 0 {
			escapedSlugs := make([]string, len(query.ParsedSlugs))
			for i, slug := range query.ParsedSlugs {
				escapedSlugs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(slug, "'", "''"))
			}
			whereConditions = append(whereConditions, fmt.Sprintf("slug IN (%s)", strings.Join(escapedSlugs, ",")))
		} else if query.Name != "" {
			// Priority 4: name
			nameConditions := []string{}
			names := strings.Split(query.Name, "|:")
			for _, name := range names {
				name = strings.TrimSpace(name)
				if name != "" {
					escapedName := strings.ReplaceAll(name, "'", "''")
					nameConditions = append(nameConditions, fmt.Sprintf("lower(name) LIKE lower('%%%s%%')", escapedName))
				}
			}
			if len(nameConditions) > 0 {
				whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(nameConditions, " OR ")))
			}
		}
	}

	// is_group
	if query.ParsedIsGroup == nil || *query.ParsedIsGroup {
		whereConditions = append(whereConditions, "is_group = 1")
	} else {
		whereConditions = append(whereConditions, "is_group = 0")
	}

	// is_designation
	if query.ParsedIsDesignation != nil && *query.ParsedIsDesignation {
		whereConditions = append(whereConditions, "is_designation = 1")
	}

	whereClause := strings.Join(whereConditions, " AND ")
	if whereClause == "" {
		whereClause = "1=1"
	}

	selectQuery := fmt.Sprintf(`
		SELECT category, category_uuid, created, is_group, name, short_name, slug, published
		FROM testing_db.event_category_ch
		WHERE %s
		GROUP BY category, category_uuid, created, is_group, name, short_name, slug, published
		ORDER BY name ASC
	`, whereClause)

	log.Printf("category query: %s", selectQuery)

	selectQuery += fmt.Sprintf(" LIMIT %d", query.ParsedTake)
	if query.ParsedSkip > 0 {
		selectQuery += fmt.Sprintf(" OFFSET %d", query.ParsedSkip)
	}

	rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
	if err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}
	defer rows.Close()

	columns := rows.Columns()
	var categories []map[string]interface{}

	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		for i, col := range columns {
			switch col {
			case "category", "event", "version":
				scanArgs[i] = new(uint32)
			case "is_group":
				scanArgs[i] = new(uint8)
			case "published":
				scanArgs[i] = new(int8)
			case "created", "last_updated_at":
				scanArgs[i] = new(time.Time)
			default:
				scanArgs[i] = new(string)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}

		category := make(map[string]interface{})
		for i, col := range columns {
			switch col {
			case "category", "event", "version":
				if val, ok := scanArgs[i].(*uint32); ok && val != nil {
					category[col] = *val
				} else {
					category[col] = uint32(0)
				}
			case "is_group":
				if val, ok := scanArgs[i].(*uint8); ok && val != nil {
					category[col] = *val
				} else {
					category[col] = uint8(0)
				}
			case "published":
				if val, ok := scanArgs[i].(*int8); ok && val != nil {
					category[col] = *val
				} else {
					category[col] = int8(0)
				}
			case "created", "last_updated_at":
				if val, ok := scanArgs[i].(*time.Time); ok && val != nil {
					category[col] = *val
				} else {
					category[col] = nil
				}
			default:
				if val, ok := scanArgs[i].(*string); ok && val != nil {
					category[col] = *val
				} else {
					category[col] = ""
				}
			}
		}
		categories = append(categories, category)
	}

	if err := rows.Err(); err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	if len(categories) == 0 {
		return nil, middleware.NewNotFoundError("No record found", "")
	}

	return categories, nil
}
