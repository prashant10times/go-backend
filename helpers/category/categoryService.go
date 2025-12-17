package category

import (
	"context"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
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

	// query
	selectQuery := fmt.Sprintf(`
		SELECT
			name,
			category_uuid,
			slug,
			is_group
		FROM testing_db.event_category_ch
		WHERE %s
		GROUP BY name, category_uuid, slug, is_group
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

	var categories []Category
	for rows.Next() {
		var category Category
		if err := rows.Scan(&category.Name, &category.CategoryUUID, &category.Slug, &category.IsGroup); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
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
