package designation

import (
	"context"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
)

type DesignationService struct {
	clickhouseService *services.ClickHouseService
}

func NewDesignationService(clickhouseService *services.ClickHouseService) *DesignationService {
	return &DesignationService{clickhouseService: clickhouseService}
}

type Designation struct {
	Name            string `json:"name"`
	DesignationUUID string `json:"designation_uuid"`
	Department      string `json:"department,omitempty"`
	Role            string `json:"role,omitempty"`
}

func (s *DesignationService) GetDesignation(query models.SearchDesignationDto) (interface{}, error) {
	if (query.ParsedIsDepartment != nil && *query.ParsedIsDepartment) ||
		(query.ParsedIsRole != nil && *query.ParsedIsRole) ||
		(query.ParsedIsDesignation != nil && *query.ParsedIsDesignation) {
		return s.getDesignationsFromDesignationTable(query)
	}

	return s.getDesignationsFromDesignationTableFallback(query)
}

func (s *DesignationService) getDesignationsFromDesignationTableFallback(query models.SearchDesignationDto) (interface{}, error) {
	ctx := context.Background()

	whereConditions := []string{}

	if len(query.ParsedID) > 0 {
		escapedIDs := make([]string, len(query.ParsedID))
		for i, id := range query.ParsedID {
			escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
		}
		whereConditions = append(whereConditions, fmt.Sprintf("designation_uuid IN (%s)", strings.Join(escapedIDs, ",")))
	} else if query.Name != "" {
		nameConditions := []string{}
		names := strings.Split(query.Name, ",")
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name != "" {
				escapedName := strings.ReplaceAll(name, "'", "''")
				// Case-insensitive LIKE in ClickHouse using lower()
				nameConditions = append(nameConditions, fmt.Sprintf("lower(display_name) LIKE lower('%%%s%%')", escapedName))
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

	selectQuery := fmt.Sprintf(`
		SELECT
			display_name,
			designation_uuid,
			department,
			role
		FROM testing_db.event_designation_ch
		WHERE display_name != '' AND %s
		GROUP BY display_name, designation_uuid, department, role
		ORDER BY display_name ASC
	`, whereClause)

	selectQuery += fmt.Sprintf(" LIMIT %d", query.ParsedTake)
	if query.ParsedSkip > 0 {
		selectQuery += fmt.Sprintf(" OFFSET %d", query.ParsedSkip)
	}

	log.Printf("Designation query (fallback): %s", selectQuery)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
	if err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}
	defer rows.Close()

	var designations []Designation
	for rows.Next() {
		var designation Designation
		if err := rows.Scan(&designation.Name, &designation.DesignationUUID, &designation.Department, &designation.Role); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		designations = append(designations, designation)
	}

	if err := rows.Err(); err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	if len(designations) == 0 {
		return nil, middleware.NewNotFoundError("No record found", "")
	}

	return designations, nil
}

func (s *DesignationService) getDesignationsFromDesignationTable(query models.SearchDesignationDto) (interface{}, error) {
	ctx := context.Background()

	if len(query.ParsedIDs) > 0 {
		escapedIDs := make([]string, len(query.ParsedIDs))
		for i, id := range query.ParsedIDs {
			escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
		}

		selectQuery := fmt.Sprintf(`
			SELECT
				display_name,
				designation_uuid,
				department,
				role
			FROM testing_db.event_designation_ch
			WHERE display_name != '' AND designation_uuid IN (%s)
			GROUP BY display_name, designation_uuid, department, role
			ORDER BY display_name ASC
		`, strings.Join(escapedIDs, ","))

		log.Printf("Designation query (by IDs): %s", selectQuery)

		rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
		if err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		defer rows.Close()

		var designations []Designation
		for rows.Next() {
			var designation Designation
			if err := rows.Scan(&designation.Name, &designation.DesignationUUID, &designation.Department, &designation.Role); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			designations = append(designations, designation)
		}

		if err := rows.Err(); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}

		if len(designations) == 0 {
			return nil, middleware.NewNotFoundError("No record found", "")
		}

		return designations, nil
	}

	name := query.Name
	if name == "" {
		name = ""
	}
	nameLower := strings.ToLower(name)
	escapedName := strings.ReplaceAll(nameLower, "'", "''")

	designationQuery := fmt.Sprintf(`
		SELECT
			display_name,
			designation_uuid as id
		FROM testing_db.event_designation_ch
		WHERE display_name != '' AND (
			lower(display_name) LIKE '%s%%' 
			OR lower(display_name) LIKE '%% %s%%'
		)
		%s
		GROUP BY display_name, designation_uuid
		ORDER BY position(lower(display_name), '%s'), length(display_name), display_name ASC
		LIMIT %d OFFSET %d
	`, escapedName, escapedName,
		func() string {
			if len(name) < 1 {
				return "AND length(display_name) > 5"
			}
			return ""
		}(),
		escapedName, query.ParsedTake, query.ParsedSkip)

	log.Printf("Designation query (designation search): %s", designationQuery)

	departmentQuery := fmt.Sprintf(`
		SELECT
			designation_uuid as id,
			department as name
		FROM testing_db.event_designation_ch
		WHERE department != '' AND lower(department) LIKE '%%%s%%'
		AND department != ''
		GROUP BY designation_uuid, department
		ORDER BY length(department) ASC, department ASC
		LIMIT %d OFFSET %d
	`, escapedName, query.ParsedTake, query.ParsedSkip)

	log.Printf("Designation query (department search): %s", departmentQuery)

	roleQuery := fmt.Sprintf(`
		SELECT
			designation_uuid as id,
			role as name
		FROM testing_db.event_designation_ch
		WHERE role != '' AND lower(role) LIKE '%%%s%%'
		AND role != ''
		AND length(role) > 1
		GROUP BY designation_uuid, role
		ORDER BY length(role) ASC, role ASC
		LIMIT %d OFFSET %d
	`, escapedName, query.ParsedTake, query.ParsedSkip)

	log.Printf("Designation query (role search): %s", roleQuery)

	type queryResult struct {
		designations []map[string]string
		departments  []map[string]string
		roles        []map[string]string
		err          error
	}

	resultChan := make(chan queryResult, 1)

	go func() {
		var result queryResult

		desRows, err := s.clickhouseService.ExecuteQuery(ctx, designationQuery)
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}
		defer desRows.Close()

		for desRows.Next() {
			var id, name string
			if err := desRows.Scan(&name, &id); err != nil {
				result.err = err
				resultChan <- result
				return
			}
			result.designations = append(result.designations, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}

		deptRows, err := s.clickhouseService.ExecuteQuery(ctx, departmentQuery)
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}
		defer deptRows.Close()

		for deptRows.Next() {
			var id, name string
			if err := deptRows.Scan(&id, &name); err != nil {
				result.err = err
				resultChan <- result
				return
			}
			result.departments = append(result.departments, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}

		roleRows, err := s.clickhouseService.ExecuteQuery(ctx, roleQuery)
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}
		defer roleRows.Close()

		for roleRows.Next() {
			var id, name string
			if err := roleRows.Scan(&id, &name); err != nil {
				result.err = err
				resultChan <- result
				return
			}
			result.roles = append(result.roles, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}

		resultChan <- result
	}()

	result := <-resultChan
	if result.err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", result.err.Error())
	}

	response := map[string]interface{}{
		"designation": result.designations,
		"department":  result.departments,
		"role":        result.roles,
	}

	return response, nil
}
