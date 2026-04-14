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
	if query.ParsedIsDepartment == nil && query.ParsedIsRole == nil && query.ParsedIsDesignation == nil {
		defaultTrue := true
		query.ParsedIsDesignation = &defaultTrue
	}

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

	isDesignationRequested := query.ParsedIsDesignation != nil && *query.ParsedIsDesignation
	isDepartmentRequested := query.ParsedIsDepartment != nil && *query.ParsedIsDepartment
	isRoleRequested := query.ParsedIsRole != nil && *query.ParsedIsRole

	name := query.Name
	if name == "" {
		name = ""
	}
	nameLower := strings.ToLower(name)
	escapedName := strings.ReplaceAll(nameLower, "'", "''")

	response := map[string]interface{}{
	}

	if len(query.ParsedIDs) > 0 {
		if !isDesignationRequested {
			return response, nil
		}

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

		response["designation"] = designations
		return response, nil
	}

	// Run only the requested queries.
	if isDesignationRequested {
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

		desRows, err := s.clickhouseService.ExecuteQuery(ctx, designationQuery)
		if err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		defer desRows.Close()

		var designations []map[string]string
		for desRows.Next() {
			var id, name string
			if err := desRows.Scan(&name, &id); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			designations = append(designations, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}
		if err := desRows.Err(); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		response["designation"] = designations
	}

	if isDepartmentRequested {
		departmentQuery := fmt.Sprintf(`
			SELECT
				any(designation_uuid) as id,
				department as name
			FROM testing_db.event_designation_ch
			WHERE department != '' AND lower(department) LIKE '%%%s%%'
			GROUP BY department
			ORDER BY length(department) ASC, department ASC
			LIMIT %d OFFSET %d
		`, escapedName, query.ParsedTake, query.ParsedSkip)

		log.Printf("Designation query (department search): %s", departmentQuery)

		deptRows, err := s.clickhouseService.ExecuteQuery(ctx, departmentQuery)
		if err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		defer deptRows.Close()

		var departments []map[string]string
		for deptRows.Next() {
			var id, name string
			if err := deptRows.Scan(&id, &name); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			departments = append(departments, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}
		if err := deptRows.Err(); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		response["department"] = departments
	}

	if isRoleRequested {
		roleQuery := fmt.Sprintf(`
			SELECT
				any(designation_uuid) as id,
				role as name
			FROM testing_db.event_designation_ch
			WHERE role != '' AND lower(role) LIKE '%%%s%%'
			AND length(role) > 1
			GROUP BY role
			ORDER BY length(role) ASC, role ASC
			LIMIT %d OFFSET %d
		`, escapedName, query.ParsedTake, query.ParsedSkip)

		log.Printf("Designation query (role search): %s", roleQuery)

		roleRows, err := s.clickhouseService.ExecuteQuery(ctx, roleQuery)
		if err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		defer roleRows.Close()

		var roles []map[string]string
		for roleRows.Next() {
			var id, name string
			if err := roleRows.Scan(&id, &name); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			roles = append(roles, map[string]string{
				"id":   id,
				"name": strings.TrimSpace(name),
			})
		}
		if err := roleRows.Err(); err != nil {
			return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
		}
		response["role"] = roles
	}

	// Match previous behavior: if nothing is found for requested queries, return not found.
	if len(response) == 0 ||
		(isDesignationRequested && response["designation"] != nil && len(response["designation"].([]map[string]string)) == 0) ||
		(isDepartmentRequested && response["department"] != nil && len(response["department"].([]map[string]string)) == 0) ||
		(isRoleRequested && response["role"] != nil && len(response["role"].([]map[string]string)) == 0) {
		// If all requested slices ended up empty, treat as not found.
		allEmpty := true
		if isDesignationRequested {
			if v, ok := response["designation"].([]map[string]string); ok && len(v) > 0 {
				allEmpty = false
			}
		}
		if isDepartmentRequested {
			if v, ok := response["department"].([]map[string]string); ok && len(v) > 0 {
				allEmpty = false
			}
		}
		if isRoleRequested {
			if v, ok := response["role"].([]map[string]string); ok && len(v) > 0 {
				allEmpty = false
			}
		}
		if allEmpty {
			return nil, middleware.NewNotFoundError("No record found", "")
		}
	}

	return response, nil
}
