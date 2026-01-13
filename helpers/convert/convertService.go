package convert

import (
	"context"
	"fmt"
	"log"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
)

type ConvertService struct {
	clickhouseService *services.ClickHouseService
}

func NewConvertService(clickhouseService *services.ClickHouseService) *ConvertService {
	return &ConvertService{clickhouseService: clickhouseService}
}

func (s *ConvertService) ConvertIds(query models.ConvertSchemaDto) (map[string]interface{}, error) {
	ctx := context.Background()

	type conversionResult struct {
		data map[string]interface{}
		err  error
	}

	eventChan := make(chan conversionResult, 1)
	countryChan := make(chan conversionResult, 1)
	cityChan := make(chan conversionResult, 1)
	categoryChan := make(chan conversionResult, 1)
	locationChan := make(chan conversionResult, 1)
	designationChan := make(chan conversionResult, 1)
	roleChan := make(chan conversionResult, 1)
	departmentChan := make(chan conversionResult, 1)

	go func() {
		log.Printf("Getting EVENT ID's Data")
		data, err := s.convertEventIds(ctx, query.ParsedEventIds, query.ParsedEventUUIDs)
		eventChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting COUNTRY ID's Data")
		data, err := s.convertCountryIds(ctx, query.ParsedCountryIds, []string{})
		countryChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting CITY ID's Data")
		data, err := s.convertCityIds(ctx, query.ParsedCityIds, []string{})
		cityChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting CATEGORY ID's Data")
		data, err := s.convertCategoryIds(ctx, query.ParsedCategoryIds, query.ParsedCategoryUUIDs)
		categoryChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting LOCATION ID's Data")
		data, err := s.convertLocationIds(ctx, query.ParsedLocationIDs)
		locationChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting DESIGNATION ID's Data")
		data, err := s.convertDesignationIds(ctx, query.ParsedDesignationIds)
		designationChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting ROLE ID's Data")
		data, err := s.convertRoleIds(ctx, query.ParsedRoleIds)
		roleChan <- conversionResult{data: data, err: err}
	}()

	go func() {
		log.Printf("Getting DEPARTMENT ID's Data")
		data, err := s.convertDepartmentIds(ctx, query.ParsedDepartmentIds)
		departmentChan <- conversionResult{data: data, err: err}
	}()

	eventRes := <-eventChan
	if eventRes.err != nil {
		return nil, eventRes.err
	}

	countryRes := <-countryChan
	if countryRes.err != nil {
		return nil, countryRes.err
	}

	cityRes := <-cityChan
	if cityRes.err != nil {
		return nil, cityRes.err
	}

	categoryRes := <-categoryChan
	if categoryRes.err != nil {
		return nil, categoryRes.err
	}

	locationRes := <-locationChan
	if locationRes.err != nil {
		return nil, locationRes.err
	}

	designationRes := <-designationChan
	if designationRes.err != nil {
		return nil, designationRes.err
	}

	roleRes := <-roleChan
	if roleRes.err != nil {
		return nil, roleRes.err
	}

	departmentRes := <-departmentChan
	if departmentRes.err != nil {
		return nil, departmentRes.err
	}

	response := map[string]interface{}{
		"cityIds":     cityRes.data,
		"countryIds":  countryRes.data,
		"eventIds":    eventRes.data,
		"categoryIds": categoryRes.data,
		"locationIds": locationRes.data,
		"designations": map[string]interface{}{
			"designation": designationRes.data,
			"role":        roleRes.data,
			"department":  departmentRes.data,
		},
	}

	return response, nil
}

func (s *ConvertService) convertEventIds(ctx context.Context, ids []string, uuids []string) (map[string]interface{}, error) {
	if len(uuids) > 0 {
		quotedUUIDs := make([]string, 0, len(uuids))
		for _, uuid := range uuids {
			uuid = strings.TrimSpace(uuid)
			if uuid != "" {
				quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
			}
		}

		if len(quotedUUIDs) > 0 {
			query := fmt.Sprintf(`
				SELECT event_uuid, event_id as id
				FROM testing_db.allevent_ch
				WHERE edition_type = 'current_edition' AND event_uuid IN (%s)
			`, strings.Join(quotedUUIDs, ","))

			log.Printf("convertEventIds uuids query: %s", query)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			result := make(map[string]interface{})
			for rows.Next() {
				var eventUUID string
				var sourceID uint32

				if err := rows.Scan(&eventUUID, &sourceID); err != nil {
					return nil, err
				}

				result[eventUUID] = fmt.Sprintf("%d", sourceID)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return result, nil
		}
	}

	if len(ids) == 0 {
		return map[string]interface{}{}, nil
	}

	idList := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			idList = append(idList, id)
		}
	}

	if len(idList) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT event_uuid, event_id as id
		FROM testing_db.allevent_ch
		WHERE edition_type = 'current_edition' AND event_id IN (%s)
	`, strings.Join(idList, ","))

	log.Printf("convertEventIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var eventUUID string
		var sourceID uint32

		if err := rows.Scan(&eventUUID, &sourceID); err != nil {
			return nil, err
		}

		result[fmt.Sprintf("%d", sourceID)] = eventUUID
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertCategoryIds(ctx context.Context, ids []string, uuids []string) (map[string]interface{}, error) {
	if len(uuids) > 0 {
		quotedUUIDs := make([]string, 0, len(uuids))
		for _, uuid := range uuids {
			uuid = strings.TrimSpace(uuid)
			if uuid != "" {
				quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
			}
		}

		if len(quotedUUIDs) > 0 {
			query := fmt.Sprintf(`
				SELECT category_uuid, category, name, is_group, slug
				FROM testing_db.event_category_ch
				WHERE category_uuid IN (%s)
				GROUP BY category_uuid, category, name, is_group, slug
			`, strings.Join(quotedUUIDs, ","))

			log.Printf("convertCategoryIds uuids query: %s", query)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			result := make(map[string]interface{})
			for rows.Next() {
				var categoryUUID, name, slug string
				var isGroup uint8
				var categoryID *uint32

				if err := rows.Scan(&categoryUUID, &categoryID, &name, &isGroup, &slug); err != nil {
					return nil, err
				}

				if categoryID != nil {
					result[categoryUUID] = map[string]interface{}{
						"id":      fmt.Sprintf("%d", *categoryID),
						"name":    name,
						"isGroup": isGroup == 1,
						"slug":    slug,
					}
				}
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return result, nil
		}
	}

	if len(ids) == 0 {
		return map[string]interface{}{}, nil
	}

	idList := make([]string, 0, len(ids))
	inputIDMap := make(map[string]bool)
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			idList = append(idList, id)
			inputIDMap[id] = true
		}
	}

	if len(idList) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT category_uuid, category, name, is_group, slug
		FROM testing_db.event_category_ch
		WHERE category IN (%s)
		GROUP BY category_uuid, category, name, is_group, slug
	`, strings.Join(idList, ","))

	log.Printf("convertCategoryIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	categoryDataMap := make(map[string]map[string]interface{})
	for rows.Next() {
		var categoryUUID, name, slug string
		var isGroup uint8
		var categoryID *uint32

		if err := rows.Scan(&categoryUUID, &categoryID, &name, &isGroup, &slug); err != nil {
			return nil, err
		}

		if categoryID != nil {
			categoryDataMap[fmt.Sprintf("%d", *categoryID)] = map[string]interface{}{
				"id":      categoryUUID,
				"name":    name,
				"isGroup": isGroup == 1,
				"slug":    slug,
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for inputID := range inputIDMap {
		if data, exists := categoryDataMap[inputID]; exists {
			result[inputID] = data
		}
	}

	return result, nil
}

func (s *ConvertService) convertCountryIds(ctx context.Context, ids []string, uuids []string) (map[string]interface{}, error) {
	if len(uuids) > 0 {
		quotedUUIDs := make([]string, 0, len(uuids))
		for _, uuid := range uuids {
			uuid = strings.TrimSpace(uuid)
			if uuid != "" {
				quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
			}
		}

		if len(quotedUUIDs) > 0 {
			query := fmt.Sprintf(`
				SELECT id_uuid, name, location_type, replace(id_10x, 'country-', '') as iso
				FROM testing_db.location_ch
				WHERE location_type = 'COUNTRY'
				AND id_uuid IN (%s)
			`, strings.Join(quotedUUIDs, ","))

			log.Printf("convertCountryIds uuids query: %s", query)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			result := make(map[string]interface{})
			for rows.Next() {
				var locationUUID, name, locationType, iso string

				if err := rows.Scan(&locationUUID, &name, &locationType, &iso); err != nil {
					return nil, err
				}

				result[locationUUID] = iso
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return result, nil
		}
	}

	if len(ids) == 0 {
		return map[string]interface{}{}, nil
	}

	prefixedIds := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			prefixed := id
			if !strings.HasPrefix(id, "country-") {
				prefixed = "country-" + strings.ToUpper(id)
			}
			prefixedIds = append(prefixedIds, fmt.Sprintf("'%s'", strings.ReplaceAll(prefixed, "'", "''")))
		}
	}

	if len(prefixedIds) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT id_uuid, name, location_type, replace(id_10x, 'country-', '') as iso
		FROM testing_db.location_ch
		WHERE location_type IN ('CITY', 'COUNTRY')
		AND (
			(
				location_type = 'COUNTRY'
				AND id_10x IN (%s)
			)
		)
	`, strings.Join(prefixedIds, ","))

	log.Printf("convertCountryIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var locationUUID, name, locationType, iso string

		if err := rows.Scan(&locationUUID, &name, &locationType, &iso); err != nil {
			return nil, err
		}

		result[iso] = locationUUID
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertCityIds(ctx context.Context, ids []string, uuids []string) (map[string]interface{}, error) {
	if len(uuids) > 0 {
		quotedUUIDs := make([]string, 0, len(uuids))
		for _, uuid := range uuids {
			uuid = strings.TrimSpace(uuid)
			if uuid != "" {
				quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
			}
		}

		if len(quotedUUIDs) > 0 {
			query := fmt.Sprintf(`
				SELECT id_uuid, name, location_type, toInt32OrNull(replace(id_10x, 'city-', '')) as idten
				FROM testing_db.location_ch
				WHERE location_type = 'CITY'
				AND id_uuid IN (%s)
			`, strings.Join(quotedUUIDs, ","))

			log.Printf("convertCityIds uuids query: %s", query)
			rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			result := make(map[string]interface{})
			for rows.Next() {
				var locationUUID, name, locationType string
				var idten *int32

				if err := rows.Scan(&locationUUID, &name, &locationType, &idten); err != nil {
					return nil, err
				}

				if idten != nil {
					result[locationUUID] = fmt.Sprintf("%d", *idten)
				}
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return result, nil
		}
	}

	if len(ids) == 0 {
		return map[string]interface{}{}, nil
	}

	prefixedIds := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			prefixed := id
			if !strings.HasPrefix(id, "city-") {
				prefixed = "city-" + id
			}
			prefixedIds = append(prefixedIds, fmt.Sprintf("'%s'", strings.ReplaceAll(prefixed, "'", "''")))
		}
	}

	if len(prefixedIds) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT id_uuid, name, location_type, toInt32OrNull(replace(id_10x, 'city-', '')) as idten
		FROM testing_db.location_ch
		WHERE location_type IN ('CITY', 'COUNTRY')
		AND (
			(
				location_type = 'CITY'
				AND id_10x IN (%s)
			)
		)
	`, strings.Join(prefixedIds, ","))

	log.Printf("convertCityIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var locationUUID, name, locationType string
		var idten *int32

		if err := rows.Scan(&locationUUID, &name, &locationType, &idten); err != nil {
			return nil, err
		}

		if idten != nil {
			result[fmt.Sprintf("%d", *idten)] = locationUUID
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertLocationIds(ctx context.Context, uuids []string) (map[string]interface{}, error) {
	if len(uuids) == 0 {
		return map[string]interface{}{
			"country": map[string]interface{}{},
			"state":   map[string]interface{}{},
			"city":    map[string]interface{}{},
			"venue":   map[string]interface{}{},
		}, nil
	}

	quotedUUIDs := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		uuid = strings.TrimSpace(uuid)
		if uuid != "" {
			quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
		}
	}

	if len(quotedUUIDs) == 0 {
		return map[string]interface{}{
			"country": map[string]interface{}{},
			"state":   map[string]interface{}{},
			"city":    map[string]interface{}{},
			"venue":   map[string]interface{}{},
		}, nil
	}

	query := fmt.Sprintf(`
		SELECT 
			id_uuid,
			location_type,
			id,
			name,
			slug,
			id_10x,
			iso,
			toInt32OrNull(replace(id_10x, 'city-', '')) as city_id
		FROM testing_db.location_ch
		WHERE location_type IN ('COUNTRY', 'CITY', 'STATE', 'VENUE')
		AND id_uuid IN (%s)
	`, strings.Join(quotedUUIDs, ","))

	log.Printf("convertLocationIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]interface{}{
		"country": make(map[string]interface{}),
		"state":   make(map[string]interface{}),
		"city":    make(map[string]interface{}),
		"venue":   make(map[string]interface{}),
	}

	countryMap := result["country"].(map[string]interface{})
	stateMap := result["state"].(map[string]interface{})
	cityMap := result["city"].(map[string]interface{})
	venueMap := result["venue"].(map[string]interface{})

	for rows.Next() {
		var locationUUID, locationType, id10x, iso string
		var name, slug *string
		var id *uint32
		var cityID *int32

		if err := rows.Scan(&locationUUID, &locationType, &id, &name, &slug, &id10x, &iso, &cityID); err != nil {
			return nil, err
		}

		locationData := map[string]interface{}{
			"name":   "",
			"slug":   "",
			"id_10x": id10x,
			"iso":    iso,
		}
		if name != nil {
			locationData["name"] = *name
		}
		if slug != nil {
			locationData["slug"] = *slug
		}

		switch locationType {
		case "COUNTRY":
			if iso != "" {
				locationData["id"] = strings.ToUpper(iso)
				countryMap[locationUUID] = locationData
			}
		case "STATE":
			if id != nil {
				locationData["id"] = fmt.Sprintf("%d", *id)
				stateMap[locationUUID] = locationData
			}
		case "CITY":
			if cityID != nil {
				locationData["id"] = fmt.Sprintf("%d", *cityID)
				cityMap[locationUUID] = locationData
			}
		case "VENUE":
			if id != nil {
				locationData["id"] = fmt.Sprintf("%d", *id)
				venueMap[locationUUID] = locationData
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertDesignationIds(ctx context.Context, uuids []string) (map[string]interface{}, error) {
	log.Printf("convertDesignationIds called with %d UUIDs: %v", len(uuids), uuids)
	if len(uuids) == 0 {
		return map[string]interface{}{}, nil
	}

	quotedUUIDs := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		uuid = strings.TrimSpace(uuid)
		if uuid != "" {
			quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
		}
	}

	if len(quotedUUIDs) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT 
			designation_uuid,
			designation_id as id,
			display_name as name,
			role,
			department
		FROM testing_db.event_designation_ch
		WHERE designation_uuid IN (%s)
		GROUP BY designation_uuid, designation_id, display_name, role, department
	`, strings.Join(quotedUUIDs, ","))

	log.Printf("convertDesignationIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var designationUUID, name, role, department string
		var designationID *uint32

		if err := rows.Scan(&designationUUID, &designationID, &name, &role, &department); err != nil {
			return nil, err
		}

		designationData := map[string]interface{}{
			"name":       name,
			"role":       role,
			"department": department,
		}

		if designationID != nil {
			designationData["id"] = *designationID
		}

		if existing, exists := result[designationUUID]; exists {
			if arr, ok := existing.([]interface{}); ok {
				result[designationUUID] = append(arr, designationData)
			} else {
				result[designationUUID] = []interface{}{existing, designationData}
			}
		} else {
			result[designationUUID] = designationData
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertRoleIds(ctx context.Context, uuids []string) (map[string]interface{}, error) {
	log.Printf("convertRoleIds called with %d UUIDs: %v", len(uuids), uuids)
	if len(uuids) == 0 {
		return map[string]interface{}{}, nil
	}

	quotedUUIDs := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		uuid = strings.TrimSpace(uuid)
		if uuid != "" {
			quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
		}
	}

	if len(quotedUUIDs) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT 
			d1.designation_uuid,
			d1.designation_id as id,
			d1.display_name as name,
			d1.role,
			d1.department
		FROM testing_db.event_designation_ch AS d1
		INNER JOIN testing_db.event_designation_ch AS d2 
			ON d1.role = d2.role
		WHERE d2.designation_uuid IN (%s)
		GROUP BY d1.designation_uuid, d1.designation_id, d1.display_name, d1.role, d1.department
	`, strings.Join(quotedUUIDs, ","))

	log.Printf("convertRoleIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var designationUUID, name, role, department string
		var designationID *uint32

		if err := rows.Scan(&designationUUID, &designationID, &name, &role, &department); err != nil {
			return nil, err
		}

		designationData := map[string]interface{}{
			"name":       name,
			"role":       role,
			"department": department,
		}

		if designationID != nil {
			designationData["id"] = *designationID
		}

		if existing, exists := result[designationUUID]; exists {
			if arr, ok := existing.([]interface{}); ok {
				result[designationUUID] = append(arr, designationData)
			} else {
				result[designationUUID] = []interface{}{existing, designationData}
			}
		} else {
			result[designationUUID] = designationData
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ConvertService) convertDepartmentIds(ctx context.Context, uuids []string) (map[string]interface{}, error) {
	log.Printf("convertDepartmentIds called with %d UUIDs: %v", len(uuids), uuids)
	if len(uuids) == 0 {
		return map[string]interface{}{}, nil
	}

	quotedUUIDs := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		uuid = strings.TrimSpace(uuid)
		if uuid != "" {
			quotedUUIDs = append(quotedUUIDs, fmt.Sprintf("'%s'", strings.ReplaceAll(uuid, "'", "''")))
		}
	}

	if len(quotedUUIDs) == 0 {
		return map[string]interface{}{}, nil
	}

	query := fmt.Sprintf(`
		SELECT 
			d1.designation_uuid,
			d1.designation_id as id,
			d1.display_name as name,
			d1.role,
			d1.department
		FROM testing_db.event_designation_ch AS d1
		INNER JOIN testing_db.event_designation_ch AS d2 
			ON d1.department = d2.department
		WHERE d2.designation_uuid IN (%s)
		GROUP BY d1.designation_uuid, d1.designation_id, d1.display_name, d1.role, d1.department
	`, strings.Join(quotedUUIDs, ","))

	log.Printf("convertDepartmentIds query: %s", query)
	rows, err := s.clickhouseService.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var designationUUID, name, role, department string
		var designationID *uint32

		if err := rows.Scan(&designationUUID, &designationID, &name, &role, &department); err != nil {
			return nil, err
		}

		designationData := map[string]interface{}{
			"name":       name,
			"role":       role,
			"department": department,
		}

		if designationID != nil {
			designationData["id"] = *designationID
		}

		if existing, exists := result[designationUUID]; exists {
			if arr, ok := existing.([]interface{}); ok {
				result[designationUUID] = append(arr, designationData)
			} else {
				result[designationUUID] = []interface{}{existing, designationData}
			}
		} else {
			result[designationUUID] = designationData
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
