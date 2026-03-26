package location

import (
	"context"
	"fmt"
	"log"
	"search-event-go/middleware"
	"search-event-go/models"
	"search-event-go/services"
	"strings"
)

type LocationService struct {
	clickhouseService *services.ClickHouseService
}

func NewLocationService(clickhouseService *services.ClickHouseService) *LocationService {
	return &LocationService{clickhouseService: clickhouseService}
}

type Location struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	DisplayName  string                 `json:"displayName"`
	Slug         *string                `json:"slug,omitempty"`
	LocationType string                 `json:"locationType"`
	Address      *string                `json:"address,omitempty"`
	Latitude     *float64               `json:"latitude,omitempty"`
	Longitude    *float64               `json:"longitude,omitempty"`
	ISO          *string                `json:"iso,omitempty"`
	Regions      []string               `json:"regions,omitempty"`
	City         *LocationDetail        `json:"city,omitempty"`
	State        *LocationDetail        `json:"state,omitempty"`
	Country      *LocationDetailWithISO `json:"country,omitempty"`
}

type LocationDetail struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
	Slug      *string  `json:"slug,omitempty"`
}

type LocationDetailWithISO struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
	ISO       *string  `json:"iso,omitempty"`
	Slug      *string  `json:"slug,omitempty"`
}

func newRegionAPIResponse(regionLabel string) Location {
	emptyStr := ""
	z := 0.0
	return Location{
		ID:           "",
		Name:         regionLabel,
		DisplayName:  regionLabel,
		Slug:         &emptyStr,
		LocationType: "REGION",
		Latitude:     &z,
		Longitude:    &z,
		ISO:          &emptyStr,
		Regions:      []string{regionLabel},
	}
}

func buildRegionDistinctSelect(query models.LocationQueryDto, take, offset int) string {
	baseParts := []string{"location_type = 'COUNTRY'", "published = 1"}
	if len(query.ParsedLocationIds) > 0 {
		escapedIDs := make([]string, len(query.ParsedLocationIds))
		for i, id := range query.ParsedLocationIds {
			escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
		}
		baseParts = append(baseParts, fmt.Sprintf("id_uuid IN (%s)", strings.Join(escapedIDs, ",")))
	}
	if query.ID10x != "" {
		id10xParts := strings.Split(query.ID10x, ",")
		escapedID10x := make([]string, 0, len(id10xParts))
		for _, id10x := range id10xParts {
			id10x = strings.TrimSpace(id10x)
			if id10x != "" {
				escapedID10x = append(escapedID10x, fmt.Sprintf("'%s'", strings.ReplaceAll(id10x, "'", "''")))
			}
		}
		if len(escapedID10x) > 0 {
			baseParts = append(baseParts, fmt.Sprintf("id_10x IN (%s)", strings.Join(escapedID10x, ",")))
		}
	}
	baseWhere := strings.Join(baseParts, " AND ")

	var rankSelect string
	var labelFilter string
	var windowOrder string

	if query.ParsedQuery != nil && strings.TrimSpace(*query.ParsedQuery) != "" {
		queryLower := strings.ToLower(strings.TrimSpace(*query.ParsedQuery))
		keywords := strings.Fields(queryLower)
		labelConds := make([]string, 0, len(keywords)+1)
		for _, keyword := range keywords {
			keyword = strings.TrimSpace(keyword)
			if keyword != "" {
				escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
				labelConds = append(labelConds, fmt.Sprintf("ilike(region_label, '%%%s%%')", escapedKeyword))
			}
		}
		queryPattern := strings.ReplaceAll(queryLower, " ", "%")
		escapedQueryPattern := strings.ReplaceAll(queryPattern, "'", "''")
		if escapedQueryPattern != "" {
			labelConds = append(labelConds, fmt.Sprintf("ilike(region_label, '%%%s%%')", escapedQueryPattern))
		}
		if len(labelConds) > 0 {
			labelFilter = "AND (" + strings.Join(labelConds, " OR ") + ")"
		}

		escapedQueryLower := strings.ReplaceAll(queryLower, "'", "''")
		rankCase := "CASE\n"
		rankCase += fmt.Sprintf("  WHEN lower(region_label) = '%s' THEN 0\n", escapedQueryLower)
		rankCase += fmt.Sprintf("  WHEN ilike(region_label, '%%%s%%') THEN 1\n", escapedQueryPattern)
		nextRank := 2
		for _, keyword := range keywords {
			keyword = strings.TrimSpace(keyword)
			if keyword != "" {
				escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
				rankCase += fmt.Sprintf("  WHEN ilike(region_label, '%%%s%%') THEN %d\n", escapedKeyword, nextRank)
				nextRank++
			}
		}
		rankCase += fmt.Sprintf("  ELSE %d\n", nextRank)
		rankCase += "END"
		rankSelect = rankCase + " AS search_rank"
		windowOrder = "search_rank ASC, length(region_label) ASC, region_label ASC"
	} else {
		rankSelect = "0 AS search_rank"
		windowOrder = "region_label ASC"
	}

	return fmt.Sprintf(`
WITH expanded AS (
	SELECT
		region_label,
		%s
	FROM testing_db.location_ch
	ARRAY JOIN ifNull(regions, []) AS region_label
	WHERE %s
	  AND length(trim(region_label)) > 0
	  %s
),
dedup AS (
	SELECT region_label, min(search_rank) AS search_rank
	FROM expanded
	GROUP BY region_label
),
ranked AS (
	SELECT
		region_label,
		row_number() OVER (ORDER BY %s) AS rn
	FROM dedup
)
SELECT region_label
FROM ranked
ORDER BY rn
LIMIT %d OFFSET %d
`, rankSelect, baseWhere, labelFilter, windowOrder, take, offset)
}

func (s *LocationService) SearchLocations(query models.LocationQueryDto) (interface{}, error) {
	ctx := context.Background()

	whereConditions := []string{}
	var rankCase string
	var windowOrderBy string
	var venueSearchWhereClause string
	var keywordMatchCountExpr string
	isVenueQuery := query.ParsedLocationType != nil && *query.ParsedLocationType == models.LocationTypeVenue
	isRegionQuery := query.ParsedLocationType != nil && *query.ParsedLocationType == models.LocationTypeRegion

	if query.Slug != "" {
		slugParts := strings.Split(query.Slug, ",")
		escapedSlugs := make([]string, 0, len(slugParts))
		for _, slug := range slugParts {
			slug = strings.TrimSpace(slug)
			if slug != "" {
				escapedSlugs = append(escapedSlugs, fmt.Sprintf("'%s'", strings.ReplaceAll(slug, "'", "''")))
			}
		}
		if len(escapedSlugs) > 0 {
			whereConditions = append(whereConditions, fmt.Sprintf("slug IN (%s)", strings.Join(escapedSlugs, ",")))
			cityType := models.LocationTypeCity
			query.ParsedLocationType = &cityType
			isVenueQuery = false
		}
	}

	if !isRegionQuery {
		if len(query.ParsedLocationIds) > 0 {
			escapedIDs := make([]string, len(query.ParsedLocationIds))
			for i, id := range query.ParsedLocationIds {
				escapedIDs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(id, "'", "''"))
			}
			if isVenueQuery {
				whereConditions = append(whereConditions, fmt.Sprintf("l.id_uuid IN (%s)", strings.Join(escapedIDs, ",")))
			} else {
				whereConditions = append(whereConditions, fmt.Sprintf("id_uuid IN (%s)", strings.Join(escapedIDs, ",")))
			}
		} else {
			if query.ParsedQuery != nil && *query.ParsedQuery != "" {
				queryLower := strings.ToLower(*query.ParsedQuery)
				keywords := strings.Fields(queryLower)

				if isVenueQuery {
					concatenatedStr := "l.search_text"

					queryPattern := strings.ReplaceAll(queryLower, " ", "%")
					escapedQueryPattern := strings.ReplaceAll(queryPattern, "'", "''")

					nameConditions := make([]string, 0, len(keywords)+1)
					for _, keyword := range keywords {
						keyword = strings.TrimSpace(keyword)
						if keyword != "" {
							escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
							nameConditions = append(nameConditions, fmt.Sprintf("(l.name ILIKE '%%%s%%' OR l.alias ILIKE '%%%s%%' OR %s ILIKE '%%%s%%')", escapedKeyword, escapedKeyword, concatenatedStr, escapedKeyword))
						}
					}
					if len(nameConditions) > 0 {
						nameConditions = append(nameConditions, fmt.Sprintf("%s ILIKE '%%%s%%'", concatenatedStr, escapedQueryPattern))
						venueSearchWhereClause = fmt.Sprintf("(%s)", strings.Join(nameConditions, " OR "))
						whereConditions = append(whereConditions, venueSearchWhereClause)
					}
					escapedQueryLower := strings.ReplaceAll(queryLower, "'", "''")
					escapedQueryPattern = strings.ReplaceAll(queryPattern, "'", "''")

					rankCase = "CASE\n"
					rankCase += fmt.Sprintf("  WHEN lower(l.name) = '%s' THEN 0\n", escapedQueryLower)
					if len(keywords) > 0 {
						firstKeyword := strings.TrimSpace(keywords[0])
						if firstKeyword != "" {
							escapedFirstKeyword := strings.ReplaceAll(firstKeyword, "'", "''")
							rankCase += fmt.Sprintf("  WHEN l.name ILIKE '%%%s%%' THEN 1\n", escapedFirstKeyword)
							rankCase += fmt.Sprintf("  WHEN %s ILIKE '%%%s%%' THEN 2\n", concatenatedStr, escapedQueryPattern)
							rankCase += fmt.Sprintf("  WHEN l.name ILIKE '%s%%' THEN 3\n", escapedQueryLower)
							rankCase += fmt.Sprintf("  WHEN l.alias ILIKE '%%%s%%' THEN 4\n", escapedFirstKeyword)
						}
					}

					for i := 1; i < len(keywords); i++ {
						keyword := strings.TrimSpace(keywords[i])
						if keyword != "" {
							escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
							rankCase += fmt.Sprintf("  WHEN l.name ILIKE '%%%s%%' OR l.alias ILIKE '%%%s%%' OR %s ILIKE '%%%s%%' THEN 5\n", escapedKeyword, escapedKeyword, concatenatedStr, escapedKeyword)
						}
					}
					rankCase += "  ELSE 5\n"
					rankCase += "END AS search_rank"

					keywordMatchParts := make([]string, 0, len(keywords))
					for _, keyword := range keywords {
						keyword = strings.TrimSpace(keyword)
						if keyword != "" {
							escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
							keywordMatchParts = append(keywordMatchParts, fmt.Sprintf("if(l.name ILIKE '%%%s%%' OR l.alias ILIKE '%%%s%%' OR l.search_text ILIKE '%%%s%%', 1, 0)", escapedKeyword, escapedKeyword, escapedKeyword))
						}
					}
					if len(keywordMatchParts) > 0 {
						keywordMatchCountExpr = fmt.Sprintf("(\n\t\t\t\t%s\n\t\t\t) AS keyword_match_count", strings.Join(keywordMatchParts, " +\n\t\t\t\t"))
					}

					// Build window function ORDER BY clause for venue query
					windowOrderBy = fmt.Sprintf("%s,\n\t\t\tlength(l.name) ASC,\n\t\t\tl.location_type DESC,\n\t\t\tl.name,\n\t\t\tl.id_uuid", rankCase)
				} else {
					nameConditions := make([]string, 0, len(keywords))
					for _, keyword := range keywords {
						keyword = strings.TrimSpace(keyword)
						if keyword != "" {
							escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
							nameConditions = append(nameConditions, fmt.Sprintf("(name ILIKE '%%%s%%' OR alias ILIKE '%%%s%%')", escapedKeyword, escapedKeyword))
						}
					}
					if len(nameConditions) > 0 {
						whereConditions = append(whereConditions, fmt.Sprintf("(%s)", strings.Join(nameConditions, " OR ")))
					}

					rankCase = "CASE\n"
					escapedQueryLower := strings.ReplaceAll(queryLower, "'", "''")
					rankCase += fmt.Sprintf("  WHEN lower(name) = '%s' THEN 0\n", escapedQueryLower)
					rankCase += fmt.Sprintf("  WHEN name ILIKE '%s%%' THEN 1\n", escapedQueryLower)

					for i, keyword := range keywords {
						keyword = strings.TrimSpace(keyword)
						if keyword != "" {
							escapedKeyword := strings.ReplaceAll(keyword, "'", "''")
							rankCase += fmt.Sprintf("  WHEN name ILIKE '%%%s%%' OR alias ILIKE '%%%s%%' THEN %d\n", escapedKeyword, escapedKeyword, i+2)
						}
					}
					rankCase += fmt.Sprintf("  ELSE %d\n", len(keywords)+2)
					rankCase += "END AS search_rank"

					windowOrderBy = fmt.Sprintf("%s,\n\t\t\tlength(name) ASC,\n\t\t\tlocation_type DESC,\n\t\t\tname,\n\t\t\tid_uuid", rankCase)
				}
			}

			if query.ParsedLocationType != nil {
				locationType := string(*query.ParsedLocationType)
				if *query.ParsedLocationType == models.LocationTypeRegion {
					locationType = "COUNTRY"
				}
				if isVenueQuery {
					whereConditions = append(whereConditions, fmt.Sprintf("l.location_type = '%s'", locationType))
				} else {
					whereConditions = append(whereConditions, fmt.Sprintf("location_type = '%s'", locationType))
				}
			} else {
				if isVenueQuery {
					whereConditions = append(whereConditions, "l.location_type = 'VENUE'")
				} else {
					whereConditions = append(whereConditions, "location_type IN ('CITY', 'COUNTRY', 'STATE')")
				}
			}

			if query.ID10x != "" {
				id10xParts := strings.Split(query.ID10x, ",")
				escapedID10x := make([]string, 0, len(id10xParts))
				for _, id10x := range id10xParts {
					id10x = strings.TrimSpace(id10x)
					if id10x != "" {
						escapedID10x = append(escapedID10x, fmt.Sprintf("'%s'", strings.ReplaceAll(id10x, "'", "''")))
					}
				}
				if len(escapedID10x) > 0 {
					if isVenueQuery {
						whereConditions = append(whereConditions, fmt.Sprintf("l.id_10x IN (%s)", strings.Join(escapedID10x, ",")))
					} else {
						whereConditions = append(whereConditions, fmt.Sprintf("id_10x IN (%s)", strings.Join(escapedID10x, ",")))
					}
				}
			}
		}
	}

	if !isRegionQuery {
		if isVenueQuery {
			whereConditions = append(whereConditions, "l.published = 1")
		} else {
			whereConditions = append(whereConditions, "published = 1")
		}
	}

	whereClause := strings.Join(whereConditions, " AND ")
	if whereClause == "" {
		whereClause = "1=1"
	}

	take := query.ParsedTake
	if take == 0 {
		take = 10
	}
	offset := query.ParsedOffset

	var selectQuery string

	if isRegionQuery {
		selectQuery = buildRegionDistinctSelect(query, take, offset)
	} else if isVenueQuery && query.ParsedQuery != nil && *query.ParsedQuery != "" {
		// Build PREWHERE and main WHERE for optimized venue query
		venueLocationType := "VENUE"
		if query.ParsedLocationType != nil {
			venueLocationType = string(*query.ParsedLocationType)
		}
		venuePreWhereClause := fmt.Sprintf("l.location_type = '%s' AND l.published = 1", venueLocationType)

		venueMainWhereParts := []string{}
		if venueSearchWhereClause != "" {
			venueMainWhereParts = append(venueMainWhereParts, venueSearchWhereClause)
		}
		if query.ID10x != "" {
			id10xParts := strings.Split(query.ID10x, ",")
			escapedID10x := make([]string, 0, len(id10xParts))
			for _, id10x := range id10xParts {
				id10x = strings.TrimSpace(id10x)
				if id10x != "" {
					escapedID10x = append(escapedID10x, fmt.Sprintf("'%s'", strings.ReplaceAll(id10x, "'", "''")))
				}
			}
			if len(escapedID10x) > 0 {
				venueMainWhereParts = append(venueMainWhereParts, fmt.Sprintf("l.id_10x IN (%s)", strings.Join(escapedID10x, ",")))
			}
		}
		venueMainWhereClause := strings.Join(venueMainWhereParts, " AND ")
		if venueMainWhereClause == "" {
			venueMainWhereClause = "1=1"
		}

		rankedFirstSelect := "l.id_uuid AS id,\n\t\t\tl.name,\n\t\t\tl.alias,\n\t\t\tl.location_type,\n\t\t\t" + rankCase
		if keywordMatchCountExpr != "" {
			rankedFirstSelect += ",\n\t\t\t" + keywordMatchCountExpr
		}

		locationIdsSelect := "id,\n\t\t\t\tname,\n\t\t\t\talias,\n\t\t\t\tlocation_type,\n\t\t\t\tsearch_rank"
		locationIdsOrderBy := "search_rank, length(name) ASC, location_type DESC, name, id"
		if keywordMatchCountExpr != "" {
			locationIdsSelect += ",\n\t\t\t\tkeyword_match_count"
			locationIdsOrderBy = "search_rank, keyword_match_count DESC, length(name) ASC, location_type DESC, name, id"
		}

		locationIdsOrderByClause := "ORDER BY search_rank, rn"
		if keywordMatchCountExpr != "" {
			locationIdsOrderByClause = "ORDER BY search_rank, keyword_match_count DESC, rn"
		}

		finalOrderBy := "ORDER BY search_rank, rn"
		if keywordMatchCountExpr != "" {
			finalOrderBy = "ORDER BY search_rank, keyword_match_count DESC, rn"
		}

		selectQuery = fmt.Sprintf(`
		WITH ranked_first AS (
			SELECT 
				%s
			FROM testing_db.location_ch AS l
			PREWHERE %s
			WHERE %s
		),
		location_ids AS (
			SELECT 
				%s,
				row_number() OVER (
					ORDER BY %s
				) AS rn
			FROM ranked_first
			%s
			LIMIT %d
			OFFSET %d
		)
		SELECT 
			location.id_uuid AS id,
			location.name,
			location.slug AS location_slug,
			location.location_type,
			location.address AS address,
			location.latitude,
			location.longitude,
			replace(location.id_10x, 'country-', '') AS location_iso,
			ifNull(location.regions, []) AS location_regions,
			city.id_uuid AS city_id,
			city.name AS city_name,
			city.latitude AS city_latitude,
			city.longitude AS city_longitude,
			city.slug AS city_slug,
			country.id_uuid AS country_id,
			country.name AS country_name,
			country.latitude AS country_latitude,
			country.longitude AS country_longitude,
			country.slug AS country_slug,
			replace(country.id_10x, 'country-', '') AS country_iso,
			state.id_uuid AS state_id,
			state.name AS state_name,
			state.latitude AS state_latitude,
			state.longitude AS state_longitude,
			state.slug AS state_slug,
			state.country_uuid AS state_country_id
		FROM location_ids
		LEFT JOIN testing_db.location_ch AS location
			ON location_ids.id = location.id_uuid 
			AND location.location_type IN ('VENUE', 'CITY', 'COUNTRY', 'STATE') 
			AND location.published = 1
		LEFT JOIN testing_db.location_ch AS city
			ON location.city_uuid = city.id_uuid 
			AND city.location_type = 'CITY' 
			AND city.published = 1
		LEFT JOIN testing_db.location_ch AS country
			ON location.country_uuid = country.id_uuid 
			AND country.location_type = 'COUNTRY' 
			AND country.published = 1
		LEFT JOIN testing_db.location_ch AS state
			ON location.state_uuid = state.id_uuid 
			AND state.location_type = 'STATE' 
			AND state.published = 1
		%s
		`, rankedFirstSelect, venuePreWhereClause, venueMainWhereClause, locationIdsSelect, locationIdsOrderBy, locationIdsOrderByClause, take, offset, finalOrderBy)
	} else {
		cteSelectFields := "id_uuid,\n\t\t\tname,\n\t\t\talias,\n\t\t\tlocation_type"
		if rankCase != "" {
			cteSelectFields += ",\n\t\t\t" + rankCase
		}

		finalOrderBy := ""
		cteSelectFieldsWithRowNum := cteSelectFields
		cteOrderByClause := ""

		if rankCase != "" {
			finalOrderBy = " ORDER BY search_rank, rn"
			cteSelectFieldsWithRowNum += ",\n\t\t\trow_number() OVER (\n\t\t\t\tORDER BY " + windowOrderBy + "\n\t\t\t) AS rn"
			cteOrderByClause = "\n\t\t\tORDER BY search_rank, rn"
		}

		selectQuery = fmt.Sprintf(`
		WITH location_ids AS (
			SELECT 
				%s
			FROM testing_db.location_ch
			WHERE %s%s
			LIMIT %d
			OFFSET %d
		)
		SELECT 
			location.id_uuid AS id,
			location.name,
			location.slug AS location_slug,
			location.location_type,
			location.address AS address,
			location.latitude,
			location.longitude,
			replace(location.id_10x, 'country-', '') AS location_iso,
			ifNull(location.regions, []) AS location_regions,
			city.id_uuid AS city_id,
			city.name AS city_name,
			city.latitude AS city_latitude,
			city.longitude AS city_longitude,
			city.slug AS city_slug,
			country.id_uuid AS country_id,
			country.name AS country_name,
			country.latitude AS country_latitude,
			country.longitude AS country_longitude,
			country.slug AS country_slug,
			replace(country.id_10x, 'country-', '') AS country_iso,
			state.id_uuid AS state_id,
			state.name AS state_name,
			state.latitude AS state_latitude,
			state.longitude AS state_longitude,
			state.slug AS state_slug,
			state.country_uuid AS state_country_id
		FROM location_ids
		LEFT JOIN testing_db.location_ch AS location
			ON location_ids.id_uuid = location.id_uuid 
			AND location.location_type IN ('VENUE', 'CITY', 'COUNTRY', 'STATE') 
			AND location.published = 1
		LEFT JOIN testing_db.location_ch AS city
			ON location.city_uuid = city.id_uuid 
			AND city.location_type = 'CITY' 
			AND city.published = 1
		LEFT JOIN testing_db.location_ch AS country
			ON location.country_uuid = country.id_uuid 
			AND country.location_type = 'COUNTRY' 
			AND country.published = 1
		LEFT JOIN testing_db.location_ch AS state
			ON location.state_uuid = state.id_uuid 
			AND state.location_type = 'STATE' 
			AND state.published = 1%s
		`, cteSelectFieldsWithRowNum, whereClause, cteOrderByClause, take, offset, finalOrderBy)
	}

	log.Printf("Location query: %s", selectQuery)

	rows, err := s.clickhouseService.ExecuteQuery(ctx, selectQuery)
	if err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}
	defer rows.Close()

	var locations []Location
	if isRegionQuery {
		for rows.Next() {
			var regionLabel string
			if err := rows.Scan(&regionLabel); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}
			locations = append(locations, newRegionAPIResponse(regionLabel))
		}
	} else {
		for rows.Next() {
			var loc Location
			var locationSlug, locationISO *string
			var cityIDUUID, cityName, citySlug *string
			var cityLatitude, cityLongitude *float64
			var countryIDUUID, countryName, countrySlug, countryISO *string
			var countryLatitude, countryLongitude *float64
			var stateIDUUID, stateName, stateSlug, stateCountryID *string
			var stateLatitude, stateLongitude *float64

			var locationAddress *string
			var regions []string
			if err := rows.Scan(
				&loc.ID,
				&loc.Name,
				&locationSlug,
				&loc.LocationType,
				&locationAddress,
				&loc.Latitude,
				&loc.Longitude,
				&locationISO,
				&regions,
				&cityIDUUID,
				&cityName,
				&cityLatitude,
				&cityLongitude,
				&citySlug,
				&countryIDUUID,
				&countryName,
				&countryLatitude,
				&countryLongitude,
				&countrySlug,
				&countryISO,
				&stateIDUUID,
				&stateName,
				&stateLatitude,
				&stateLongitude,
				&stateSlug,
				&stateCountryID,
			); err != nil {
				return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
			}

			loc.Slug = locationSlug
			if len(regions) > 0 {
				loc.Regions = regions
			}
			if loc.LocationType == "VENUE" {
				loc.Address = locationAddress
			}

			displayNameParts := []string{loc.Name}
			if cityName != nil && *cityName != "" {
				displayNameParts = append(displayNameParts, *cityName)
			}
			if stateName != nil && *stateName != "" {
				displayNameParts = append(displayNameParts, *stateName)
			}
			if countryName != nil && *countryName != "" {
				displayNameParts = append(displayNameParts, *countryName)
			}
			loc.DisplayName = strings.Join(displayNameParts, ", ")

			if loc.LocationType == "COUNTRY" && locationISO != nil && *locationISO != "" {
				loc.ISO = locationISO
			}

			// Add city for VENUE locations
			if loc.LocationType == "VENUE" && cityIDUUID != nil && *cityIDUUID != "" && cityName != nil {
				loc.City = &LocationDetail{
					ID:        *cityIDUUID,
					Name:      *cityName,
					Latitude:  cityLatitude,
					Longitude: cityLongitude,
					Slug:      citySlug,
				}
			}

			// Add state for VENUE and CITY locations
			if (loc.LocationType == "VENUE" || loc.LocationType == "CITY") && stateIDUUID != nil && *stateIDUUID != "" && stateName != nil {
				loc.State = &LocationDetail{
					ID:        *stateIDUUID,
					Name:      *stateName,
					Latitude:  stateLatitude,
					Longitude: stateLongitude,
					Slug:      stateSlug,
				}
			}

			// Add country for VENUE, CITY, and STATE locations
			if (loc.LocationType == "VENUE" || loc.LocationType == "CITY" || loc.LocationType == "STATE") && countryIDUUID != nil && *countryIDUUID != "" && countryName != nil {
				loc.Country = &LocationDetailWithISO{
					ID:        *countryIDUUID,
					Name:      *countryName,
					Latitude:  countryLatitude,
					Longitude: countryLongitude,
					ISO:       countryISO,
					Slug:      countrySlug,
				}
			}

			locations = append(locations, loc)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, middleware.NewInternalServerError("Something went wrong", err.Error())
	}

	if len(locations) == 0 {
		return map[string]interface{}{
			"formattedLocations": []Location{},
		}, nil
	}

	return map[string]interface{}{
		"formattedLocations": locations,
	}, nil
}
