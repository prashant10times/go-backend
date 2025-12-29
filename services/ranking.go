package services

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

type RankingService struct {
	clickhouseService *ClickHouseService
}

func NewRankingService(clickhouseService *ClickHouseService) *RankingService {
	return &RankingService{
		clickhouseService: clickhouseService,
	}
}

type RankRange struct {
	Min int
	Max int
}

type EventRanking struct {
	Rank         int    `json:"rank"`
	Country      string `json:"country"`
	CategoryName string `json:"category_name"`
}

type RankingCategoryCountryRank struct {
	Country  *string `json:"country"`
	Category *string `json:"category_name"`
	Rank     int     `json:"rank"`
}

type PrioritizedRanks struct {
	Global          *EventRanking               `json:"global"`
	Country         *EventRanking               `json:"country"`
	Category        *EventRanking               `json:"category"`
	CategoryCountry *RankingCategoryCountryRank `json:"category_country"`
}

func extractRank(rankValue interface{}) int {
	switch v := rankValue.(type) {
	case int:
		return v
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return 0
	}
}

func isEffectivelyEmpty(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	cleaned := strings.Map(func(r rune) rune {
		if r >= 32 && r < 127 {
			return r
		}
		return -1
	}, s)
	return strings.TrimSpace(cleaned) == ""
}

func (s *RankingService) GetEventRankings(eventId string) (any, error) {
	ctx := context.Background()
	rankRange := &RankRange{
		Min: 1,
		//Max: 5,
	}
	rankLimit := 5

	query := fmt.Sprintf(`
		SELECT event_rank, country, category_name FROM event_ranking_ch WHERE event_id = '%s'
	`, eventId)

	log.Printf("Event Rankings Query: %s", query)
	timeStart := time.Now()
	eventRankings, err := s.clickhouseService.ExecuteQuery(ctx, query)
	timeEnd := time.Now()
	log.Printf("Event rankings query: %s", timeEnd.Sub(timeStart))

	if err != nil {
		return nil, err
	}
	defer eventRankings.Close()

	var results []map[string]interface{}
	for eventRankings.Next() {
		var eventRank uint32
		var country string
		var categoryName string
		if err := eventRankings.Scan(&eventRank, &country, &categoryName); err != nil {
			return nil, err
		}
		countryStr := strings.Map(func(r rune) rune {
			if r >= 32 && r < 127 {
				return r
			}
			return -1
		}, country)
		countryStr = strings.TrimSpace(countryStr)

		categoryNameStr := strings.Map(func(r rune) rune {
			if r >= 32 && r < 127 {
				return r
			}
			return -1
		}, categoryName)
		categoryNameStr = strings.TrimSpace(categoryNameStr)
		results = append(results, map[string]interface{}{
			"rank":          eventRank,
			"country":       countryStr,
			"category_name": categoryNameStr,
		})
	}

	if err := eventRankings.Err(); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return fiber.Map{"data": []interface{}{}}, nil
	}

	var eventRankingsList []EventRanking
	for _, result := range results {
		countryStr := ""
		if country, ok := result["country"].(string); ok {
			countryStr = country
		}
		categoryNameStr := ""
		if categoryName, ok := result["category_name"].(string); ok {
			categoryNameStr = categoryName
		}
		eventRanking := EventRanking{
			Rank:         extractRank(result["rank"]),
			Country:      countryStr,
			CategoryName: categoryNameStr,
		}
		eventRankingsList = append(eventRankingsList, eventRanking)
	}

	prioritizedRanks := PrioritizedRanks{
		Global:          nil,
		Country:         nil,
		Category:        nil,
		CategoryCountry: nil,
	}

	for _, eventRanking := range eventRankingsList {
		categoryName := eventRanking.CategoryName
		country := eventRanking.Country

		var rankType string
		categoryEmpty := isEffectivelyEmpty(categoryName)
		countryEmpty := isEffectivelyEmpty(country)

		if categoryEmpty {
			categoryName = ""
		} else {
			categoryName = strings.TrimSpace(categoryName)
		}
		if countryEmpty {
			country = ""
		} else {
			country = strings.TrimSpace(country)
		}

		if !categoryEmpty && !countryEmpty {
			rankType = "category_country"
		} else if !categoryEmpty {
			rankType = "category"
		} else if !countryEmpty {
			rankType = "country"
		} else {
			rankType = "global"
		}

		switch rankType {
		case "category_country":
			if prioritizedRanks.CategoryCountry == nil {
				categoryCountry := &RankingCategoryCountryRank{
					Category: &categoryName,
					Country:  &country,
					Rank:     eventRanking.Rank,
				}
				prioritizedRanks.CategoryCountry = categoryCountry
			} else if prioritizedRanks.CategoryCountry.Rank > eventRanking.Rank {
				if prioritizedRanks.Category != nil {
					continue
				}
				categoryCountry := &RankingCategoryCountryRank{
					Category: &categoryName,
					Country:  &country,
					Rank:     eventRanking.Rank,
				}
				prioritizedRanks.CategoryCountry = categoryCountry
			}
		case "category":
			eventRanking.CategoryName = categoryName
			eventRanking.Country = country
			if prioritizedRanks.Category == nil {
				prioritizedRanks.Category = &eventRanking
			} else if prioritizedRanks.Category.Rank > eventRanking.Rank {
				prioritizedRanks.Category = &eventRanking
			}
		case "country":
			eventRanking.CategoryName = categoryName
			eventRanking.Country = country
			if prioritizedRanks.Country == nil {
				prioritizedRanks.Country = &eventRanking
			} else if prioritizedRanks.Country.Rank > eventRanking.Rank {
				prioritizedRanks.Country = &eventRanking
			}
		case "global":
			eventRanking.CategoryName = categoryName
			eventRanking.Country = country
			if prioritizedRanks.Global == nil {
				prioritizedRanks.Global = &eventRanking
			} else if prioritizedRanks.Global.Rank > eventRanking.Rank {
				prioritizedRanks.Global = &eventRanking
			}
		}

		if rankType == "category" {
			for _, er := range eventRankingsList {
				erCategoryName := strings.TrimSpace(er.CategoryName)
				erCountry := strings.TrimSpace(er.Country)
				if erCategoryName == categoryName && erCountry != "" {
					categoryNameCopy := erCategoryName
					countryCopy := erCountry
					prioritizedRanks.CategoryCountry = &RankingCategoryCountryRank{
						Category: &categoryNameCopy,
						Country:  &countryCopy,
						Rank:     er.Rank,
					}
					break
				}
			}
		}
	}

	var prioritizedRanksArray []interface{}
	if prioritizedRanks.Global != nil {
		prioritizedRanksArray = append(prioritizedRanksArray, prioritizedRanks.Global)
	}
	if prioritizedRanks.Country != nil {
		prioritizedRanksArray = append(prioritizedRanksArray, prioritizedRanks.Country)
	}
	if prioritizedRanks.Category != nil {
		prioritizedRanksArray = append(prioritizedRanksArray, prioritizedRanks.Category)
	}
	if prioritizedRanks.CategoryCountry != nil {
		prioritizedRanksArray = append(prioritizedRanksArray, prioritizedRanks.CategoryCountry)
	}

	maxRankWhere := make([]string, 0, len(prioritizedRanksArray))
	for _, eventRanking := range prioritizedRanksArray {
		var categoryNameCondition string
		var countryCondition string

		if er, ok := eventRanking.(*EventRanking); ok {
			categoryNameRaw := er.CategoryName
			countryRaw := er.Country
			categoryEmpty := isEffectivelyEmpty(categoryNameRaw)
			countryEmpty := isEffectivelyEmpty(countryRaw)

			categoryName := ""
			if !categoryEmpty {
				categoryName = strings.TrimSpace(categoryNameRaw)
			}
			country := ""
			if !countryEmpty {
				country = strings.TrimSpace(countryRaw)
			}

			if !categoryEmpty {
				categoryNameCondition = fmt.Sprintf("category_name = '%s'", strings.ReplaceAll(categoryName, "'", "''"))
			} else {
				categoryNameCondition = "(category_name IS NULL OR trim(category_name) = '' OR category_name = '')"
			}
			if !countryEmpty {
				countryCondition = fmt.Sprintf("country = '%s'", strings.ReplaceAll(country, "'", "''"))
			} else {
				countryCondition = "(country IS NULL OR trim(country) = '' OR country = '')"
			}
		} else if ccr, ok := eventRanking.(*RankingCategoryCountryRank); ok {
			categoryNameRaw := ""
			if ccr.Category != nil {
				categoryNameRaw = *ccr.Category
			}
			countryRaw := ""
			if ccr.Country != nil {
				countryRaw = *ccr.Country
			}

			categoryEmpty := isEffectivelyEmpty(categoryNameRaw)
			countryEmpty := isEffectivelyEmpty(countryRaw)

			categoryName := ""
			if !categoryEmpty {
				categoryName = strings.TrimSpace(categoryNameRaw)
			}
			country := ""
			if !countryEmpty {
				country = strings.TrimSpace(countryRaw)
			}

			if !categoryEmpty {
				categoryNameCondition = fmt.Sprintf("category_name = '%s'", strings.ReplaceAll(categoryName, "'", "''"))
			} else {
				categoryNameCondition = "(category_name IS NULL OR trim(category_name) = '' OR category_name = '')"
			}
			if !countryEmpty {
				countryCondition = fmt.Sprintf("country = '%s'", strings.ReplaceAll(country, "'", "''"))
			} else {
				countryCondition = "(country IS NULL OR trim(country) = '' OR country = '')"
			}
		}

		query := fmt.Sprintf(
			"select * from (select event_rank, category_name, country from event_ranking_ch where %s and %s order by event_rank desc limit 1)",
			categoryNameCondition,
			countryCondition,
		)
		maxRankWhere = append(maxRankWhere, query)
	}

	var maxRankedEvents []map[string]interface{}
	if len(maxRankWhere) > 0 {
		maxRankedEventQuery := strings.Join(maxRankWhere, "  UNION DISTINCT ")
		log.Printf("Max Ranked Event Query: %s", maxRankedEventQuery)
		rows, err := s.clickhouseService.ExecuteQuery(ctx, maxRankedEventQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var rank uint32
			var categoryName string
			var country string
			if err := rows.Scan(&rank, &categoryName, &country); err != nil {
				return nil, err
			}
			maxRankedEvents = append(maxRankedEvents, map[string]interface{}{
				"rank":          rank,
				"category_name": categoryName,
				"country":       country,
			})
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	whereConditions := make([]string, 0, len(prioritizedRanksArray))
	for _, eventRanking := range prioritizedRanksArray {
		var categoryNameCondition string
		var countryCondition string
		var rankValue int

		if er, ok := eventRanking.(*EventRanking); ok {
			categoryName := er.CategoryName
			country := er.Country

			if categoryName != "" {
				categoryNameCondition = fmt.Sprintf("er.category_name = '%s'", strings.ReplaceAll(categoryName, "'", "''"))
			} else {
				categoryNameCondition = "(trim(er.category_name) = '' OR er.category_name = '')"
			}
			if country != "" {
				countryCondition = fmt.Sprintf("er.country = '%s'", strings.ReplaceAll(country, "'", "''"))
			} else {
				countryCondition = "(trim(er.country) = '' OR er.country = '')"
			}
			rankValue = er.Rank
		} else if ccr, ok := eventRanking.(*RankingCategoryCountryRank); ok {
			categoryName := ""
			if ccr.Category != nil {
				categoryName = strings.TrimSpace(*ccr.Category)
			}
			country := ""
			if ccr.Country != nil {
				country = strings.TrimSpace(*ccr.Country)
			}

			if categoryName != "" {
				categoryNameCondition = fmt.Sprintf("er.category_name = '%s'", strings.ReplaceAll(categoryName, "'", "''"))
			} else {
				categoryNameCondition = "(trim(er.category_name) = '' OR er.category_name = '')"
			}
			if country != "" {
				countryCondition = fmt.Sprintf("er.country = '%s'", strings.ReplaceAll(country, "'", "''"))
			} else {
				countryCondition = "(trim(er.country) = '' OR er.country = '')"
			}
			rankValue = ccr.Rank
		}

		rankMin := rankValue - rankLimit
		if rankMin < rankRange.Min {
			rankMin = rankRange.Min
		}
		rankMax := rankValue + rankLimit

		whereCondition := fmt.Sprintf(
			"(%s AND %s AND er.event_rank BETWEEN %d AND %d)",
			countryCondition,
			categoryNameCondition,
			rankMin,
			rankMax,
		)
		whereConditions = append(whereConditions, whereCondition)
	}

	var closestRankingsQuery string
	var closestRankings []map[string]interface{}
	var groupRankedEventsByRankType map[string]interface{}

	if len(whereConditions) > 0 {
		whereClause := strings.Join(whereConditions, " OR ")

		closestRankingsQuery = fmt.Sprintf(`
			WITH
				ranking_cte AS (
					SELECT
						event_rank,
						country,
						category,
						category_name,
						event_id
					FROM testing_db.event_ranking_ch
				),
			events_cte AS (
				SELECT
					event_id,
					event_uuid,
					event_name,
					start_date,
					end_date,
					edition_country,
					edition_city_name,
					edition_city_state,
					venue_name,
					venue_id,
					venue_city,
					edition_city_state_id
				FROM testing_db.allevent_ch
				WHERE edition_type = 'current_edition'
			)
		SELECT
			er.event_rank,
			er.category,
			er.category_name,
			er.country,
			ec.event_id,
			ec.event_uuid,
			ec.event_name,
			ec.start_date,
			ec.end_date,
			cat.category_uuid,
			arrayStringConcat(
			arrayCompact([
				if(trim(toString(any(ec.edition_country))) != '', 
					concat(
						toString(any(country_loc.id_uuid)), 
						'<val-split>', 
						any(ec.edition_country), 
						'<val-split>', 
						'COUNTRY'
					), ''),
				if(trim(toString(any(ec.edition_city_name))) != '', 
					concat(
						toString(any(city_loc.id_uuid)), 
						'<val-split>', 
						any(ec.edition_city_name), 
						'<val-split>', 
						'CITY'
					), ''),
				if(trim(toString(any(ec.edition_city_state))) != '', 
					concat(
						toString(any(state_loc.id_uuid)), 
						'<val-split>', 
						any(ec.edition_city_state), 
						'<val-split>', 
						'STATE'
					), ''),
				if(trim(toString(any(ec.venue_name))) != '', 
					concat(
						toString(any(venue_loc.id_uuid)), 
						'<val-split>', 
						any(ec.venue_name), 
						'<val-split>', 
						'VENUE'
					), '')
			]),
			'<line-split>'
		) AS location
		FROM
			ranking_cte AS er
			INNER JOIN events_cte AS ec ON er.event_id = ec.event_id
			LEFT JOIN testing_db.event_category_ch AS cat ON ec.event_id = cat.event AND er.category_name = cat.name AND cat.is_group = 1
			LEFT JOIN testing_db.location_ch AS country_loc ON ec.edition_country = country_loc.iso AND country_loc.location_type = 'COUNTRY'
			LEFT JOIN testing_db.location_ch AS city_loc ON ec.venue_city = city_loc.id AND city_loc.location_type = 'CITY'
			LEFT JOIN testing_db.location_ch AS state_loc ON ec.edition_city_state_id = state_loc.id AND state_loc.location_type = 'STATE'
			LEFT JOIN testing_db.location_ch AS venue_loc ON ec.venue_id = venue_loc.id AND venue_loc.location_type = 'VENUE'
			WHERE %s
			GROUP BY
				er.event_rank,
				er.category,
				er.category_name,
				er.country,
				ec.event_id,
				ec.event_uuid,
				ec.event_name,
				ec.start_date,
				ec.end_date,
				cat.category_uuid,
				country_loc.id_uuid,
				city_loc.id_uuid,
				state_loc.id_uuid,
				venue_loc.id_uuid
		`, whereClause)

		log.Printf("Closest Rankings Query: %s", closestRankingsQuery)
		timeStart := time.Now()
		rows, err := s.clickhouseService.ExecuteQuery(ctx, closestRankingsQuery)
		timeEnd := time.Now()
		log.Printf("Closest rankings query: %s", timeEnd.Sub(timeStart))

		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var eventRank uint32
			var category uint32
			var categoryName string
			var country string
			var eventID uint32
			var eventUUID string
			var eventName string
			var startDate time.Time
			var endDate time.Time
			var categoryUUID *string
			var location string

			if err := rows.Scan(&eventRank, &category, &categoryName, &country, &eventID, &eventUUID, &eventName, &startDate, &endDate, &categoryUUID, &location); err != nil {
				return nil, err
			}

			categoryUUIDStr := ""
			if categoryUUID != nil {
				categoryUUIDStr = *categoryUUID
			}

			closestRankings = append(closestRankings, map[string]interface{}{
				"rank":          eventRank,
				"category":      category,
				"category_name": categoryName,
				"category_uuid": categoryUUIDStr,
				"country":       country,
				"event_id":      eventID,
				"event_uuid":    eventUUID,
				"event_name":    eventName,
				"start_date":    startDate.Format("2006-01-02"),
				"end_date":      endDate.Format("2006-01-02"),
				"location":      location,
			})
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}

		groupRankedEventsByRankType = make(map[string]interface{})

		for _, curr := range closestRankings {
			rank := extractRank(curr["rank"])
			categoryNameRaw := curr["category_name"].(string)
			countryRaw := curr["country"].(string)
			eventID := curr["event_id"]
			eventUUID := curr["event_uuid"]
			eventName := curr["event_name"]
			startDate := curr["start_date"]
			endDate := curr["end_date"]
			locationStr := curr["location"].(string)
			categoryUUID := ""
			if curr["category_uuid"] != nil {
				categoryUUID = curr["category_uuid"].(string)
			}

			categoryName := categoryNameRaw
			country := countryRaw
			if isEffectivelyEmpty(categoryNameRaw) {
				categoryName = ""
			} else {
				categoryName = strings.TrimSpace(categoryNameRaw)
			}
			if isEffectivelyEmpty(countryRaw) {
				country = ""
			} else {
				country = strings.TrimSpace(countryRaw)
			}

			var rankType string
			categoryEmpty := isEffectivelyEmpty(categoryNameRaw)
			countryEmpty := isEffectivelyEmpty(countryRaw)

			if !categoryEmpty && !countryEmpty {
				rankType = "category_country"
			} else if !categoryEmpty {
				rankType = "category"
			} else if !countryEmpty {
				rankType = "country"
			} else {
				rankType = "global"
			}

			var maxRank int
			for _, mre := range maxRankedEvents {
				mreCategoryNameRaw := ""
				if mre["category_name"] != nil {
					mreCategoryNameRaw = mre["category_name"].(string)
				}
				mreCountryRaw := ""
				if mre["country"] != nil {
					mreCountryRaw = mre["country"].(string)
				}

				mreCategoryName := mreCategoryNameRaw
				if isEffectivelyEmpty(mreCategoryNameRaw) {
					mreCategoryName = ""
				} else {
					mreCategoryName = strings.TrimSpace(mreCategoryNameRaw)
				}
				mreCountry := mreCountryRaw
				if isEffectivelyEmpty(mreCountryRaw) {
					mreCountry = ""
				} else {
					mreCountry = strings.TrimSpace(mreCountryRaw)
				}

				if mreCategoryName == categoryName && mreCountry == country {
					maxRank = extractRank(mre["rank"])
					// foundMaxRank = true
					break
				}
			}

			// Fallback logic for maxRank
			// since maxRankedEvents contains entries for each prioritized rank type
			// if !foundMaxRank {
			// 	if len(maxRankedEvents) > 0 {
			// 		maxRank = extractRank(maxRankedEvents[0]["rank"])
			// 	} else {
			// 		for _, er := range eventRankingsList {
			// 			erCategoryNameRaw := er.CategoryName
			// 			erCountryRaw := er.Country
			// 			erCategoryEmpty := isEffectivelyEmpty(erCategoryNameRaw)
			// 			erCountryEmpty := isEffectivelyEmpty(erCountryRaw)

			// 			erCategoryName := ""
			// 			if !erCategoryEmpty {
			// 				erCategoryName = strings.TrimSpace(erCategoryNameRaw)
			// 			}
			// 			erCountry := ""
			// 			if !erCountryEmpty {
			// 				erCountry = strings.TrimSpace(erCountryRaw)
			// 			}

			// 			if erCategoryName == categoryName && erCountry == country {
			// 				maxRank = er.Rank
			// 				foundMaxRank = true
			// 				break
			// 			}
			// 		}
			// 		if !foundMaxRank && len(eventRankingsList) > 0 {
			// 			maxRank = eventRankingsList[0].Rank
			// 		}
			// 	}
			// }

			if groupRankedEventsByRankType[rankType] == nil {
				groupRankedEventsByRankType[rankType] = map[string]interface{}{
					"maxRank": maxRank,
				}
			}

			var prioritizedRank int
			var foundPrioritizedRank bool
			for _, er := range eventRankingsList {
				erCategoryNameRaw := er.CategoryName
				erCountryRaw := er.Country

				erCategoryNameNormalized := erCategoryNameRaw
				if isEffectivelyEmpty(erCategoryNameRaw) {
					erCategoryNameNormalized = ""
				} else {
					erCategoryNameNormalized = strings.TrimSpace(erCategoryNameRaw)
				}
				erCountryNormalized := erCountryRaw
				if isEffectivelyEmpty(erCountryRaw) {
					erCountryNormalized = ""
				} else {
					erCountryNormalized = strings.TrimSpace(erCountryRaw)
				}

				if erCategoryNameNormalized == categoryName && erCountryNormalized == country {
					prioritizedRank = er.Rank
					foundPrioritizedRank = true
					break
				}
			}

			isPreceding := false
			isCurrent := false
			if foundPrioritizedRank {
				isPreceding = rank < prioritizedRank
				isCurrent = rank == prioritizedRank
			}

			eventLocation := make(map[string]interface{})
			if locationStr != "" {
				locations := strings.Split(locationStr, "<line-split>")
				for _, loc := range locations {
					if loc == "" {
						continue
					}
					parts := strings.Split(loc, "<val-split>")
					if len(parts) >= 3 {
						locationUUID := parts[0]
						locationName := parts[1]
						locationType := parts[2]
						eventLocation[locationType] = map[string]interface{}{
							"id":           locationUUID,
							"name":         locationName,
							"locationType": locationType,
						}
					}
				}
			}

			group := groupRankedEventsByRankType[rankType].(map[string]interface{})
			eventData := map[string]interface{}{
				"id":            eventID,
				"event_uuid":    eventUUID,
				"name":          eventName,
				"rank":          rank,
				"start_date":    startDate,
				"end_date":      endDate,
				"location":      eventLocation,
				"category_uuid": categoryUUID,
			}

			if isPreceding {
				if group["precedingEvents"] == nil {
					group["precedingEvents"] = []map[string]interface{}{}
				}
				precedingEvents := group["precedingEvents"].([]map[string]interface{})
				precedingEvents = append(precedingEvents, eventData)
				group["precedingEvents"] = precedingEvents
			} else if isCurrent {
				eventData["category_name"] = categoryName
				group["currentEvent"] = eventData
			} else {
				if group["succeedingEvents"] == nil {
					group["succeedingEvents"] = []map[string]interface{}{}
				}
				succeedingEvents := group["succeedingEvents"].([]map[string]interface{})
				succeedingEvents = append(succeedingEvents, eventData)
				group["succeedingEvents"] = succeedingEvents
			}
		}
	}

	sortedRankedEvents := make(map[string]interface{})
	for rankType := range groupRankedEventsByRankType {
		group := groupRankedEventsByRankType[rankType].(map[string]interface{})

		var precedingEvents []map[string]interface{}
		if pe, ok := group["precedingEvents"].([]map[string]interface{}); ok {
			precedingEvents = pe
		}

		var succeedingEvents []map[string]interface{}
		if se, ok := group["succeedingEvents"].([]map[string]interface{}); ok {
			succeedingEvents = se
		}

		var currentEvent map[string]interface{}
		if ce, ok := group["currentEvent"].(map[string]interface{}); ok {
			currentEvent = ce
		}

		if len(precedingEvents) > 0 {
			sort.Slice(precedingEvents, func(i, j int) bool {
				return extractRank(precedingEvents[i]["rank"]) < extractRank(precedingEvents[j]["rank"])
			})
		} else {
			precedingEvents = []map[string]interface{}{}
		}

		if len(succeedingEvents) > 0 {
			sort.Slice(succeedingEvents, func(i, j int) bool {
				return extractRank(succeedingEvents[i]["rank"]) < extractRank(succeedingEvents[j]["rank"])
			})
		} else {
			succeedingEvents = []map[string]interface{}{}
		}

		sortedRankedEvents[rankType] = map[string]interface{}{
			"maxRank":          group["maxRank"],
			"precedingEvents":  precedingEvents,
			"succeedingEvents": succeedingEvents,
			"currentEvent":     currentEvent,
		}
	}

	return fiber.Map{
		"data": sortedRankedEvents,
	}, nil
}
