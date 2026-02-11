package services

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"strings"
)

type ResponseGroups string

const (
	ResponseGroupBasic      ResponseGroups = "basic"
	ResponseGroupAdvance    ResponseGroups = "advance"
	ResponseGroupInsights   ResponseGroups = "insights"
	ResponseGroupAudience   ResponseGroups = "audience"
	ResponseGroupSuggestion ResponseGroups = "suggestion"
)

var EventResponseKeys = []string{
	"id",
	"name",
	"primaryEventType",
	"categories",
	"eventTypes",
	"tags",
	"format",
	"shortName",
	"eventLocation",
	"tickets",
	"bannerUrl",
	"createdAt",
	"description",
	"designations",
	"startDateTime",
	"endDateTime",
	"entryType",
	"lastVerifiedOn",
	"logoUrl",
	"organizer",
	"status",
	"timings",
	"website",
	"10timesEventPageUrl",
	"rating",
	"isNew",
	"editions",
	"isBranded",
	"isSeries",
	"rehostDate",
	"estimatedExhibitors",
	"maturity",
	"frequency",
	"estimatedAttendance",
	"estimatedAttendanceMean",
	"estimatedVisitorRangeTag",
	"matchedKeywords",
	"rankings",
	"exhibitorsCount",
	"speakersCount",
	"sponsorsCount",
	"economicImpact",
	"economicImpactBreakdown",
	"yoyGrowth",
	"impactScore",
	"inboundScore",
	"internationalScore",
	"trustScore",
	"trustChangePercentage",
	"trustChangeTag",
	"reputationChangePercentage",
	"reputationChangeTag",
	"audienceSpread",
	"designationSpread",
	"audienceZone",
	"eventLocationHoliday",
	"alerts",
	"sourceId",
	"publishStatus",
	"jobComposite",
	"matchedKeywords",
	"matchedKeywordsPercentage",
	"futureExpectedStartDate",
	"futureExpectedEndDate",
	"futurePredictionScore",
}

var ResponseGroupsMap = map[ResponseGroups][]string{
	ResponseGroupSuggestion: {
		"id",
		"name",
		"shortName",
		"startDateTime",
		"endDateTime",
		"eventLocation",
		"organizer",
		"primaryEventType",
		"status",
	},
	ResponseGroupBasic: {
		"id",
		"name",
		"shortName",
		"primaryEventType",
		"categories",
		"eventTypes",
		"tags",
		"format",
		"tickets",
		"bannerUrl",
		"createdAt",
		"description",
		"designations",
		"startDateTime",
		"endDateTime",
		"entryType",
		"lastVerifiedOn",
		"logoUrl",
		"status",
		"timings",
		"website",
		"10timesEventPageUrl",
		"rating",
		"isNew",
		"organizer",
		"sourceId",
		"matchedKeywords",
		"matchedKeywordsPercentage",
		"publishStatus",
		"eventLocation",
		"jobComposite",
		"estimatedAttendance",
		"estimatedVisitorRangeTag",
	},
	ResponseGroupAdvance: {
		"editions",
		"isBranded",
		"isSeries",
		"rehostDate",
		"maturity",
		"frequency",
		"futureExpectedStartDate",
		"futureExpectedEndDate",
		"futurePredictionScore",
	},
	ResponseGroupInsights: {
		"estimatedExhibitors",
		"estimatedAttendanceMean",
		"rankings",
		"exhibitorsCount",
		"speakersCount",
		"sponsorsCount",
		"economicImpact",
		"economicImpactBreakdown",
		"yoyGrowth",
		"impactScore",
		"inboundScore",
		"internationalScore",
		"trustScore",
		"trustChangePercentage",
		"trustChangeTag",
		"reputationChangePercentage",
		"reputationChangeTag",
	},
	ResponseGroupAudience: {
		"audienceSpread",
		"designationSpread",
		"audienceZone",
		"followers",
	},
}

var APIFieldToDBSelect = map[string]string{
	"id":                         "ee.event_uuid as id",
	"name":                       "ee.event_name as name",
	"shortName":                  "ee.event_abbr_name as shortName",
	"primaryEventType":           "ee.PrimaryEventType as PrimaryEventType",
	"startDateTime":              "ee.start_date as start",
	"endDateTime":                "ee.end_date as end",
	"description":                "ee.event_description as description",
	"createdAt":                  "ee.event_created as createdAt",
	"logoUrl":                    "ee.event_logo as logo",
	"rating":                     "ee.event_avgRating as avgRating",
	"format":                     "ee.event_format as format",
	"tickets":                    "ee.tickets as tickets",
	"status":                     "ee.status as status",
	"entryType":                  "ee.event_pricing as entryType",
	"lastVerifiedOn":             "ee.verifiedOn as lastVerifiedOn",
	"timings":                    "ee.timings as timings",
	"website":                    "ee.edition_website as website",
	"10timesEventPageUrl":        "ee.10timesEventPageUrl as 10timesEventPageUrl",
	"publishStatus":              "ee.published as publishStatus",
	"futureExpectedStartDate":    "ee.futureExpexctedStartDate as futureExpectedStartDate",
	"futureExpectedEndDate":      "ee.futureExpexctedEndDate as futureExpectedEndDate",
	"rehostDate":                 "ee.futureExpexctedStartDate as rehostDate",
	"futurePredictionScore":      "ee.predictionScore as futurePredictionScore",
	"estimatedVisitorRangeTag":   "ee.event_estimatedVisitors as estimatedVisitorRangeTag",
	"estimatedAttendance":        "ee.estimatedSize as estimatedAttendance",
	"editions":                   "ee.event_editions as editions",
	"isBranded":                  "ee.eventBrandId as isBranded",
	"isSeries":                   "ee.eventSeriesId as isSeries",
	"maturity":                   "ee.maturity as maturity",
	"frequency":                  "ee.event_frequency as frequency",
	"estimatedExhibitors":        "ee.exhibitors_mean as estimatedExhibitors",
	"exhibitorsCount":            "ee.event_exhibitor as exhibitorsCount",
	"speakersCount":              "ee.event_speaker as speakersCount",
	"sponsorsCount":              "ee.event_sponsor as sponsorsCount",
	"economicImpact":             "ee.event_economic_value as economicImpact",
	"economicImpactBreakdown":    "ee.event_economic_breakdown as economicImpactBreakdown",
	"yoyGrowth":                  "ee.yoyGrowth as yoyGrowth",
	"impactScore":                "ee.impactScore as impactScore",
	"estimatedAttendanceMean":    "ee.estimatedVisitorsMean as estimatedAttendanceMean",
	"inboundScore":               "ee.inboundScore as inboundScore",
	"internationalScore":         "ee.internationalScore as internationalScore",
	"trustScore":                 "ee.repeatSentiment as trustScore",
	"trustChangePercentage":      "ee.repeatSentimentChangePercentage as trustChangePercentage",
	"reputationChangePercentage": "ee.reputationChangePercentage as reputationChangePercentage",
	"audienceZone":               "ee.audienceZone as audienceZone",
	"followers":                  "ee.event_followers as followers",
	"isNew":                      "CASE WHEN ee.event_updated >= ee.event_created THEN true ELSE false END as isNew",
	"updated":                    "ee.event_updated as updated",
	"organizer":                  "ee.company_uuid as organizer_id, ee.company_name as organizer_name, ee.company_website as organizer_website, ee.companyLogoUrl as organizer_logoUrl, ee.company_id as organizer_companyId, ee.company_address as organizer_address, ee.company_city_name as organizer_city, ee.company_state as organizer_state, ee.company_country as organizer_country",
	"duration":                  "(ee.end_date - ee.start_date) as duration",
	"score":                     "ee.event_score as score",
	"inboundAttendance":         "ee.inboundAttendance as inboundAttendance",
	"internationalAttendance":   "ee.internationalAttendance as internationalAttendance",
}

var DBColumnToAPIField = map[string]string{
	"id":                      "id",
	"event_id":                "id",
	"event_name":              "name",
	"start":                   "startDateTime",
	"start_date":              "startDateTime",
	"end":                     "endDateTime",
	"end_date":                "endDateTime",
	"logo":                    "logoUrl",
	"avgRating":               "rating",
	"event_avgRating":         "rating",
	"updated":                 "updated",
	"event_updated":           "updated",
	"createdAt":               "createdAt",
	"event_created":           "createdAt",
	"PrimaryEventType":        "primaryEventType",
	"organizer_id":            "organizer",
	"organizer_name":          "organizer",
	"organizer_website":       "organizer",
	"organizer_logoUrl":       "organizer",
	"organizer_companyId":     "organizer",
	"organizer_address":       "organizer",
	"organizer_city":          "organizer",
	"organizer_state":         "organizer",
	"organizer_country":       "organizer",
	"futurePredictionScore":   "futurePredictionScore",
	"event_followers":         "followers",
	"estimatedVisitorsMean":   "estimatedAttendanceMean",
	"event_exhibitor":         "exhibitorsCount",
	"event_speaker":           "speakersCount",
	"event_sponsor":           "sponsorsCount",
	"exhibitors_mean":         "estimatedExhibitors",
	"impactScore":             "impactScore",
	"event_score":             "score",
	"inboundScore":            "inboundScore",
	"internationalScore":      "internationalScore",
	"inboundAttendance":       "inboundAttendance",
	"internationalAttendance": "internationalAttendance",
	"status":                  "status",
}

var PastEditionExtraDBSelects = []string{
	"ee.edition_type", "ee.edition_uuid as edition_uuid", "ee.edition_id",
}

var PastEditionMinimalDBSelects = []string{
	"ee.event_id", "ee.edition_id", "ee.event_uuid as id",
	"ee.edition_type", "ee.edition_uuid as edition_uuid",
	"ee.start_date as start", "ee.end_date as end", "ee.status",
	"ee.company_uuid as organizer_id", "ee.company_name as organizer_name", "ee.company_website as organizer_website",
	"ee.companyLogoUrl as organizer_logoUrl", "ee.company_id as organizer_companyId",
	"ee.event_format as format",
	"ee.futureExpexctedStartDate as futureExpectedStartDate", "ee.futureExpexctedEndDate as futureExpectedEndDate",
	"ee.event_score as score",
}

var FieldsFromRelatedQueries = map[string]bool{
	"categories":        true,
	"tags":              true,
	"eventTypes":        true,
	"jobComposite":      true,
	"audienceSpread":    true,
	"designationSpread": true,
	"matchedKeywords":   true,
}

func ResolveShowValuesToFields(showValues []string) []string {
	var allFields []string
	fieldSet := make(map[string]bool)

	for _, value := range showValues {
		value = strings.TrimSpace(value)
		if groupFields, exists := ResponseGroupsMap[ResponseGroups(value)]; exists {
			for _, field := range groupFields {
				if !fieldSet[field] {
					allFields = append(allFields, field)
					fieldSet[field] = true
				}
			}
		}
	}

	return allFields
}

func MapAPIFieldsToDBSelect(apiFields []string) []string {
	var dbSelects []string
	fieldSet := make(map[string]bool)

	for _, field := range apiFields {
		if FieldsFromRelatedQueries[field] {
			continue
		}

		if dbSelect, exists := APIFieldToDBSelect[field]; exists {
			if field == "organizer" {
				organizerFields := strings.Split(dbSelect, ", ")
				for _, orgField := range organizerFields {
					if !fieldSet[orgField] {
						dbSelects = append(dbSelects, orgField)
						fieldSet[orgField] = true
					}
				}
			} else {
				if !fieldSet[dbSelect] {
					dbSelects = append(dbSelects, dbSelect)
					fieldSet[dbSelect] = true
				}
			}
		}
	}

	hasIsNew := false
	hasEstimatedExhibitors := false
	for _, field := range apiFields {
		if field == "isNew" {
			hasIsNew = true
		}
		if field == "estimatedExhibitors" {
			hasEstimatedExhibitors = true
		}
		if hasIsNew && hasEstimatedExhibitors {
			break
		}
	}
	if hasIsNew {
		if !fieldSet["ee.event_updated as updated"] {
			dbSelects = append(dbSelects, "ee.event_updated as updated")
		}
		if !fieldSet["ee.event_created as createdAt"] {
			dbSelects = append(dbSelects, "ee.event_created as createdAt")
		}
	}
	if hasEstimatedExhibitors {
		if !fieldSet["ee.exhibitors_lower_bound as exhibitors_lower_bound"] {
			dbSelects = append(dbSelects, "ee.exhibitors_lower_bound as exhibitors_lower_bound")
		}
		if !fieldSet["ee.exhibitors_upper_bound as exhibitors_upper_bound"] {
			dbSelects = append(dbSelects, "ee.exhibitors_upper_bound as exhibitors_upper_bound")
		}
	}

	return dbSelects
}

var FieldToResponseGroup = func() map[string]ResponseGroups {
	fieldToGroup := make(map[string]ResponseGroups)
	for group, fields := range ResponseGroupsMap {
		for _, field := range fields {
			fieldToGroup[field] = group
		}
	}
	return fieldToGroup
}()

func GetResponseGroupForField(fieldName string) ResponseGroups {
	if group, exists := FieldToResponseGroup[fieldName]; exists {
		return group
	}
	return ""
}

type EventTypeGroup struct {
	Group    string
	Priority int
}

var validEventTypeGroups = map[string]bool{
	"social":     true,
	"business":   true,
	"unattended": true,
}

var EventTypeGroups = map[string]EventTypeGroup{
	"festival": {
		Group:    "social",
		Priority: 1,
	},
	"sport": {
		Group:    "social",
		Priority: 2,
	},
	"concert": {
		Group:    "social",
		Priority: 3,
	},
	"stage-performance": {
		Group:    "social",
		Priority: 4,
	},
	"community-group": {
		Group:    "social",
		Priority: 5,
	},
	"tradeshows": {
		Group:    "business",
		Priority: 1,
	},
	"conferences": {
		Group:    "business",
		Priority: 2,
	},
	"workshops": {
		Group:    "business",
		Priority: 3,
	},
	"holiday": {
		Group:    "unattended",
		Priority: 1,
	},
}

func (s *SearchEventService) Encrypt(text string) (string, error) {
	if s.cfg.EventQueryEncrypt == "" {
		return "", fmt.Errorf("EVENT_QUERY_ENCRYPT not configured")
	}
	if s.cfg.EventChiprIV == "" {
		return "", fmt.Errorf("EVENT_CHIPR_IV not configured")
	}

	iv, err := hex.DecodeString(s.cfg.EventChiprIV)
	if err != nil {
		return "", fmt.Errorf("failed to decode IV: %w", err)
	}

	key := []byte(s.cfg.EventQueryEncrypt)
	if len(key) != 32 {
		keyBytes := make([]byte, 32)
		copy(keyBytes, key)
		if len(key) < 32 {
			for i := len(key); i < 32; i++ {
				keyBytes[i] = 0
			}
		}
		key = keyBytes
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	if len(iv) != 16 {
		return "", fmt.Errorf("IV must be 16 bytes, got %d", len(iv))
	}

	mode := cipher.NewCBCEncrypter(block, iv)

	plaintext := []byte(text)
	padding := 16 - len(plaintext)%16
	padtext := make([]byte, padding)
	for i := range padtext {
		padtext[i] = byte(padding)
	}
	plaintext = append(plaintext, padtext...)

	ciphertext := make([]byte, len(plaintext))
	mode.CryptBlocks(ciphertext, plaintext)

	return hex.EncodeToString(ciphertext), nil
}
