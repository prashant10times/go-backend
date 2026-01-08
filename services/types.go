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
	"eventToDesignationMatchInfo",
	"futureExpectedStartDate",
	"futureExpectedEndDate",
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
		"publishStatus",
		"eventLocation",
		"jobComposite",
		"eventToDesignationMatchInfo",
		"futureExpectedStartDate",
		"estimatedAttendance",
		"estimatedVisitorRangeTag",
		"futureExpectedEndDate",
	},
	ResponseGroupAdvance: {
		"editions",
		"isBranded",
		"isSeries",
		"rehostDate",
		"maturity",
		"frequency",
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
	"isNew":                      "CASE WHEN ee.event_updated >= ee.event_created THEN true ELSE false END as isNew",
	"updated":                    "ee.event_updated as updated",
	"organizer":                  "ee.company_uuid as organizer_id, ee.company_name as organizer_name, ee.company_website as organizer_website, ee.companyLogoUrl as organizer_logoUrl, ee.company_id as organizer_companyId, ee.company_address as organizer_address, ee.company_city_name as organizer_city, ee.company_state as organizer_state, ee.company_country as organizer_country",
}

var DBColumnToAPIField = map[string]string{
	"id":                  "id",
	"start":               "startDateTime",
	"end":                 "endDateTime",
	"logo":                "logoUrl",
	"avgRating":           "rating",
	"updated":             "updated",
	"createdAt":           "createdAt",
	"PrimaryEventType":    "primaryEventType",
	"organizer_id":        "organizer",
	"organizer_name":      "organizer",
	"organizer_website":   "organizer",
	"organizer_logoUrl":   "organizer",
	"organizer_companyId": "organizer",
	"organizer_address":   "organizer",
	"organizer_city":      "organizer",
	"organizer_state":     "organizer",
	"organizer_country":   "organizer",
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
	for _, field := range apiFields {
		if field == "isNew" {
			hasIsNew = true
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

var EventTypeById = map[string]string{
	"e5283caa-f655-504b-8e44-49ae0edb3faa": "festival",
	"bffa5040-c654-5991-a1c5-0610e2c0ec74": "sport",
	"69cf1329-0c71-5dae-b7a9-838c5712bce0": "concert",
	"94fcb56e-2838-5d74-9092-e582d873a03e": "stage-performance",
	"3a3609e5-56df-5a8b-ad47-c9e168eb4f59": "community-group",
	"9b5524b4-60f5-5478-b3f0-38e2e12e3981": "tradeshows",
	"4de48054-46fb-5452-a23f-8aac6c00592e": "conferences",
	"ad7c83a5-b8fc-5109-a159-9306848de22c": "workshops",
	"5b37e581-53f7-5dcf-8177-c6a43774b168": "holiday",
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
