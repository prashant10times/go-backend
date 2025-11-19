package services

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
		"id", //done
		"name", //done
		"shortName", //done
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

type EventTypeGroup struct {
	Group    string
	Priority int
}

var validEventTypeGroups = map[string]bool{
	"social":     true,
	"business":   true,
	"unattended": true,
}

// var EventTypeById = map[string]string{
// 	"e5283caa-f655-504b-8e44-49ae0edb3faa": "festival",
// 	"bffa5040-c654-5991-a1c5-0610e2c0ec74": "sport",
// 	"69cf1329-0c71-5dae-b7a9-838c5712bce0": "concert",
// 	"94fcb56e-2838-5d74-9092-e582d873a03e": "stage-performance",
// 	"3a3609e5-56df-5a8b-ad47-c9e168eb4f59": "community-group",
// 	"9b5524b4-60f5-5478-b3f0-38e2e12e3981": "tradeshows",
// 	"4de48054-46fb-5452-a23f-8aac6c00592e": "conferences",
// 	"ad7c83a5-b8fc-5109-a159-9306848de22c": "workshops",
// 	"5b37e581-53f7-5dcf-8177-c6a43774b168": "holiday",
// }

var EventTypeById = map[string]string{
	"455a1427-5459-5ae3-be3a-1c680f4bc4c7": "festival",
	"504013af-dfc6-5e43-b1ca-34b0ec065c86": "sport",
	"c1c8b213-0f3d-57fd-9555-5bcc7135130a": "concert",
	// "94fcb56e-2838-5d74-9092-e582d873a03e": "stage-performance",
	"c42d0a8e-d77e-5899-a3e4-3147803d2309": "community-group",
	"41ee28a5-918e-59bc-ada8-9f6e194869c4": "tradeshows",
	"7050e5af-f491-5280-aa66-d6e8c55b1b3d": "conferences",
	"21a41a54-43b0-5198-8306-5e8326a259ef": "workshops",
	// "5b37e581-53f7-5dcf-8177-c6a43774b168": "holiday",
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
