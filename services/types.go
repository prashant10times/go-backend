package services

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