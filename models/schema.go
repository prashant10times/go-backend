package models

import (
	"time"

	"gorm.io/datatypes"
)

type Status string

const (
	StatusActive   Status = "ACTIVE"
	StatusInactive Status = "INACTIVE"
)

type FilterType string

const (
	FilterTypeBasic    FilterType = "BASIC"
	FilterTypeAdvanced FilterType = "ADVANCED"
)

type ParameterType string

const (
	ParameterTypeBasic    ParameterType = "BASIC"
	ParameterTypeAdvanced ParameterType = "ADVANCED"
	ParameterTypeInsights ParameterType = "INSIGHTS"
	ParameterTypeAudience ParameterType = "AUDIENCE"
)

type User struct {
	ID           string    `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	Email        string    `gorm:"uniqueIndex;not null" json:"email"`
	Name         *string   `json:"name,omitempty"`
	PasswordHash *string   `gorm:"column:password_hash" json:"-"`
	Status       *Status   `gorm:"type:status" json:"status,omitempty"`
	CreatedAt    time.Time `gorm:"autoCreateTime;column:created_at" json:"created_at"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime;column:updated_at" json:"updated_at"`
	APITokens         []APIToken            `gorm:"foreignKey:UserID" json:"api_tokens"`
	APIAccesses       []UserAPIAccess       `gorm:"foreignKey:UserID" json:"api_accesses"`
	UsageLogs         []APIUsageLog         `gorm:"foreignKey:UserID" json:"usage_logs"`
	FilterAccesses    []UserFilterAccess    `gorm:"foreignKey:UserID" json:"filter_accesses"`
	ParameterAccesses []UserParameterAccess `gorm:"foreignKey:UserID" json:"parameter_accesses"`
}

type API struct {
	ID        string    `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	APIName   string    `gorm:"uniqueIndex;not null" json:"api_name"`
	Slug      string    `gorm:"uniqueIndex;not null" json:"slug"`
	IsActive  bool      `gorm:"default:true;index" json:"is_active"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
	UserAPIAccesses []UserAPIAccess `gorm:"foreignKey:APIID" json:"user_api_accesses"`
	UsageLogs       []APIUsageLog   `gorm:"foreignKey:APIID" json:"usage_logs"`
	APIFilters      []APIFilter     `gorm:"foreignKey:APIID" json:"api_filters"`
	APIParameters   []APIParameter  `gorm:"foreignKey:APIID" json:"api_parameters"`
}

type APIToken struct {
	ID          string     `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	UserID      string     `gorm:"not null" json:"user_id"`
	Token       string     `gorm:"uniqueIndex;not null" json:"token"`
	IsActive    bool       `gorm:"default:true" json:"is_active"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	RefreshedAt *time.Time `json:"refreshed_at,omitempty"`

	User User `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"user"`
}

type UserAPIAccess struct {
	ID         string    `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	UserID     string    `gorm:"not null" json:"user_id"`
	APIID      string    `gorm:"not null" json:"api_id"`
	DailyLimit int       `gorm:"default:100" json:"daily_limit"`
	HasAccess  bool      `gorm:"default:true" json:"has_access"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime" json:"updated_at"`

	User User `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"user"`
	API  API  `gorm:"foreignKey:APIID;constraint:OnDelete:CASCADE" json:"api"`
}

type APIUsageLog struct {
	ID              string          `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	UserID          string          `gorm:"not null" json:"user_id"`
	APIID           string          `gorm:"not null" json:"api_id"`
	Endpoint        string          `gorm:"not null" json:"endpoint"`
	Payload         *datatypes.JSON `json:"payload,omitempty"`
	IPAddress       string          `gorm:"not null" json:"ip_address"`
	StatusCode      *int            `json:"status_code,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	CreatedAt       time.Time       `gorm:"autoCreateTime;index:idx_user_api_created,priority:3;index:idx_created;index:idx_user_api_status,priority:3" json:"created_at"`
	APIResponseTime *float64        `json:"api_response_time,omitempty"`

	User User `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"user"`
	API  API  `gorm:"foreignKey:APIID;constraint:OnDelete:CASCADE" json:"api"`
}

type APIFilter struct {
	ID         string     `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	APIID      string     `gorm:"not null" json:"api_id"`
	FilterType FilterType `gorm:"type:filter_type;not null" json:"filter_type"`
	FilterName string     `gorm:"not null" json:"filter_name"`
	IsPaid     bool       `gorm:"default:false" json:"is_paid"`
	IsActive   bool       `gorm:"default:true" json:"is_active"`
	CreatedAt  time.Time  `gorm:"autoCreateTime" json:"created_at"`

	API          API                `gorm:"foreignKey:APIID;constraint:OnDelete:CASCADE" json:"api"`
	UserAccesses []UserFilterAccess `gorm:"foreignKey:FilterID" json:"user_accesses"`
}

type UserFilterAccess struct {
	ID        string    `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	UserID    string    `gorm:"not null" json:"user_id"`
	FilterID  string    `gorm:"not null" json:"filter_id"`
	HasAccess bool      `gorm:"default:true" json:"has_access"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`

	User   User      `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"user"`
	Filter APIFilter `gorm:"foreignKey:FilterID;constraint:OnDelete:CASCADE" json:"filter"`
}

type APIParameter struct {
	ID            string        `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	APIID         string        `gorm:"not null" json:"api_id"`
	ParameterName string        `gorm:"not null" json:"parameter_name"`
	ParameterType ParameterType `gorm:"type:parameter_type;default:'BASIC'" json:"parameter_type"`
	IsPaid        bool          `gorm:"default:false" json:"is_paid"`
	IsActive      bool          `gorm:"default:true" json:"is_active"`
	CreatedAt     time.Time     `json:"created_at"`

	API          API                   `gorm:"foreignKey:APIID;constraint:OnDelete:CASCADE" json:"api"`
	UserAccesses []UserParameterAccess `gorm:"foreignKey:ParameterID" json:"user_accesses"`
}

type UserParameterAccess struct {
	ID          string    `gorm:"type:text;primary_key;default:uuid_generate_v4()" json:"id"`
	UserID      string    `gorm:"not null" json:"user_id"`
	ParameterID string    `gorm:"not null" json:"parameter_id"`
	HasAccess   bool      `gorm:"default:true" json:"has_access"`
	CreatedAt   time.Time `json:"created_at"`

	User      User         `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"user"`
	Parameter APIParameter `gorm:"foreignKey:ParameterID;constraint:OnDelete:CASCADE" json:"parameter"`
}

func (*User) TableName() string {
	return "User"
}

func (*API) TableName() string {
	return "Api"
}

func (*APIToken) TableName() string {
	return "ApiToken"
}

func (*UserAPIAccess) TableName() string {
	return "UserApiAccess"
}

func (*APIUsageLog) TableName() string {
	return "ApiUsageLog"
}

func (*APIFilter) TableName() string {
	return "ApiFilter"
}

func (*UserFilterAccess) TableName() string {
	return "UserFilterAccess"
}

func (*APIParameter) TableName() string {
	return "ApiParameter"
}

func (*UserParameterAccess) TableName() string {
	return "UserParameterAccess"
}
