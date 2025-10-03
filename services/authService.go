package services

import (
	"search-event-go/models"

	"gorm.io/gorm"
)

type AuthService struct {
	db *gorm.DB
}

func NewAuthService(db *gorm.DB) *AuthService {
	return &AuthService{db: db}
}

// GiveFilterAndApiAccess grants API and filter access to a user in a single transaction
func (s *AuthService) GiveFilterAndApiAccess(tx *gorm.DB, userId string, apiId string) error {
	var api models.API
	err := tx.Where("id = ?", apiId).First(&api).Error
	if err != nil {
		return err
	}

	// Give API access to user
	userApiAccess := models.UserAPIAccess{
		UserID:    userId,
		APIID:     apiId,
		HasAccess: true,
	}

	err = tx.Where("user_id = ? AND api_id = ?", userId, apiId).
		Assign(models.UserAPIAccess{HasAccess: true}).
		FirstOrCreate(&userApiAccess).Error
	if err != nil {
		return err
	}

	// Get all basic filters for this API
	var basicFilters []models.APIFilter
	err = tx.Where("api_id = ? AND is_active = ? AND filter_type = ?", apiId, true, "BASIC").Find(&basicFilters).Error
	if err != nil {
		return err
	}

	// Grant access to all basic filters
	for _, filter := range basicFilters {
		userFilterAccess := models.UserFilterAccess{
			UserID:    userId,
			FilterID:  filter.ID,
			HasAccess: true,
		}

		err = tx.Where("user_id = ? AND filter_id = ?", userId, filter.ID).
			Assign(models.UserFilterAccess{HasAccess: true}).
			FirstOrCreate(&userFilterAccess).Error
		if err != nil {
			return err
		}
	}

	return nil
}
