package handlers

import (
	"context"
	"encoding/json"
	"log"
	"search-event-go/auth"
	"search-event-go/models"
	"search-event-go/redis"
	"search-event-go/services"
	"time"

	"github.com/gofiber/fiber/v2"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type AuthHandler struct {
	db          *gorm.DB
	authService *services.AuthService
}

type LoginHandler struct {
	db *gorm.DB
}

func NewLoginHandler(db *gorm.DB) *LoginHandler {
	return &LoginHandler{db: db}
}

func NewAuthHandler(db *gorm.DB) *AuthHandler {
	return &AuthHandler{
		db:          db,
		authService: services.NewAuthService(db),
	}
}

func (h *AuthHandler) Register(c *fiber.Ctx) error {
	var registerUserDto models.RegisterUserDto
	if err := c.BodyParser(&registerUserDto); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(registerUserDto.Password), bcrypt.DefaultCost)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to hash password"})
	}
	hashedPasswordStr := string(hashedPassword)

	tx := h.db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	status := models.StatusActive
	user := models.User{
		Email:        registerUserDto.Email,
		PasswordHash: &hashedPasswordStr,
		Name:         &registerUserDto.Name,
		Status:       &status,
	}

	err = tx.Create(&user).Error
	if err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	err = h.authService.GiveFilterAndApiAccess(tx, user.ID, Apis["SEARCH_EVENTS"].ID)
	if err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to grant access"})
	}

	// Generate token before commit
	token, err := auth.GenerateToken(user.ID, user.Email)
	if err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Register Token: %s", token)

	// Check for old token and revoke it BEFORE commit
	var oldToken string
	if redis.GetClient() != nil {
		userCacheKey := "user:" + user.ID
		cachedUser, err := redis.GetClient().Get(context.Background(), userCacheKey).Result()
		if err == nil && cachedUser != "" {
			var userData map[string]interface{}
			if json.Unmarshal([]byte(cachedUser), &userData) == nil {
				if tokenField, exists := userData["current_token"]; exists {
					oldToken = tokenField.(string)
				}
			}
		}
		// Revoke old token immediately
		if oldToken != "" && oldToken != token {
			redis.GetClient().Set(context.Background(), "token:"+oldToken, "revoked", time.Hour*24)
			log.Printf("Revoked old token: %s", oldToken)
		}
	}

	// Store new token in transaction
	now := time.Now()
	apiToken := models.APIToken{
		UserID:     user.ID,
		Token:      token,
		IsActive:   true,
		LastUsedAt: &now,
	}

	err = tx.Where("user_id = ?", user.ID).
		Assign(models.APIToken{Token: token, IsActive: true, LastUsedAt: &now}).
		FirstOrCreate(&apiToken).Error
	if err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to store token"})
	}

	err = tx.Commit().Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(models.AuthResponse{
		Success: true,
		Token:   token,
		Email:   user.Email,
	})
}

func (h *LoginHandler) Login(c *fiber.Ctx) error {
	var loginUserDto models.LoginUserDto
	if err := c.BodyParser(&loginUserDto); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	user := models.User{}
	err := h.db.Where("email = ?", loginUserDto.Email).First(&user).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid email or password"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	err = bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(loginUserDto.Password))
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid email or password"})
	}

	// Check user status
	if user.Status != nil && *user.Status == models.StatusInactive {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Account is inactive"})
	}

	token, err := auth.GenerateToken(user.ID, user.Email)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Login Token: %s", token)

	// Check for old token and revoke it immediately
	var oldToken string
	if redis.GetClient() != nil {
		userCacheKey := "user:" + user.ID
		cachedUser, err := redis.GetClient().Get(context.Background(), userCacheKey).Result()
		if err == nil && cachedUser != "" {
			var userData map[string]interface{}
			if json.Unmarshal([]byte(cachedUser), &userData) == nil {
				if tokenField, exists := userData["current_token"]; exists {
					oldToken = tokenField.(string)
				}
			}
		}
		// Revoke old token immediately in Redis
		if oldToken != "" && oldToken != token {
			redis.GetClient().Set(context.Background(), "token:"+oldToken, "revoked", time.Hour*24)
			log.Printf("Revoked old token in Redis: %s", oldToken)
		}
	}

	// Defensive: Mark old tokens inactive before creating new one, handles edge cases where multiple active tokens might exist
	h.db.Model(&models.APIToken{}).
		Where("user_id = ? AND is_active = ?", user.ID, true).
		Update("is_active", false)

	now := time.Now()
	apiToken := models.APIToken{
		UserID:     user.ID,
		Token:      token,
		IsActive:   true,
		LastUsedAt: &now,
	}

	err = h.db.Where("user_id = ?", user.ID).
		Assign(models.APIToken{Token: token, IsActive: true, LastUsedAt: &now}).
		FirstOrCreate(&apiToken).Error
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to store token"})
	}

	return c.JSON(models.AuthResponse{
		Success: true,
		Token:   token,
		Email:   user.Email,
	})
}
