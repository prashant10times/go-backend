package middleware

import (
	"context"
	"encoding/json"
	"log"
	auth "search-event-go/auth"
	"search-event-go/models"
	"search-event-go/redis"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func JwtAuthMiddleware(db *gorm.DB) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Check if db is nil
		if db == nil {
			return NewAuthorizationError("Database connection not available")
		}

		authHeader := c.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			return NewAuthorizationError("Invalid or missing Authorization header")
		}
		token := strings.TrimPrefix(authHeader, "Bearer ")

		claims, err := auth.ParseToken(token)
		if err != nil {
			return NewAuthorizationError("Invalid or expired token", err.Error())
		}

		if claims.UserId == "" {
			return NewAuthorizationError("Invalid user ID in token")
		}

		cacheKey := "user:" + claims.UserId
		tokenCacheKey := "token:" + token

		var cachedUser string
		var user map[string]interface{}
		var fromCache bool

		// Check if this specific token is explicitly revoked
		if redis.GetClient() != nil {
			tokenValid, err := redis.GetClient().Get(context.Background(), tokenCacheKey).Result()
			if err == nil && tokenValid == "revoked" {
				return NewAuthorizationError("Token has been revoked")
			}

			// If token is marked as valid, try to get user from cache
			if err == nil && tokenValid == "valid" {
				cachedUser, err = redis.GetClient().Get(context.Background(), cacheKey).Result()
				if err == nil && cachedUser != "" {
					user = make(map[string]interface{})
					if err := json.Unmarshal([]byte(cachedUser), &user); err != nil {
						log.Printf("Failed to unmarshal cached user data: %v", err)
						fromCache = false
					} else {
						fromCache = true
					}
				}
			}
		}

		// If not in cache, get from database and cache it
		if !fromCache {
			log.Printf("Querying database for user ID: %s", claims.UserId)
			var dbUser models.User
			result := db.Where("id = ?", claims.UserId).First(&dbUser)

			if result.Error != nil {
				log.Printf("ERROR: Database query failed: %v", result.Error)
				return NewAuthorizationError("User not found", result.Error.Error())
			}
			if result.RowsAffected == 0 {
				log.Printf("ERROR: No user found with ID: %s", claims.UserId)
				return NewAuthorizationError("User not found", result.Error.Error())
			}

			log.Printf("DEBUG: User found in database: %s", dbUser.Email)

			if dbUser.Status != nil && *dbUser.Status == models.StatusInactive {
				log.Printf("ERROR: User is inactive: %s", claims.UserId)
				return NewAuthorizationError("User is inactive")
			}

			user = make(map[string]interface{})
			user["id"] = dbUser.ID
			user["email"] = dbUser.Email
			user["name"] = dbUser.Name
			user["status"] = dbUser.Status
			user["current_token"] = token

			if redis.GetClient() != nil {
				log.Printf("Caching user data: %v", user)
				go func() {
					jsonData, _ := json.Marshal(user)
					_, err := redis.GetClient().Set(context.Background(), cacheKey, string(jsonData), time.Hour*24).Result()
					if err != nil {
						log.Printf("Error caching user data: %v", err)
					}
					_, err = redis.GetClient().Set(context.Background(), tokenCacheKey, "valid", time.Hour*24).Result()
					if err != nil {
						log.Printf("Error caching token validity: %v", err)
					}
				}()
			}
		}

		if user == nil {
			log.Printf("ERROR: User map is nil after processing")
			return NewAuthorizationError("Failed to load user data")
		}

		c.Locals("user", user)
		c.Locals("userId", claims.UserId)
		return c.Next()
	}
}
