package middleware

import (
	"context"
	"encoding/json"
	"log"
	auth "search-event-go/auth"
	"search-event-go/config"
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

		// Load config to check for infinite validity users
		cfg := config.LoadConfig()
		claims, err := auth.ParseToken(token)
		if err != nil {
			log.Printf("ParseToken failed with error: %v - attempting ParseTokenWithoutExpiration", err)
			claimsWithoutExp, parseErr := auth.ParseTokenWithoutExpiration(token)
			if parseErr != nil {
				log.Printf("ParseTokenWithoutExpiration also failed: %v", parseErr)
				return NewAuthorizationError("Invalid or expired token", err.Error())
			}
			log.Printf("ParseTokenWithoutExpiration succeeded, claims: %v", claimsWithoutExp)

			if hasInfiniteTokenValidity(claimsWithoutExp.UserId, cfg.UnlimitedAccessUserIDs) {
				log.Printf("User %s has infinite token validity, allowing expired token", claimsWithoutExp.UserId)
				claims = claimsWithoutExp
			} else {
				return NewAuthorizationError("Invalid or expired token", err.Error())
			}
		} else {
			// Token is valid, log expiration info
			if claims.ExpiresAt != nil {
				log.Printf("Token is valid, expires at: %v", claims.ExpiresAt.Time)
			}
		}

		if claims.UserId == "" {
			return NewAuthorizationError("Invalid user ID in token")
		}

		cacheKey := "user:" + claims.UserId
		tokenCacheKey := "token:" + token

		var cachedUser string
		var user map[string]interface{}
		var fromCache bool

		// Check if user has infinite token validity (calculate once)
		hasInfiniteValidity := hasInfiniteTokenValidity(claims.UserId, cfg.UnlimitedAccessUserIDs)

		// Check token validity: First try Redis, fallback to database if Redis is down
		redisAvailable := redis.GetClient() != nil
		tokenValidInRedis := false

		if redisAvailable && !hasInfiniteValidity {
			tokenValid, err := redis.GetClient().Get(context.Background(), tokenCacheKey).Result()
			if err == nil && tokenValid == "revoked" {
				return NewAuthorizationError("Token has been revoked")
			}
			if err == nil && tokenValid == "valid" {
				tokenValidInRedis = true
				// Try to get user from cache
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

		// Fallback to database token validation
		// - If Redis is down: Always validate from database (for all users)
		// - If Redis is up but token not cached: Validate from database (except infinite validity users for performance)
		if !redisAvailable {
			// Redis is down, validate from database (all users including infinite validity)
			var apiToken models.APIToken
			result := db.Where("token = ? AND is_active = ?", token, true).First(&apiToken)
			if result.Error != nil {
				log.Printf("Token not found in database or inactive: %v", result.Error)
				return NewAuthorizationError("Token is not active or has been revoked")
			}
		} else if !tokenValidInRedis && !fromCache && !hasInfiniteValidity {
			// Redis is up but token not cached, validate from database (first request, skip infinite validity for performance)
			var apiToken models.APIToken
			result := db.Where("token = ? AND is_active = ?", token, true).First(&apiToken)
			if result.Error != nil {
				log.Printf("Token not found in database or inactive: %v", result.Error)
				return NewAuthorizationError("Token is not active or has been revoked")
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

func hasInfiniteTokenValidity(userId, infiniteValidityUserIDs string) bool {
	if infiniteValidityUserIDs == "" {
		return false
	}

	userIDs := strings.Split(infiniteValidityUserIDs, ",")
	for _, id := range userIDs {
		if strings.TrimSpace(id) == userId {
			return true
		}
	}
	return false
}
