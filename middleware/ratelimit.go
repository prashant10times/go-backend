package middleware

import (
	"context"
	"fmt"
	"search-event-go/redis"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
)

type RateLimitConfig struct {
	Limit         int
	Window        time.Duration
	BlockDuration time.Duration
	KeyPrefix     string
}

func RateLimitMiddleware(config RateLimitConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if redis.GetClient() == nil {
			return c.Next()
		}

		userID, ok := c.Locals("userId").(string)
		if !ok || userID == "" {
			userID = c.IP()
		}
		key := fmt.Sprintf("%s:user:%s", config.KeyPrefix, userID)
		blockKey := fmt.Sprintf("%s:block:%s", config.KeyPrefix, userID)

		blocked, err := redis.GetClient().Get(context.Background(), blockKey).Result()
		if err == nil && blocked != "" {
			ttl, _ := redis.GetClient().TTL(context.Background(), blockKey).Result()
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error":       "Rate limit exceeded",
				"retry_after": ttl.Seconds(),
			})
		}

		currentCount, err := redis.GetClient().Get(context.Background(), key).Result()
		if err != nil && err.Error() != "redis: nil" {
			fmt.Printf("Rate limit Redis error: %v\n", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Rate limiting service unavailable",
			})
		}

		count := 0
		if currentCount != "" {
			count, _ = strconv.Atoi(currentCount)
		}

		if count >= config.Limit {
			redis.GetClient().Set(context.Background(), blockKey, "blocked", config.BlockDuration)

			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error":       "Rate limit exceeded",
				"retry_after": config.BlockDuration.Seconds(),
			})
		}

		pipe := redis.GetClient().Pipeline()
		pipe.Incr(context.Background(), key)
		pipe.Expire(context.Background(), key, config.Window)
		_, err = pipe.Exec(context.Background())

		if err != nil {
			fmt.Printf("Rate limit increment error: %v\n", err)
		}

		return c.Next()
	}
}

func SearchRateLimit(limit int, window time.Duration, blockDuration time.Duration) fiber.Handler {
	return RateLimitMiddleware(RateLimitConfig{
		Limit:         limit,
		Window:        window,
		BlockDuration: blockDuration,
		KeyPrefix:     "search_rate_limit",
	})
}
