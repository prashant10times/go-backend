package redis

import (
	"os"

	"github.com/redis/go-redis/v9"
)

var Client *redis.Client

func InitRedis() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")

	Client = redis.NewClient(&redis.Options{
		Addr: redisHost + ":" + redisPort,
		DB:   0,
	})
}

func GetClient() *redis.Client {
	return Client
}

func Close() {
	Client.Close()
}
