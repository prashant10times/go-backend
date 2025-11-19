package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                   string
	DirectDBURL            string
	DBURL                  string
	RedisHost              string
	RedisPort              string
	JWTSecret              string
	JWTExpiresIn           string
	ElasticSearchHost      string
	SwaggerSecret          string
	IndexName              string
	ThrottleLimit          string
	ThrottleTTL            string
	ThrottleBlockDuration  string
	ServerHost             string
	TestingIndex           string
	ClickhouseURL          string
	Origin                 string
	AllowMethods           string
	AlertId                string
	UnlimitedAccessUserIDs string // Comma-separated list of user IDs with unlimited access to all filters and parameters
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	config := &Config{
		Port:                   getEnv("PORT", "2000"),
		DirectDBURL:            getEnv("DIRECT_DB_URL", ""),
		DBURL:                  getEnv("DB_URL", ""),
		RedisHost:              getEnv("REDIS_HOST", "localhost"),
		RedisPort:              getEnv("REDIS_PORT", "6379"),
		JWTSecret:              getEnv("JWT_SECRET", ""),
		JWTExpiresIn:           getEnv("JWT_EXPIRES_IN", "1d"),
		ElasticSearchHost:      getEnv("ELASTIC_SEARCH_HOST", ""),
		SwaggerSecret:          getEnv("SWAGGER_SECRET", ""),
		IndexName:              getEnv("INDEX_NAME", ""),
		ThrottleLimit:          getEnv("THROTTLE_LIMIT", "100"),
		ThrottleTTL:            getEnv("THROTTLE_TTL", "30000"),
		ThrottleBlockDuration:  getEnv("THROTTLE_BLOCK_DURATION", "60000"),
		ServerHost:             getEnv("SERVER_HOST", ""),
		TestingIndex:           getEnv("TESTING_INDEX", ""),
		ClickhouseURL:          getEnv("CLICKHOUSE_URL", ""),
		Origin:                 getEnv("ORIGIN", ""),
		AllowMethods:           getEnv("ALLOWED_METHODS", ""),
		AlertId:                getEnv("ALERT_ID", ""),
		UnlimitedAccessUserIDs: getEnv("UNLIMITED_ACCESS_USER_IDS", ""),
	}
	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
