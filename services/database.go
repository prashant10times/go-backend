package services

import (
	"fmt"
	"log"
	"search-event-go/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DatabaseService struct {
	DB *gorm.DB
}

func NewDatabaseService(cfg *config.Config) (*DatabaseService, error) {
	dsn := cfg.DirectDBURL
	if dsn == "" {
		return nil, fmt.Errorf("DIRECT_DB_URL not configured")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("SQL Database connection established successfully")

	return &DatabaseService{DB: db}, nil
}

func (d *DatabaseService) Close() error {
	sqlDB, err := d.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
