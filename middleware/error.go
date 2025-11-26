package middleware

import (
	"errors"
	"log"
	"net"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

type ErrorType string

const (
	ValidationError         ErrorType = "VALIDATION_ERROR"
	AuthorizationError      ErrorType = "AUTHORIZATION_ERROR"
	NotFoundError           ErrorType = "NOT_FOUND"
	InternalServerError     ErrorType = "INTERNAL_SERVER_ERROR"
	BadRequestError         ErrorType = "BAD_REQUEST"
	ForbiddenError          ErrorType = "FORBIDDEN"
	TooManyRequestsError    ErrorType = "TOO_MANY_REQUESTS"
	ServiceUnavailableError ErrorType = "SERVICE_UNAVAILABLE"
)

type CustomError struct {
	Type       ErrorType `json:"type"`
	Message    string    `json:"message"`
	StatusCode int       `json:"status_code"`
	Details    string    `json:"details,omitempty"`
}

func (e *CustomError) Error() string {
	return e.Message
}

func NewCustomError(errorType ErrorType, message string, statusCode int, details ...string) *CustomError {
	err := &CustomError{
		Type:       errorType,
		Message:    message,
		StatusCode: statusCode,
	}
	if len(details) > 0 {
		err.Details = details[0]
	}
	return err
}

func NewValidationError(message string, details ...string) *CustomError {
	return NewCustomError(ValidationError, message, fiber.StatusBadRequest, details...)
}

func NewAuthorizationError(message string, details ...string) *CustomError {
	return NewCustomError(AuthorizationError, message, fiber.StatusUnauthorized, details...)
}

func NewNotFoundError(message string, details ...string) *CustomError {
	return NewCustomError(NotFoundError, message, fiber.StatusNotFound, details...)
}

func NewInternalServerError(message string, details ...string) *CustomError {
	return NewCustomError(InternalServerError, message, fiber.StatusInternalServerError, details...)
}

func NewBadRequestError(message string, details ...string) *CustomError {
	return NewCustomError(BadRequestError, message, fiber.StatusBadRequest, details...)
}

func NewForbiddenError(message string, details ...string) *CustomError {
	return NewCustomError(ForbiddenError, message, fiber.StatusForbidden, details...)
}

func NewTooManyRequestsError(message string, details ...string) *CustomError {
	return NewCustomError(TooManyRequestsError, message, fiber.StatusTooManyRequests, details...)
}

func NewServiceUnavailableError(message string, details ...string) *CustomError {
	return NewCustomError(ServiceUnavailableError, message, fiber.StatusServiceUnavailable, details...)
}

func isDatabaseConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	errLower := strings.ToLower(errStr)

	if errors.Is(err, net.ErrClosed) || strings.Contains(errLower, "eof") {
		return true
	}

	if strings.Contains(errLower, "connection refused") ||
		strings.Contains(errLower, "connection reset") ||
		strings.Contains(errLower, "broken pipe") ||
		strings.Contains(errLower, "no connection") {
		return true
	}

	if strings.Contains(errLower, "timeout") ||
		strings.Contains(errLower, "deadline exceeded") {
		return true
	}

	if strings.Contains(errLower, "no route to host") ||
		strings.Contains(errLower, "network is unreachable") {
		return true
	}

	if strings.Contains(errLower, "no such host") ||
		strings.Contains(errLower, "name resolution") {
		return true
	}

	if strings.Contains(errLower, "clickhouse") && (strings.Contains(errLower, "connection") ||
		strings.Contains(errLower, "server") ||
		strings.Contains(errLower, "unavailable")) {
		return true
	}

	return false
}

func formatDatabaseError(err error) (string, string) {
	if isDatabaseConnectionError(err) {
		return "Database server connection error: The database server is currently unavailable or unreachable. Please try again later.",
			"DB_CONNECTION_ERROR: " + err.Error()
	}
	return "Database query error: An error occurred while executing the database query.",
		err.Error()
}

type ErrorResponse struct {
	StatusCode int                    `json:"statusCode"`
	Message    string                 `json:"message"`
	Data       map[string]interface{} `json:"data"`
	Meta       map[string]interface{} `json:"meta"`
}

func GlobalErrorHandler(c *fiber.Ctx, err error) error {
	statusCode := fiber.StatusInternalServerError
	errorType := string(InternalServerError)
	message := "An unexpected error occurred"
	details := err.Error()

	if customErr, ok := err.(*CustomError); ok {
		statusCode = customErr.StatusCode
		errorType = string(customErr.Type)
		message = customErr.Message
		if customErr.Details != "" {
			details = customErr.Details
			if isDatabaseConnectionError(errors.New(customErr.Details)) {
				statusCode = fiber.StatusServiceUnavailable
				errorType = string(ServiceUnavailableError)
				userMessage, detailedError := formatDatabaseError(errors.New(customErr.Details))
				message = userMessage
				details = detailedError
				log.Printf("Database connection error detected in CustomError details: %v", customErr.Details)
			}
		}
	} else if fiberErr, ok := err.(*fiber.Error); ok {
		statusCode = fiberErr.Code
		message = fiberErr.Message

		switch statusCode {
		case fiber.StatusBadRequest:
			errorType = string(BadRequestError)
		case fiber.StatusUnauthorized:
			errorType = string(AuthorizationError)
		case fiber.StatusForbidden:
			errorType = string(ForbiddenError)
		case fiber.StatusNotFound:
			errorType = string(NotFoundError)
		case fiber.StatusTooManyRequests:
			errorType = string(TooManyRequestsError)
		case fiber.StatusServiceUnavailable:
			errorType = string(ServiceUnavailableError)
		default:
			errorType = string(InternalServerError)
		}
	} else {
		if isDatabaseConnectionError(err) {
			statusCode = fiber.StatusServiceUnavailable
			errorType = string(ServiceUnavailableError)
			userMessage, detailedError := formatDatabaseError(err)
			message = userMessage
			details = detailedError
			log.Printf("Database connection error detected: %v", err)
		}
	}

	if statusCode >= 500 {
		log.Printf("Error [%d]: %s - Details: %s", statusCode, message, details)
	}

	errorResponse := ErrorResponse{
		StatusCode: statusCode,
		Message:    errorType,
		Data: map[string]interface{}{
			"message": message,
			"error":   details,
		},
		Meta: map[string]interface{}{
			"timestamp": time.Now().Unix(),
		},
	}

	c.Status(statusCode)
	return c.JSON(errorResponse)
}

func RecoverMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				var err error
				switch x := r.(type) {
				case string:
					err = NewInternalServerError("Panic recovered", x)
				case error:
					err = NewInternalServerError("Panic recovered", x.Error())
				default:
					err = NewInternalServerError("Panic recovered", "Unknown panic")
				}

				log.Printf("Panic recovered: %v", r)
				GlobalErrorHandler(c, err)
			}
		}()

		return c.Next()
	}
}
