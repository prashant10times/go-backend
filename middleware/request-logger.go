package middleware

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

func RequestLoggerMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		var requestBody []byte
		if c.Body() != nil {
			requestBody = c.Body()
		}

		c.Request().SetBody(requestBody)

		headers := make([]string, 0)
		c.Request().Header.VisitAll(func(key, value []byte) {
			headers = append(headers, fmt.Sprintf("%s: %s", string(key), string(value)))
		})

		queryParams := c.Request().URI().QueryString()
		queryStr := ""
		if len(queryParams) > 0 {
			queryStr = fmt.Sprintf("?%s", string(queryParams))
		}

		remoteIP := c.IP()
		userAgent := c.Get("User-Agent", "N/A")
		contentType := c.Get("Content-Type", "N/A")
		contentLength := len(requestBody)

		bodyPreview := ""
		if len(requestBody) > 0 {
			bodyStr := string(requestBody)
			maxBodyLength := 500
			if len(bodyStr) > maxBodyLength {
				bodyPreview = bodyStr[:maxBodyLength] + "... (truncated)"
			} else {
				bodyPreview = bodyStr
			}
		} else {
			bodyPreview = "(empty)"
		}

		log.Printf("REQUEST: %s %s%s", c.Method(), c.Path(), queryStr)
		log.Printf("Remote IP: %s | User-Agent: %s", remoteIP, userAgent)
		log.Printf("Content-Type: %s | Content-Length: %d bytes", contentType, contentLength)

		if len(headers) > 0 {
			log.Printf("Headers:")
			for _, header := range headers {
				log.Printf("  %s", header)
			}
		}

		log.Printf("Request Body: %s", bodyPreview)

		err := c.Next()

		statusCode := c.Response().StatusCode()
		responseSize := len(c.Response().Body())
		log.Printf("Status: %d", statusCode)
		log.Printf("Response Size: %d bytes", responseSize)

		if err != nil {
			log.Printf("Error occurred: %v", err)
		}
		return err
	}
}

func SimpleRequestLogger() fiber.Handler {
	return func(c *fiber.Ctx) error {
		err := c.Next()

		statusCode := c.Response().StatusCode()

		log.Printf("[%s] %s %s -> %d | IP: %s",
			time.Now().Format("15:04:05"),
			c.Method(),
			c.OriginalURL(),
			statusCode,
			c.IP())

		return err
	}
}

func FormatHeaders(c *fiber.Ctx) string {
	var builder strings.Builder
	c.Request().Header.VisitAll(func(key, value []byte) {
		builder.WriteString(fmt.Sprintf("\n  %s: %s", string(key), string(value)))
	})
	return builder.String()
}

func ReadRequestBody(c *fiber.Ctx) ([]byte, error) {
	body := c.Body()
	if body == nil {
		return []byte{}, nil
	}

	reader := bytes.NewReader(body)

	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	c.Request().SetBody(bodyBytes)

	return bodyBytes, nil
}
