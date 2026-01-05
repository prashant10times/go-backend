package middleware

import (
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
)

func RequestLoggerMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/metrics" {
			return c.Next()
		}
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

		// Debug: Print IP-related values
		xForwardedFor := c.Get("X-Forwarded-For", "N/A")
		xRealIP := c.Get("X-Real-IP", "N/A")
		remoteAddr := c.Context().RemoteAddr().String()
		remoteIP := c.IP()
		log.Printf("IP Debug - X-Forwarded-For: %s | X-Real-IP: %s | RemoteAddr: %s | Final IP: %s", xForwardedFor, xRealIP, remoteAddr, remoteIP)

		userAgent := c.Get("User-Agent", "N/A")
		contentType := c.Get("Content-Type", "N/A")
		contentLength := len(requestBody)

		bodyPreview := ""
		if len(requestBody) > 0 {
			bodyStr := string(requestBody)
			bodyPreview = bodyStr
		} else {
			bodyPreview = ""
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
