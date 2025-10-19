package util

import (
	"os"
	"strconv"
	"strings"
)

// GetEnvOrDefault retrieves the value of the environment variable named by the key.
// It returns the default value if the variable is not set.
func GetEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// GetEnvInt retrieves an integer value from an environment variable.
// It returns the default value if the variable is not set or parsing fails.
func GetEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

// GetEnvFloat retrieves a float64 value from an environment variable.
// It returns the default value if the variable is not set or parsing fails.
func GetEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// EscapeJSONPointer escapes characters in a string according to JSON Pointer syntax (RFC 6901).
// Specifically, it replaces '~' with '~0' and '/' with '~1'.
func EscapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}
