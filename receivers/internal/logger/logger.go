package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New builds a JSON slog logger writing to stdout.
// Level comes from UNIFLOW_LOG_LEVEL (DEBUG/INFO/WARN/ERROR),
// defaulting to INFO on empty or unrecognized values.
func New(component string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("UNIFLOW_LOG_LEVEL"))) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("component", component)
}
