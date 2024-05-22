package log

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogrusLogger struct {
	*logrus.Entry
}

var ErrorLogger *LogrusLogger
var SlowQueryLogger *LogrusLogger

func (l *LogrusLogger) With(fields map[string]interface{}) *LogrusLogger {
	entry := l.WithFields(fields)
	return &LogrusLogger{entry}
}

func InitLoggers(logPath string, logLevel string, slowQueryLogPath string) error {
	level, err := getLogLevel(logLevel)
	if err != nil {
		return err
	}

	if logPath != "" {
		ErrorLogger = NewFileLogger(logPath, true, level)
	} else {
		ErrorLogger = NewConsoleLogger(true, level)
	}

	if slowQueryLogPath != "" {
		SlowQueryLogger = NewFileLogger(slowQueryLogPath, true, level)
	} else {
		SlowQueryLogger = NewConsoleLogger(true, level)
	}

	return nil
}

func NewConsoleLogger(formatJSON bool, level logrus.Level) *LogrusLogger {
	logger := logrus.New()
	logger.SetLevel(level)

	if formatJSON {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		formatter := &prefixed.TextFormatter{
			ForceColors:     true,
			ForceFormatting: true,
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05.00",
		}
		formatter.SetColorScheme(&prefixed.ColorScheme{
			TimestampStyle:  "cyan",
			PrefixStyle:     "blue",
			DebugLevelStyle: "magenta",
		})
		logger.SetFormatter(formatter)
	}

	entry := logrus.NewEntry(logger)
	return &LogrusLogger{entry}
}

func NewFileLogger(path string, formatJSON bool, level logrus.Level) *LogrusLogger {
	logger := logrus.New()
	logger.SetLevel(level)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			logger.Errorf("Failed to open log file: %s", err)
		} else {
			file.Close()
		}
	}

	lum := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     3,
	}
	logger.SetOutput(lum)

	if formatJSON {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		formatter := &prefixed.TextFormatter{
			ForceColors:     true,
			ForceFormatting: true,
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05.00",
		}
		formatter.SetColorScheme(&prefixed.ColorScheme{
			TimestampStyle:  "cyan",
			PrefixStyle:     "blue",
			DebugLevelStyle: "magenta",
		})
		logger.SetFormatter(formatter)
	}

	entry := logrus.NewEntry(logger)
	return &LogrusLogger{entry}
}

func getLogLevel(level string) (logrus.Level, error) {
	switch level {
	case "debug":
		return logrus.DebugLevel, nil
	case "info":
		return logrus.InfoLevel, nil
	case "warn":
		return logrus.WarnLevel, nil
	case "error":
		return logrus.ErrorLevel, nil
	default:
		return logrus.DebugLevel, fmt.Errorf("invaild error level: level=%s", level)
	}
}
