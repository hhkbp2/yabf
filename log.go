package yabf

import (
	"fmt"
	"io"
	"os"
)

type LogLevelType uint8

const (
	LevelVerbose LogLevelType = 50
	LevelDebug   LogLevelType = 40
	LevelInfo    LogLevelType = 30
	LevelWarn    LogLevelType = 20
	LevelError   LogLevelType = 10
	LevelQuiet   LogLevelType = 0
)

var (
	nameToLevels = map[string]LogLevelType{
		"verbose": LevelVerbose,
		"debug":   LevelDebug,
		"info":    LevelInfo,
		"warn":    LevelWarn,
		"error":   LevelError,
		"quiet":   LevelQuiet,
	}
)

var (
	logLevel LogLevelType = LevelQuiet
)

func Flogf(w io.Writer, level LogLevelType, format string, args ...interface{}) {
	if level <= logLevel {
		fmt.Fprintf(w, format, args...)
		fmt.Fprintln(w, "")
	}
}

func Logf(level LogLevelType, format string, args ...interface{}) {
	Flogf(os.Stdout, level, format, args...)
}

func Errorf(format string, args ...interface{}) {
	Logf(LevelError, format, args...)
}

func Warnf(format string, args ...interface{}) {
	Logf(LevelWarn, format, args...)
}

func Infof(format string, args ...interface{}) {
	Logf(LevelInfo, format, args...)
}

func Debugf(format string, args ...interface{}) {
	Logf(LevelDebug, format, args...)
}

func Verbosef(format string, args ...interface{}) {
	Logf(LevelVerbose, format, args...)
}

func PromptPrintf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func Printf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println("")
}

func EPrintf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr, "")
}
