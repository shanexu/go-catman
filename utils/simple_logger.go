package utils

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type severity int

// Severity levels.
const (
	sDebug severity = iota
	sInfo
	sWarn
	sError
	sFatal
)

// Severity tags.
const (
	tagDebug = "DEBUG: "
	tagInfo  = "INFO : "
	tagWarn  = "WARN : "
	tagError = "ERROR: "
	tagFatal = "FATAL: "
)

const (
	flags = log.Ldate | log.Lmicroseconds | log.Lshortfile
)

// A SimpleLogger represents an active logging object. Multiple loggers can be used
// simultaneously even if they are using the same same writers.
type SimpleLogger struct {
	debugLog *log.Logger
	infoLog  *log.Logger
	warnLog  *log.Logger
	errorLog *log.Logger
	fatalLog *log.Logger
	logLock  sync.Mutex
}

var _ Logger = (*SimpleLogger)(nil)

func NewSimpleLogger() *SimpleLogger {
	return &SimpleLogger{
		debugLog: log.New(os.Stderr, tagDebug, flags),
		infoLog:  log.New(os.Stderr, tagInfo, flags),
		warnLog:  log.New(os.Stderr, tagWarn, flags),
		errorLog: log.New(os.Stderr, tagError, flags),
		fatalLog: log.New(os.Stderr, tagFatal, flags),
	}
}

func (l *SimpleLogger) output(s severity, depth int, txt string) {
	l.logLock.Lock()
	defer l.logLock.Unlock()
	switch s {
	case sDebug:
		l.debugLog.Output(3+depth, txt)
	case sInfo:
		l.infoLog.Output(3+depth, txt)
	case sWarn:
		l.warnLog.Output(3+depth, txt)
	case sError:
		l.errorLog.Output(3+depth, txt)
	case sFatal:
		l.fatalLog.Output(3+depth, txt)
	default:
		panic(fmt.Sprintln("unrecognized severity:", s))
	}
}

// Close closes all the underlying log writers, which will flush any cached logs.
// Any errors from closing the underlying log writers will be printed to stderr.
// Once Close is called, all future calls to the logger will panic.
func (l *SimpleLogger) Close() {
	l.logLock.Lock()
	defer l.logLock.Unlock()
}

// Info logs with the Info severity.
// Arguments are handled in the manner of fmt.Print.
func (l *SimpleLogger) Debug(v ...interface{}) {
	l.output(sDebug, 0, fmt.Sprint(v...))
}

// Infof logs with the Info severity.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Debugf(format string, v ...interface{}) {
	l.output(sDebug, 0, fmt.Sprintf(format, v...))
}

// Info logs with the Info severity.
// Arguments are handled in the manner of fmt.Print.
func (l *SimpleLogger) Info(v ...interface{}) {
	l.output(sInfo, 0, fmt.Sprint(v...))
}

// Infof logs with the Info severity.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Infof(format string, v ...interface{}) {
	l.output(sInfo, 0, fmt.Sprintf(format, v...))
}

// Warn logs with the Warning severity.
// Arguments are handled in the manner of fmt.Print.
func (l *SimpleLogger) Warn(v ...interface{}) {
	l.output(sWarn, 0, fmt.Sprint(v...))
}

// Warnf logs with the Warning severity.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Warnf(format string, v ...interface{}) {
	l.output(sWarn, 0, fmt.Sprintf(format, v...))
}

// Error logs with the ERROR severity.
// Arguments are handled in the manner of fmt.Print.
func (l *SimpleLogger) Error(v ...interface{}) {
	l.output(sError, 0, fmt.Sprint(v...))
}

// Errorf logs with the Error severity.
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Errorf(format string, v ...interface{}) {
	l.output(sError, 0, fmt.Sprintf(format, v...))
}

// Fatal logs with the Fatal severity, and ends with os.Exit(1).
// Arguments are handled in the manner of fmt.Print.
func (l *SimpleLogger) Fatal(v ...interface{}) {
	l.output(sFatal, 0, fmt.Sprint(v...))
	l.Close()
	os.Exit(1)
}

// Fatalf logs with the Fatal severity, and ends with os.Exit(1).
// Arguments are handled in the manner of fmt.Printf.
func (l *SimpleLogger) Fatalf(format string, v ...interface{}) {
	l.output(sFatal, 0, fmt.Sprintf(format, v...))
	l.Close()
	os.Exit(1)
}
