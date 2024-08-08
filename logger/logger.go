/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package logger

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/rivo/tview"
)

type Options struct {
	Verbosity string
	Logfile   string
}

func Setup(o Options, buffer bytes.Buffer) *os.File {
	// set up log level
	var level slog.Level
	switch o.Verbosity {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelDebug // default to DEBUG if verbosity is not recognized
	}

	var logger *slog.Logger

	if o.Logfile != "" {
		logFile, err := os.OpenFile(o.Logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}

		/*
			// File handler
			fileHandler := tint.NewHandler(logFile, &tint.Options{
				NoColor:   !isatty.IsTerminal(logFile.Fd()),
				Level:     level,
				AddSource: (level < 0), // only for debugging
			})

			logger = slog.New(NewErrorHandler(level, fileHandler, &buffer))
		*/
		logger = slog.New(
			slog.NewTextHandler(logFile, &slog.HandlerOptions{
				Level:     level,
				AddSource: (level < 0), // only for debugging
			}),
		)
		slog.SetDefault(logger)
		return logFile

	} else {
		fmt.Println("No log file specified, using stderr")
		w := os.Stderr
		logger = slog.New(
			tint.NewHandler(w, &tint.Options{
				NoColor:   !isatty.IsTerminal(w.Fd()),
				Level:     level,
				AddSource: (level < 0), //only for debugging
			}),
		)
		slog.SetDefault(logger)
	}
	return nil
}

func SetUpTviewLogger(o Options) *tview.TextView {
	errorLogs := tview.NewTextView().SetScrollable(true).SetDynamicColors(true).ScrollToEnd()
	//writer := &TextViewWriter{errorLogs}

	logger := slog.New(NewErrorHandler(slog.LevelInfo, nil, &bytes.Buffer{}))
	slog.SetDefault(logger)

	return errorLogs
}

type TextViewWriter struct {
	textView *tview.TextView
}

func (w *TextViewWriter) Write(p []byte) (n int, err error) {
	w.textView.Write(p)
	return len(p), nil
}

type ErrorHandler struct {
	level       slog.Level
	fileHandler slog.Handler
	buffer      *bytes.Buffer

	slog.Handler
}

func NewErrorHandler(level slog.Level, fileHandler slog.Handler, buffer *bytes.Buffer) *ErrorHandler {
	return &ErrorHandler{level: level, fileHandler: fileHandler, buffer: buffer}
}
func (h *ErrorHandler) Handle(ctx context.Context, record slog.Record) error {
	if err := h.fileHandler.Handle(ctx, record); err != nil {
		return err
	}

	if record.Level >= slog.LevelWarn {
		if _, err := h.buffer.Write([]byte(fmt.Sprintf("%s: %s\n", record.Level, record.Message))); err != nil {
			return err
		}
	}
	return nil
}

func (h *ErrorHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.level <= level
}
