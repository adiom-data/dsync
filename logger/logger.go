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
	"io"
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
	"github.com/rivo/tview"
	slogmulti "github.com/samber/slog-multi"
)

type Options struct {
	Verbosity  string   //log verbosity
	Logfile    *os.File //log file handle. When not provided, logs are written to stderr
	FormatJSON bool     // false = human readable, true = json

	ErrorView io.Writer //error view for UI
}

func Setup(o Options) {
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

	var slogHandler slog.Handler

	if o.Logfile == nil { // just log to stderr
		if o.FormatJSON {
			slogHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level:     level,
				AddSource: (level < 0), // only for debugging
			})
		} else {
			slogHandler = tint.NewHandler(os.Stderr, &tint.Options{
				NoColor:   false, // colorize output
				Level:     level,
				AddSource: (level < 0), //only for debugging
			})
		}
	} else { // log to file and potentially send errors to the UI
		if o.FormatJSON {
			slogHandler = slog.NewTextHandler(o.Logfile, &slog.HandlerOptions{
				Level:     level,
				AddSource: (level < 0), // only for debugging
			})
		} else {
			slogHandler = tint.NewHandler(o.Logfile, &tint.Options{
				NoColor:   true, // no colors
				Level:     level,
				AddSource: (level < 0), //only for debugging
			})
		}

		if o.ErrorView != nil {
			// create a dedicated error handler and fanout
			slogHandlerEV := tint.NewHandler(o.ErrorView, &tint.Options{
				NoColor: true,           // no colors
				Level:   slog.LevelWarn, //only warnings and errors
			})
			slogHandler = slogmulti.Fanout(slogHandler, slogHandlerEV)
		}
	}

	logger := slog.New(slogHandler)
	slog.SetDefault(logger)
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
