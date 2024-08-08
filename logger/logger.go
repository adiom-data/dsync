/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package logger

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

type Options struct {
	Verbosity string
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

	w := os.Stderr
	logger := slog.New(
		tint.NewHandler(w, &tint.Options{
			NoColor:   !isatty.IsTerminal(w.Fd()),
			Level:     level,
			AddSource: level < 0, //only for debugging
		}),
	)

	slog.SetDefault(logger)
}
