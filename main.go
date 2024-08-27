// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	dsync "github.com/adiom-data/dsync/internal/app"
)

func main() {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGPIPE)
	cancellableCtx, cancelApp := context.WithCancel(context.Background())

	go func() {
		for s := range sigChan {
			if s != syscall.SIGPIPE {
				cancelApp()
				break
			}
		}
	}()

	app := dsync.NewApp()
	err := app.RunContext(cancellableCtx, os.Args)
	if err != nil {
		fmt.Printf("dsync exited with error: %v\n", err)
	} else {
		fmt.Println("dsync exited successfully")
	}
}
