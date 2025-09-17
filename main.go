/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package main

import (
	"fmt"
	"os"

	dsync "github.com/adiom-data/dsync/internal/app"
	"github.com/adiom-data/dsync/metrics"
)

func main() {
	defer metrics.Done()
	app := dsync.NewApp()
	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("dsync exited with error: %v\n", err)
	} else {
		fmt.Println("dsync exited successfully")
	}
}
