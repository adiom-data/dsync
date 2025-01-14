/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/VictoriaMetrics/metrics"
	dsync "github.com/adiom-data/dsync/internal/app"
)

func main() {
	if err := metrics.InitPush("http://localhost:8428/api/v1/import/prometheus", 5*time.Second, `instance="dsync"`, true); err != nil {
		panic(err)
	}
	metrics.NewCounter("test_dsync").Inc()
	app := dsync.NewApp()
	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("dsync exited with error: %v\n", err)
	} else {
		fmt.Println("dsync exited successfully")
	}
}
