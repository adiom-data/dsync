/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

var (
	// System databases that we don't want to copy
	ExcludedDBListForIC = []string{"local", "config", "admin", "adiom-internal", dummyDB}
	// System collections that we don't want to copy (regex pattern)
	ExcludedSystemCollPattern = "^system[.]"

	//XXX (AK, 6/2024): these need to use negative lookahead to make change stream work with shared tier Mongo (although it seems they rewrite the whole thing in a funny way that doesn't work)
	//XXX (AK, 6/2024): dummyDB business is not excluded as a hack to get the resume token from the change stream
	ExcludedDBPatternCS         = `^(?!local$|config$|admin$|adiom-internal$)`
	ExcludedSystemCollPatternCS = `^(?!system.)`
)
