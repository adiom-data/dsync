/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
)

func shouldIgnoreChangeStreamEvent(change bson.M) bool {
	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)

	//We need to filter out the dummy collection
	//TODO (AK, 6/2024): is it the best way to do it?
	if (db == dummyDB) && (col == dummyCol) {
		return true
	}

	return false
}
