/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package test

// TestDataStore is an interface for the underlying data store used for testing
// Used primarily for direct data manipulation in tests
type TestDataStore interface {
	Setup() error                                              // instantiate and connect to the datastore
	InsertDummy(db string, col string, data interface{}) error // insert a dummy record
	DeleteNamespace(db string, col string) error               // delete all records in a namespace
	Teardown() error                                           // clean up and disconnect
}
