/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import "context"

type Statestore interface {
	// General
	Setup(ctx context.Context) error
	Teardown()

	// PersistObject persists an object into a store
	// Requires object id to be provided explicitly so we can overwrite existing objects
	PersistObject(storeName string, id interface{}, obj interface{}) error

	// RetrieveObject retrieves an object from a store by id
	RetrieveObject(storeName string, id interface{}, obj interface{}) error

	// DeleteObject deletes an object from a store by id
	DeleteObject(storeName string, id interface{}) error
}
