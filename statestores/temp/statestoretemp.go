/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package temp

import (
	"context"
	"fmt"
	"log/slog"
)

/**
 * Implements the iface.Statestore interface
 * Does exactly nothing - pure stub
 */

type StateStore struct {
}

func NewTempStateStore() *StateStore {
	return &StateStore{}
}

func (s *StateStore) Setup(ctx context.Context) error {
	slog.Debug("Setting up temp state store")
	return nil
}

func (s *StateStore) Teardown() {
}

func (s *StateStore) PersistObject(storeName string, id interface{}, obj interface{}) error {
	return nil
}

func (s *StateStore) RetrieveObject(storeName string, id interface{}, obj interface{}) error {
	return fmt.Errorf("not implemented")
}

func (s *StateStore) DeleteObject(storeName string, id interface{}) error {
	return nil
}
