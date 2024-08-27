// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package statestores

import (
	"context"
	"fmt"
	"log/slog"
)

/**
 * Implements the iface.Statestore interface
 * Does exactly nothing - pure stub
 */

type TempStateStore struct {
}

func NewTempStateStore() *TempStateStore {
	return &TempStateStore{}
}

func (s *TempStateStore) Setup(ctx context.Context) error {
	slog.Debug("Setting up temp state store")
	return nil
}

func (s *TempStateStore) Teardown() {
}

func (s *TempStateStore) PersistObject(storeName string, id interface{}, obj interface{}) error {
	return nil
}

func (s *TempStateStore) RetrieveObject(storeName string, id interface{}, obj interface{}) error {
	return fmt.Errorf("not implemented")
}

func (s *TempStateStore) DeleteObject(storeName string, id interface{}) error {
	return nil
}
