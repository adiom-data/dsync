/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package cosmos

import (
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
)

// MultiNsLSNTracker is a thread-safe tracker for Last Sequence Numbers (LSN) across multiple namespaces.
type MultiNsLSNTracker struct {
	mu         sync.Mutex
	namespaces map[iface.Namespace]int64 // Stores the LSNs for each namespace.
}

// NewMultiNsLSNTracker creates a new LSNTracker instance.
func NewMultiNsLSNTracker() *MultiNsLSNTracker {
	return &MultiNsLSNTracker{
		namespaces: make(map[iface.Namespace]int64),
	}
}

// SetLSN sets the LSN for a specific namespace.
func (l *MultiNsLSNTracker) SetLSN(namespace iface.Namespace, lsn int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.namespaces[namespace] = lsn
}

// GetLSN gets the LSN for a specific namespace.
func (l *MultiNsLSNTracker) GetLSN(namespace iface.Namespace) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.namespaces[namespace]
}

// GetGlobalLSN returns the sum of all LSNs across all namespaces.
func (l *MultiNsLSNTracker) GetGlobalLSN() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	var globalLSN int64
	for _, lsn := range l.namespaces {
		globalLSN += lsn
	}
	return globalLSN
}

// IncrementLSN increments the LSN for a specific namespace.
func (l *MultiNsLSNTracker) IncrementLSN(namespace iface.Namespace) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.namespaces[namespace]++
}
