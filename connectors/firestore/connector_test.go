/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package firestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFirestoreURI(t *testing.T) {
	tests := []struct {
		name           string
		uri            string
		wantProjectID  string
		wantDatabaseID string
		wantErr        error
	}{
		{
			name:           "basic project ID",
			uri:            "firestore://my-project",
			wantProjectID:  "my-project",
			wantDatabaseID: "(default)",
			wantErr:        nil,
		},
		{
			name:           "project ID with database",
			uri:            "firestore://my-project/my-database",
			wantProjectID:  "my-project",
			wantDatabaseID: "my-database",
			wantErr:        nil,
		},
		{
			name:           "uppercase prefix",
			uri:            "FIRESTORE://my-project",
			wantProjectID:  "my-project",
			wantDatabaseID: "(default)",
			wantErr:        nil,
		},
		{
			name:           "mixed case prefix",
			uri:            "Firestore://my-project/db",
			wantProjectID:  "my-project",
			wantDatabaseID: "db",
			wantErr:        nil,
		},
		{
			name:    "invalid prefix",
			uri:     "mongodb://my-project",
			wantErr: ErrInvalidURI,
		},
		{
			name:    "missing project ID",
			uri:     "firestore://",
			wantErr: ErrProjectIDRequired,
		},
		{
			name:    "empty string",
			uri:     "",
			wantErr: ErrInvalidURI,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectID, databaseID, err := parseFirestoreURI(tt.uri)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantProjectID, projectID)
			assert.Equal(t, tt.wantDatabaseID, databaseID)
		})
	}
}

func TestNamespaceToCollection(t *testing.T) {
	tests := []struct {
		namespace string
		want      string
	}{
		{"mydb.users", "mydb_users"},
		{"db.collection", "db_collection"},
		{"simple", "simple"},
		{"a.b.c", "a_b_c"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			got := namespaceToCollection(tt.namespace)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValueToString(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"int32", int32(42), "42"},
		{"int64", int64(42), "42"},
		{"uint", uint(42), "42"},
		{"float64", 3.14, "3.14"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := valueToString(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
