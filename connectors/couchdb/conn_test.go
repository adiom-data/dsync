/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package couchdb

import (
	"testing"
)

// Unit tests - always run

func TestConvertUriToDSN(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    string
		wantErr bool
	}{
		{
			name:    "couchdb scheme",
			uri:     "couchdb://localhost:5984",
			want:    "http://localhost:5984",
			wantErr: false,
		},
		{
			name:    "couchdbs scheme (TLS)",
			uri:     "couchdbs://localhost:5984",
			want:    "https://localhost:5984",
			wantErr: false,
		},
		{
			name:    "cloudant scheme",
			uri:     "cloudant://user:pass@account.cloudant.com",
			want:    "https://user:pass@account.cloudant.com",
			wantErr: false,
		},
		{
			name:    "http passthrough",
			uri:     "http://localhost:5984",
			want:    "http://localhost:5984",
			wantErr: false,
		},
		{
			name:    "https passthrough",
			uri:     "https://localhost:5984",
			want:    "https://localhost:5984",
			wantErr: false,
		},
		{
			name:    "unsupported scheme",
			uri:     "mongodb://localhost:27017",
			want:    "",
			wantErr: true,
		},
		{
			name:    "couchdb with auth",
			uri:     "couchdb://admin:secret@localhost:5984",
			want:    "http://admin:secret@localhost:5984",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertUriToDSN(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertUriToDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("convertUriToDSN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncodeCursor(t *testing.T) {
	tests := []struct {
		name     string
		startKey string
		endKey   string
		wantNil  bool
	}{
		{
			name:     "both empty",
			startKey: "",
			endKey:   "",
			wantNil:  true,
		},
		{
			name:     "only startKey",
			startKey: "doc1",
			endKey:   "",
			wantNil:  false,
		},
		{
			name:     "only endKey",
			startKey: "",
			endKey:   "doc2",
			wantNil:  false,
		},
		{
			name:     "both keys",
			startKey: "doc1",
			endKey:   "doc2",
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeCursor(tt.startKey, tt.endKey)
			if tt.wantNil && got != nil {
				t.Errorf("encodeCursor() = %v, want nil", got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("encodeCursor() = nil, want non-nil")
			}
		})
	}
}

func TestDecodeCursor(t *testing.T) {
	tests := []struct {
		name          string
		cursor        []byte
		wantStartKey  string
		wantEndKey    string
	}{
		{
			name:         "nil cursor",
			cursor:       nil,
			wantStartKey: "",
			wantEndKey:   "",
		},
		{
			name:         "empty cursor",
			cursor:       []byte{},
			wantStartKey: "",
			wantEndKey:   "",
		},
		{
			name:         "valid cursor with both keys",
			cursor:       []byte(`{"s":"doc1","e":"doc2"}`),
			wantStartKey: "doc1",
			wantEndKey:   "doc2",
		},
		{
			name:         "valid cursor with only startKey",
			cursor:       []byte(`{"s":"doc1","e":""}`),
			wantStartKey: "doc1",
			wantEndKey:   "",
		},
		{
			name:         "invalid JSON",
			cursor:       []byte(`invalid`),
			wantStartKey: "",
			wantEndKey:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := decodeCursor(tt.cursor)
			if gotStart != tt.wantStartKey {
				t.Errorf("decodeCursor() startKey = %v, want %v", gotStart, tt.wantStartKey)
			}
			if gotEnd != tt.wantEndKey {
				t.Errorf("decodeCursor() endKey = %v, want %v", gotEnd, tt.wantEndKey)
			}
		})
	}
}

func TestEncodeDecode_Roundtrip(t *testing.T) {
	tests := []struct {
		startKey string
		endKey   string
	}{
		{"", ""},
		{"doc1", ""},
		{"", "doc2"},
		{"doc1", "doc2"},
		{"special-chars!@#", "unicode-日本語"},
	}

	for _, tt := range tests {
		cursor := encodeCursor(tt.startKey, tt.endKey)
		gotStart, gotEnd := decodeCursor(cursor)

		if tt.startKey == "" && tt.endKey == "" {
			if cursor != nil {
				t.Errorf("expected nil cursor for empty keys")
			}
			continue
		}

		if gotStart != tt.startKey {
			t.Errorf("roundtrip startKey = %v, want %v", gotStart, tt.startKey)
		}
		if gotEnd != tt.endKey {
			t.Errorf("roundtrip endKey = %v, want %v", gotEnd, tt.endKey)
		}
	}
}

func TestGenerateConnectorID(t *testing.T) {
	id1 := generateConnectorID("couchdb://localhost:5984")
	id2 := generateConnectorID("couchdb://localhost:5984")
	id3 := generateConnectorID("couchdb://otherhost:5984")

	if id1 != id2 {
		t.Errorf("same connection string should produce same ID: %v != %v", id1, id2)
	}
	if id1 == id3 {
		t.Errorf("different connection strings should produce different IDs")
	}
	if id1 == "" {
		t.Errorf("connector ID should not be empty")
	}
}
