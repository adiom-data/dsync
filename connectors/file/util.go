/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package file

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// parseFileConnectionString extracts the file path from a file:// URI.
func parseFileConnectionString(raw string) (string, error) {
	const prefix = "file://"
	if !strings.HasPrefix(strings.ToLower(raw), prefix) {
		return "", fmt.Errorf("invalid connection URI %q: must start with 'file://' (e.g., file:///path/to/data)", raw)
	}
	path := raw[len(prefix):]
	if path == "" {
		return "", fmt.Errorf("missing path in connection URI %q: expected file:///path/to/dir or file:///path/to/file.csv", raw)
	}
	return path, nil
}

// pathToNamespace converts a file path to a dotted namespace.
// e.g., "/base/subdir/file.csv" with base "/base" becomes "subdir.file"
func pathToNamespace(basePath, filePath, extension string) string {
	relPath, err := filepath.Rel(basePath, filePath)
	if err != nil {
		relPath = filepath.Base(filePath)
	}
	relPath = strings.TrimSuffix(relPath, extension)
	relPath = strings.ReplaceAll(relPath, string(filepath.Separator), ".")
	return relPath
}

// namespaceToPath converts a dotted namespace back to a file path.
// e.g., "subdir.file" with base "/base" becomes "/base/subdir/file.csv"
func namespaceToPath(basePath, namespace, extension string) string {
	relPath := strings.ReplaceAll(namespace, ".", string(filepath.Separator))
	return filepath.Join(basePath, relPath+extension)
}

// countCSVRows counts CSV rows efficiently by scanning bytes and tracking quote state.
// This handles quoted fields containing newlines correctly without parsing field values.
func countCSVRows(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	inQuotes := false
	lastWasNewline := true

	for {
		n, err := r.Read(buf)
		if n > 0 {
			for i := 0; i < n; i++ {
				b := buf[i]
				switch {
				case b == '"':
					inQuotes = !inQuotes
					lastWasNewline = false
				case b == '\n' && !inQuotes:
					count++
					lastWasNewline = true
				case b == '\r' && !inQuotes:
					lastWasNewline = false
				default:
					lastWasNewline = false
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				if !lastWasNewline {
					count++
				}
				break
			}
			return 0, err
		}
	}

	return count, nil
}
