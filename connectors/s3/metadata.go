package s3

import (
	"path"
	"strings"
)

// MetadataKey returns the S3 key for the metadata file for a given namespace.
func MetadataKey(prefix, namespace string) string {
	nsPath := strings.ReplaceAll(strings.Trim(namespace, "/"), ".", "/")
	if nsPath == "" {
		nsPath = "default"
	}

	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return path.Join(nsPath, ".metadata.json")
	}
	return path.Join(prefix, nsPath, ".metadata.json")
}
