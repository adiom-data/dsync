package util

import (
	"fmt"
	"strings"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
)

// MapNamespace looks up the map or tries to look up a prefix in the map
func MapNamespace(m map[string]string, namespace string, keySep string, valSep string) string {
	if res, ok := m[namespace]; ok {
		return res
	}
	if left, right, ok := strings.Cut(namespace, keySep); ok {
		if res, ok := m[left]; ok {
			return res + valSep + right
		}
	}
	return namespace
}

// Mapify is convenience for making a map. both sides expected to be the same size
func Mapify(keys []string, vals []string) map[string]string {
	if len(keys) != len(vals) {
		return nil
	}
	m := map[string]string{}
	for i, k := range keys {
		m[k] = vals[i]
	}
	return m
}

// NamespaceSplit splits colon separated namespace mappings into a left and right
func NamespaceSplit(namespaces []string, sep string) ([]string, []string) {
	var left []string
	var right []string
	for _, namespace := range namespaces {
		if l, r, ok := strings.Cut(namespace, sep); ok {
			left = append(left, l)
			right = append(right, r)
		} else {
			left = append(left, namespace)
			right = append(right, namespace)
		}
	}
	return left, right
}

// ValidateNamespaces checks provided fully qualified namespaces against provided capabilities
func ValidateNamespaces(namespaces []string, cap *adiomv1.Capabilities) error {
	if cap.GetSource() == nil {
		return fmt.Errorf("not a source")
	}
	if len(namespaces) == 0 && !cap.GetSource().GetDefaultPlan() {
		return fmt.Errorf("default plan not allowed")
	}
	if len(namespaces) > 1 && !cap.GetSource().GetMultiNamespacePlan() {
		return fmt.Errorf("multiple namespaces not supported")
	}
	return nil
}

func ValidateCompatibility(src *adiomv1.Capabilities, dst *adiomv1.Capabilities, transforms []*adiomv1.GetTransformInfoResponse_TransformInfo) (adiomv1.DataType, adiomv1.DataType, error) {
	if src.GetSource() == nil {
		return adiomv1.DataType_DATA_TYPE_UNKNOWN, adiomv1.DataType_DATA_TYPE_UNKNOWN, fmt.Errorf("provided source is not a source")
	}
	if dst.GetSink() == nil {
		return adiomv1.DataType_DATA_TYPE_UNKNOWN, adiomv1.DataType_DATA_TYPE_UNKNOWN, fmt.Errorf("provided destination is not a destination")
	}
	srcTypes := src.GetSource().GetSupportedDataTypes()
	dstTypes := dst.GetSink().GetSupportedDataTypes()

	if transforms != nil {
		// could use maps but these are super small
		for _, t := range srcTypes {
			for _, transform := range transforms {
				if transform.GetRequestType() == t {
					for _, t2 := range transform.GetResponseTypes() {
						for _, t3 := range dstTypes {
							if t2 == t3 {
								return t, t2, nil
							}
						}
					}
				}
			}
		}
		return adiomv1.DataType_DATA_TYPE_UNKNOWN, adiomv1.DataType_DATA_TYPE_UNKNOWN, fmt.Errorf("no compatible data path with transform")
	}
	for _, t := range srcTypes {
		for _, t2 := range dstTypes {
			if t == t2 {
				return t, t2, nil
			}
		}
	}
	return adiomv1.DataType_DATA_TYPE_UNKNOWN, adiomv1.DataType_DATA_TYPE_UNKNOWN, fmt.Errorf("no compatible data path")
}
