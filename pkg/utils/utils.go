// Package utils provides reusable helpers that may be imported by other projects.
package utils

import "strconv"

// StringPtr returns a pointer to the string s.
func StringPtr(s string) *string {
	return &s
}

// IntPtr returns a pointer to the int i.
func IntPtr(i int) *int {
	return &i
}

// Int64Ptr returns a pointer to the int64 i.
func Int64Ptr(i int64) *int64 {
	return &i
}

// BoolPtr returns a pointer to the bool b.
func BoolPtr(b bool) *bool {
	return &b
}

// ParseInt64 parses s as int64. Returns 0 and false on failure.
func ParseInt64(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

// ParseInt parses s as int. Returns 0 and false on failure.
func ParseInt(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// CoalesceString returns the first non-empty string, or "" if all are empty.
func CoalesceString(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
