// Package ocitags validates OCI free-form and defined tags against the documented limits and tag definitions.
package ocitags

import (
	"encoding/json/v2"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// Limits from https://docs.oracle.com/en-us/iaas/Content/Tagging/Concepts/taggingoverview.htm.
const (
	MaxFreeform  = 10
	MaxDefined   = 64
	MaxKeyLen    = 100
	MaxValueLen  = 256
	MaxTotalSize = 5 * 1024
)

// Def is what a tag definition contributes to validation.
type Def struct {
	EnumValues []string
	HasEnum    bool
	Retired    bool
}

// Lookup resolves a namespace and key (case-insensitive) to its definition; found is false when either is unknown.
type Lookup func(namespace, key string) (def Def, found bool)

// Error carries the OCI status and code for a rejected tag set.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func invalid(format string, a ...any) *Error {
	return &Error{Status: http.StatusBadRequest, Code: "InvalidParameter", Message: fmt.Sprintf(format, a...)}
}

func validKey(k string) bool {
	if k == "" || utf8.RuneCountInString(k) > MaxKeyLen {
		return false
	}

	for _, r := range k {
		if r < '!' || r > '~' || r == '.' {
			return false
		}
	}

	return true
}

// Validate checks tag counts, key and value shapes, total size and, when lookup is not nil, that every defined
// tag names an existing, active namespace and key whose ENUM validator accepts the value.
func Validate(free map[string]string, defined map[string]map[string]any, lookup Lookup) *Error {
	if err := checkFree(free); err != nil {
		return err
	}

	if err := checkDefined(defined, lookup); err != nil {
		return err
	}

	raw, _ := json.Marshal([]any{free, defined}, jsonx.V1())
	if len(raw) > MaxTotalSize {
		return invalid("The total tag data for a resource is limited to 5 K")
	}

	return nil
}

func checkFree(free map[string]string) *Error {
	if len(free) > MaxFreeform {
		return invalid("A resource can have at most %d free-form tags", MaxFreeform)
	}

	for k, v := range free {
		if !validKey(k) {
			return invalid("Free-form tag key %q must be 1 to %d printable ASCII characters without periods or spaces",
				k, MaxKeyLen)
		}

		if utf8.RuneCountInString(v) > MaxValueLen {
			return invalid("The value of free-form tag %q exceeds %d characters", k, MaxValueLen)
		}
	}

	return nil
}

func checkDefined(defined map[string]map[string]any, lookup Lookup) *Error {
	total := 0
	missing := []string{}

	for ns, keys := range defined {
		for k, v := range keys {
			total++

			if msg, notFound := checkOne(ns, k, v, lookup); notFound {
				missing = append(missing, "'"+ns+"."+k+"'")
			} else if msg != "" {
				return invalid("%s", msg)
			}
		}
	}

	if total > MaxDefined {
		return invalid("A resource can have at most %d defined tags", MaxDefined)
	}

	if len(missing) > 0 {
		sort.Strings(missing)

		return &Error{
			Status: http.StatusNotFound,
			Code:   "NotAuthorizedOrNotFound",
			Message: "The following tag namespaces / keys are not authorized or not found: " + strings.Join(
				missing,
				", ",
			),
		}
	}

	return nil
}

func checkOne(ns, key string, value any, lookup Lookup) (string, bool) {
	if !validKey(ns) || !validKey(key) {
		return fmt.Sprintf("Defined tag %q.%q must use 1 to %d printable ASCII characters without periods or spaces",
			ns, key, MaxKeyLen), false
	}

	if s, isStr := value.(string); isStr && utf8.RuneCountInString(s) > MaxValueLen {
		return fmt.Sprintf("The value of defined tag %s.%s exceeds %d characters", ns, key, MaxValueLen), false
	}

	if lookup == nil {
		return "", false
	}

	def, found := lookup(ns, key)
	if !found {
		return "", true
	}

	if def.Retired {
		return fmt.Sprintf("Tag %s.%s is retired and cannot be applied", ns, key), false
	}

	if def.HasEnum {
		s, _ := value.(string)
		if !slices.Contains(def.EnumValues, s) {
			return fmt.Sprintf("Value for tag %s.%s must be one of the validator's values", ns, key), false
		}
	}

	return "", false
}
