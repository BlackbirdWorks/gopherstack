package ocitags_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func lookup(ns, key string) (ocitags.Def, bool) {
	switch strings.ToLower(ns + "." + key) {
	case "ops.cost":
		return ocitags.Def{}, true
	case "ops.tier":
		return ocitags.Def{HasEnum: true, EnumValues: []string{"gold", "silver"}}, true
	case "ops.old":
		return ocitags.Def{Retired: true}, true
	}

	return ocitags.Def{}, false
}

func TestValidate(t *testing.T) {
	t.Parallel()

	many := map[string]string{}
	for _, k := range "abcdefghijk" {
		many[string(k)] = "v"
	}

	tests := []struct {
		free   map[string]string
		def    map[string]map[string]any
		lookup ocitags.Lookup
		name   string
		code   string
		status int
	}{
		{name: "empty", lookup: lookup},
		{
			name:   "valid",
			free:   map[string]string{"env": "x"},
			def:    map[string]map[string]any{"Ops": {"Cost": "1"}},
			lookup: lookup,
		},
		{name: "too many free", free: many, code: "InvalidParameter", status: http.StatusBadRequest},
		{
			name:   "free key dot",
			free:   map[string]string{"a.b": "x"},
			code:   "InvalidParameter",
			status: http.StatusBadRequest,
		},
		{
			name:   "free key space",
			free:   map[string]string{"a b": "x"},
			code:   "InvalidParameter",
			status: http.StatusBadRequest,
		},
		{
			name: "free value long", free: map[string]string{"a": strings.Repeat("x", 257)},
			code: "InvalidParameter", status: http.StatusBadRequest,
		},
		{
			name: "unknown namespace", def: map[string]map[string]any{"Nope": {"k": "v"}}, lookup: lookup,
			code: "NotAuthorizedOrNotFound", status: http.StatusNotFound,
		},
		{
			name: "unknown key", def: map[string]map[string]any{"ops": {"nope": "v"}}, lookup: lookup,
			code: "NotAuthorizedOrNotFound", status: http.StatusNotFound,
		},
		{name: "standalone accepts any", def: map[string]map[string]any{"Nope": {"k": "v"}}},
		{name: "enum ok", def: map[string]map[string]any{"OPS": {"TIER": "gold"}}, lookup: lookup},
		{
			name: "enum bad", def: map[string]map[string]any{"ops": {"tier": "bronze"}}, lookup: lookup,
			code: "InvalidParameter", status: http.StatusBadRequest,
		},
		{
			name: "retired", def: map[string]map[string]any{"ops": {"old": "x"}}, lookup: lookup,
			code: "InvalidParameter", status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ocitags.Validate(tt.free, tt.def, tt.lookup)
			if tt.code == "" {
				assert.Nil(t, err)

				return
			}

			if assert.NotNil(t, err) {
				assert.Equal(t, tt.code, err.Code)
				assert.Equal(t, tt.status, err.Status)
			}
		})
	}
}

func TestValidateDefinedLimits(t *testing.T) {
	t.Parallel()

	def := map[string]map[string]any{"ns": {}}
	for i := range 65 {
		def["ns"][strings.Repeat("k", i+1)] = "v"
	}

	err := ocitags.Validate(nil, def, nil)
	if assert.NotNil(t, err) {
		assert.Equal(t, http.StatusBadRequest, err.Status)
	}

	big := map[string]string{}
	for i := range 10 {
		big[strings.Repeat("k", i+1)] = strings.Repeat("v", 256)
	}

	bigDef := map[string]map[string]any{"ns": {}}
	for i := range 12 {
		bigDef["ns"][strings.Repeat("d", i+1)] = strings.Repeat("v", 256)
	}

	err = ocitags.Validate(big, bigDef, nil)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Message, "5 K")
	}
}
