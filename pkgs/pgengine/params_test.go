package pgengine_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
)

func TestSetParameterRefusesUnsafeInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		param string
		value string
	}{
		{"name with statement", "work_mem; DROP DATABASE postgres", "1"},
		{"name with quote", `work_mem"`, "1"},
		{"upper case name", "Work_Mem", "1"},
		{"empty name", "", "1"},
		{"value with backslash", "work_mem", `1\`},
		{"value with newline", "work_mem", "1\n2"},
		{"value with nul", "work_mem", "1\x002"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := (&pgengine.Server{}).SetParameter(t.Context(), tt.param, tt.value, false)
			require.ErrorIs(t, err, pgengine.ErrInvalidParameter)
		})
	}
}

func TestDumpRefusesUnsafeDatabaseNames(t *testing.T) {
	t.Parallel()

	for _, db := range []string{"a=b", "x y", "", "a;b", `a"b`, "-h"} {
		t.Run(db, func(t *testing.T) {
			t.Parallel()

			err := (&pgengine.Server{}).Dump(t.Context(), &bytes.Buffer{}, []string{db}, "admin")
			assert.ErrorIs(t, err, pgengine.ErrInvalidDatabase)
		})
	}
}
