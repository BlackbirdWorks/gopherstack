package translate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

// TestInMemoryBackend_ImportTerminology_DirectionalityPassthrough verifies that a
// caller-specified Directionality ("UNI" or "MULTI") is honored instead of
// always being forced to "UNI", and that an invalid value is rejected.
func TestInMemoryBackend_ImportTerminology_DirectionalityPassthrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		directionality  string
		wantDirectional string
		wantError       bool
	}{
		{name: "defaults to UNI when unspecified", directionality: "", wantDirectional: "UNI"},
		{name: "UNI accepted", directionality: "UNI", wantDirectional: "UNI"},
		{name: "MULTI accepted", directionality: "MULTI", wantDirectional: "MULTI"},
		{name: "invalid value rejected", directionality: "BIDIRECTIONAL", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			data := &translate.TerminologyData{
				Format:         "CSV",
				Directionality: tt.directionality,
				File:           []byte("en,es\nhi,hola"),
			}
			term, err := b.ImportTerminology("dir-term-"+tt.name, "", data, nil, nil)

			if tt.wantError {
				require.ErrorIs(t, err, translate.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDirectional, term.Directionality)
		})
	}
}
