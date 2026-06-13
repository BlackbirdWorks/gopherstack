package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestListKeys_LimitBound verifies that ListKeys rejects an out-of-range Limit
// (AWS bound: 1–1000) with ValidationException, and accepts in-range values.
func TestListKeys_LimitBound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	i32 := func(v int32) *int32 { return &v }

	tests := []struct {
		limit   *int32
		name    string
		wantErr bool
	}{
		{name: "nil ok", limit: nil, wantErr: false},
		{name: "min ok", limit: i32(1), wantErr: false},
		{name: "max ok", limit: i32(1000), wantErr: false},
		{name: "zero rejected", limit: i32(0), wantErr: true},
		{name: "over cap rejected", limit: i32(1001), wantErr: true},
		{name: "negative rejected", limit: i32(-5), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.ListKeys(context.Background(), &kms.ListKeysInput{Limit: tc.limit})
			if tc.wantErr {
				require.ErrorIs(t, err, kms.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListAliases_LimitBound verifies ListAliases enforces the same 1–1000 bound.
func TestListAliases_LimitBound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	over := int32(1001)
	_, err := b.ListAliases(context.Background(), &kms.ListAliasesInput{Limit: &over})
	require.ErrorIs(t, err, kms.ErrValidation)

	ok := int32(50)
	_, err = b.ListAliases(context.Background(), &kms.ListAliasesInput{Limit: &ok})
	require.NoError(t, err)
}
