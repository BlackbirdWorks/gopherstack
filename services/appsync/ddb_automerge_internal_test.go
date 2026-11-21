package appsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func strA(v string) map[string]any    { return map[string]any{"S": v} }
func numA(v string) map[string]any    { return map[string]any{"N": v} }
func listA(v ...any) map[string]any   { return map[string]any{"L": v} }
func setAttr(v ...any) map[string]any { return map[string]any{"SS": v} }
func mapA(v map[string]any) any       { return map[string]any{"M": v} }

func deepMap(depth int) map[string]any {
	leaf := map[string]any{"v": strA("x")}
	for range depth {
		leaf = map[string]any{"c": mapA(leaf)}
	}

	return leaf
}

func TestAutomerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		existing map[string]any
		incoming map[string]any
		want     map[string]any
		name     string
	}{
		{
			name:     "scalar keeps existing",
			existing: map[string]any{"name": strA("Nadia"), "jersey": numA("5")},
			incoming: map[string]any{"name": strA("Shaggy"), "jersey": numA("55")},
			want:     map[string]any{"name": strA("Nadia"), "jersey": numA("5")},
		},
		{
			name:     "new fields added and omitted fields preserved",
			existing: map[string]any{"name": strA("Nadia"), "jersey": numA("5")},
			incoming: map[string]any{"name": strA("Shaggy"), "points": listA(numA("24")), "interests": setAttr("a")},
			want: map[string]any{
				"name": strA("Nadia"), "jersey": numA("5"), "points": listA(numA("24")), "interests": setAttr("a"),
			},
		},
		{
			name:     "lists concatenate keeping duplicates",
			existing: map[string]any{"points": listA(numA("24"), numA("30"), numA("27"))},
			incoming: map[string]any{"points": listA(numA("30"), numA("35"))},
			want:     map[string]any{"points": listA(numA("24"), numA("30"), numA("27"), numA("30"), numA("35"))},
		},
		{
			name:     "sets union without duplicates",
			existing: map[string]any{"interests": setAttr("breakfast", "lunch", "dinner")},
			incoming: map[string]any{"interests": map[string]any{"SS": []string{"breakfast", "lunch", "brunch"}}},
			want:     map[string]any{"interests": setAttr("breakfast", "lunch", "dinner", "brunch")},
		},
		{
			name:     "maps merge per property",
			existing: map[string]any{"stats": mapA(map[string]any{"ppg": strA("35.4"), "apg": strA("6.3")})},
			incoming: map[string]any{"stats": mapA(map[string]any{"ppg": strA("25.7"), "rpg": strA("6.9")})},
			want: map[string]any{
				"stats": mapA(map[string]any{"ppg": strA("35.4"), "apg": strA("6.3"), "rpg": strA("6.9")}),
			},
		},
		{
			name:     "null existing takes incoming",
			existing: map[string]any{"name": map[string]any{"NULL": true}},
			incoming: map[string]any{"name": strA("Shaggy")},
			want:     map[string]any{"name": strA("Shaggy")},
		},
		{
			name:     "type mismatch keeps existing",
			existing: map[string]any{"a": strA("x")},
			incoming: map[string]any{"a": listA(strA("y"))},
			want:     map[string]any{"a": strA("x")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, automerge(tt.existing, tt.incoming, 0))
		})
	}
}

func TestAutomergeBoundsRecursion(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		got := automerge(deepMap(maxMergeDepth+8), deepMap(maxMergeDepth+8), 0)
		assert.NotNil(t, got)
	})
}

func lambdaSync(arn string) *SyncConfig {
	return &SyncConfig{
		ConflictHandler:             "LAMBDA",
		LambdaConflictHandlerConfig: &LambdaConflictHandlerConfig{LambdaConflictHandlerARN: arn},
	}
}

func TestValidateSyncConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *SyncConfig
		name    string
		wantErr bool
	}{
		{name: "nil", cfg: nil},
		{name: "version automerge", cfg: &SyncConfig{ConflictDetection: "VERSION", ConflictHandler: "AUTOMERGE"}},
		{name: "bad detection", cfg: &SyncConfig{ConflictDetection: "TIME"}, wantErr: true},
		{name: "bad handler", cfg: &SyncConfig{ConflictHandler: "LAST_WRITER"}, wantErr: true},
		{name: "lambda without config", cfg: &SyncConfig{ConflictHandler: "LAMBDA"}, wantErr: true},
		{name: "lambda arn not an arn", cfg: lambdaSync("function:f"), wantErr: true},
		{name: "lambda arn of another service", cfg: lambdaSync("arn:aws:s3:::bucket"), wantErr: true},
		{name: "lambda layer arn", cfg: lambdaSync("arn:aws:lambda:us-east-1:000000000000:layer:l:1"), wantErr: true},
		{
			name: "lambda arn without a name", cfg: lambdaSync("arn:aws:lambda:us-east-1:000000000000:function:"),
			wantErr: true,
		},
		{name: "lambda arn with a qualifier", cfg: lambdaSync("arn:aws:lambda:us-east-1:000000000000:function:f:live")},
		{
			name: "lambda with arn",
			cfg: &SyncConfig{ConflictHandler: "LAMBDA", LambdaConflictHandlerConfig: &LambdaConflictHandlerConfig{
				LambdaConflictHandlerARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateSyncConfig(tt.cfg)
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrValidation)

				return
			}

			assert.NoError(t, err)
		})
	}
}
