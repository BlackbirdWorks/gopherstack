package chaos_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

func TestFaultRule_Validate(t *testing.T) {
	t.Parallel()

	one := 1.0
	tests := []struct {
		name    string
		rule    chaos.FaultRule
		wantErr bool
	}{
		{name: "plain rule", rule: chaos.FaultRule{Service: "s3"}},
		{name: "dynamodb conflict effect", rule: chaos.FaultRule{
			Effect: chaos.EffectDynamoDBTransactionConflict, Service: "dynamodb", Probability: &one, HoldOps: 2,
			Target: &chaos.EffectTarget{Table: "t", Key: map[string]string{"pk": "a"}},
		}},
		{name: "unknown effect", rule: chaos.FaultRule{Effect: "dynamodb:nope"}, wantErr: true},
		{name: "target without effect", rule: chaos.FaultRule{Target: &chaos.EffectTarget{Table: "t"}}, wantErr: true},
		{name: "hold without effect", rule: chaos.FaultRule{HoldMs: 5}, wantErr: true},
		{
			name:    "wrong service",
			rule:    chaos.FaultRule{Effect: chaos.EffectDynamoDBTransactionConflict, Service: "s3"},
			wantErr: true,
		},
		{name: "effect with error", rule: chaos.FaultRule{
			Effect: chaos.EffectDynamoDBTransactionConflict, Error: &chaos.FaultError{Code: "X", StatusCode: 500},
		}, wantErr: true},
		{
			name:    "negative hold",
			rule:    chaos.FaultRule{Effect: chaos.EffectDynamoDBTransactionConflict, HoldOps: -1},
			wantErr: true,
		},
		{name: "key without table", rule: chaos.FaultRule{
			Effect: chaos.EffectDynamoDBTransactionConflict,
			Target: &chaos.EffectTarget{Key: map[string]string{"pk": "a"}},
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rule.Validate()
			if tt.wantErr {
				require.ErrorIs(t, err, chaos.ErrInvalidFaultRule)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestFaultStore_EffectRulesAreNotHTTPFaults(t *testing.T) {
	t.Parallel()

	effect := chaos.FaultRule{
		Effect:  chaos.EffectDynamoDBTransactionConflict,
		Service: "dynamodb",
		Target:  &chaos.EffectTarget{Table: "t"},
	}
	plain := chaos.FaultRule{Service: "dynamodb"}

	store := chaos.NewFaultStore()
	store.SetRules([]chaos.FaultRule{effect})

	_, matched := store.Match("dynamodb", "PutItem", "us-east-1")
	assert.False(t, matched, "Match must skip effect rules")
	assert.Len(t, store.MatchEffects(chaos.EffectDynamoDBTransactionConflict, "dynamodb", "PutItem", "us-east-1"), 1)
	assert.Empty(t, store.MatchEffects(chaos.EffectDynamoDBTransactionConflict, "s3", "PutItem", "us-east-1"))

	store.SetRules([]chaos.FaultRule{effect, plain})
	rule, matched := store.Match("dynamodb", "PutItem", "us-east-1")
	assert.True(t, matched)
	assert.Empty(t, rule.Effect)

	store.DeleteRules([]chaos.FaultRule{plain})
	assert.Equal(t, []chaos.FaultRule{effect}, store.GetRules(), "deleting a plain rule leaves the effect rule")

	store.DeleteExactRules([]chaos.FaultRule{{Effect: chaos.EffectDynamoDBTransactionConflict, Service: "dynamodb"}})
	assert.Len(t, store.GetRules(), 1, "exact delete compares the target too")

	store.DeleteExactRules([]chaos.FaultRule{effect})
	assert.Empty(t, store.GetRules())
}

func TestMiddleware_PassesThroughEffectRules(t *testing.T) {
	t.Parallel()

	store := chaos.NewFaultStore()
	store.SetRules([]chaos.FaultRule{{Effect: chaos.EffectDynamoDBTransactionConflict}})

	called := false
	wrapped := chaos.Middleware(store)(func(c *echo.Context) error {
		called = true

		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	req.Header.Set("Authorization", buildSigV4Auth("dynamodb", "us-east-1"))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")

	rec := httptest.NewRecorder()
	require.NoError(t, wrapped(echo.New().NewContext(req, rec)))

	assert.True(t, called)
	assert.Equal(t, http.StatusOK, rec.Code)
}
