package ssm_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// putVersions creates name with each value in order, returning after the final
// PutParameter. The first call creates v1; subsequent calls overwrite to v2..vN.
func putVersions(t *testing.T, b *ssm.InMemoryBackend, name string, values ...string) {
	t.Helper()
	for i, v := range values {
		_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:      name,
			Type:      "String",
			Value:     v,
			Overwrite: i > 0,
		})
		require.NoError(t, err)
	}
}

func TestGetParameter_VersionSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		selector  string
		wantValue string
		wantSel   string
		wantVer   int64
	}{
		{name: "no selector returns latest", selector: "", wantValue: "v3", wantVer: 3, wantSel: ""},
		{name: "version 1", selector: ":1", wantValue: "v1", wantVer: 1, wantSel: ":1"},
		{name: "version 2", selector: ":2", wantValue: "v2", wantVer: 2, wantSel: ":2"},
		{name: "version 3 (latest explicit)", selector: ":3", wantValue: "v3", wantVer: 3, wantSel: ":3"},
		{name: "missing version", selector: ":99", wantErr: ssm.ErrParameterVersionNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			putVersions(t, b, "/app/db", "v1", "v2", "v3")

			out, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{
				Name: "/app/db" + tc.selector,
			})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantValue, out.Parameter.Value)
			assert.Equal(t, tc.wantVer, out.Parameter.Version)
			assert.Equal(t, tc.wantSel, out.Parameter.Selector)
			// Base name (without selector) is always returned.
			assert.Equal(t, "/app/db", out.Parameter.Name)
			// ARN is always populated and excludes any selector suffix.
			assert.Equal(t, "arn:aws:ssm:us-east-1:000000000000:parameter/app/db", out.Parameter.ARN)
		})
	}
}

func TestGetParameter_LabelSelector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	putVersions(t, b, "/app/key", "v1", "v2", "v3")

	// Label version 1 as "prod".
	_, err := b.LabelParameterVersion(context.TODO(), &ssm.LabelParameterVersionInput{
		Name:             "/app/key",
		ParameterVersion: 1,
		Labels:           []string{"prod"},
	})
	require.NoError(t, err)

	out, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/app/key:prod"})
	require.NoError(t, err)
	assert.Equal(t, "v1", out.Parameter.Value)
	assert.Equal(t, int64(1), out.Parameter.Version)
	assert.Equal(t, ":prod", out.Parameter.Selector)

	// Unknown label resolves to ParameterNotFound (AWS semantics).
	_, err = b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/app/key:staging"})
	require.ErrorIs(t, err, ssm.ErrParameterNotFound)
}

func TestLabelParameterVersion_MovesLabel(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	putVersions(t, b, "/app/move", "v1", "v2")

	// Label v1 as "live".
	_, err := b.LabelParameterVersion(context.TODO(), &ssm.LabelParameterVersionInput{
		Name: "/app/move", ParameterVersion: 1, Labels: []string{"live"},
	})
	require.NoError(t, err)

	// Re-apply "live" to v2 — it must move, not duplicate.
	out, err := b.LabelParameterVersion(context.TODO(), &ssm.LabelParameterVersionInput{
		Name: "/app/move", ParameterVersion: 2, Labels: []string{"live"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), out.ParameterVersion)

	// The label now points at v2.
	got, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/app/move:live"})
	require.NoError(t, err)
	assert.Equal(t, "v2", got.Parameter.Value)
	assert.Equal(t, int64(2), got.Parameter.Version)
}

func TestGetParameters_SelectorAndArn(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	putVersions(t, b, "/app/a", "a1", "a2")
	putVersions(t, b, "/app/b", "b1")

	out, err := b.GetParameters(context.TODO(), &ssm.GetParametersInput{
		Names: []string{"/app/a:1", "/app/b", "/app/missing", "/app/a:99"},
	})
	require.NoError(t, err)

	// Two valid: /app/a:1 and /app/b. Two invalid: missing name + missing version.
	require.Len(t, out.Parameters, 2)
	assert.ElementsMatch(t, []string{"/app/missing", "/app/a:99"}, out.InvalidParameters)

	byName := map[string]ssm.ParameterOutput{}
	for _, p := range out.Parameters {
		byName[p.Name] = p
		assert.True(t, strings.HasPrefix(p.ARN, "arn:aws:ssm:"), "ARN populated")
	}
	assert.Equal(t, "a1", byName["/app/a"].Value)
	assert.Equal(t, ":1", byName["/app/a"].Selector)
	assert.Equal(t, "b1", byName["/app/b"].Value)
	assert.Empty(t, byName["/app/b"].Selector)
}

func TestGetParametersByPath_PopulatesArn(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	putVersions(t, b, "/app/x", "x1")
	putVersions(t, b, "/app/y", "y1")

	out, err := b.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{Path: "/app"})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	for _, p := range out.Parameters {
		assert.Equal(t,
			"arn:aws:ssm:us-east-1:000000000000:parameter"+p.Name,
			p.ARN,
		)
	}
}
