package gcpsecretmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

func TestFilterEvaluation(t *testing.T) {
	t.Parallel()

	fields := map[string][]string{
		"name":          {"projects/p/secrets/db-password"},
		"labels.env":    {"prod"},
		"create_time":   {"2030-01-01T00:00:00Z"},
		"annotations.x": {"y"},
		"*":             {"projects/p/secrets/db-password", "env", "prod"},
	}

	tests := []struct {
		name    string
		filter  string
		want    bool
		wantErr bool
	}{
		{"empty", "", true, false},
		{"name contains", "name:password", true, false},
		{"name contains case", "name:PASSWORD", true, false},
		{"name equals miss", "name=db", false, false},
		{"not equals", "labels.env!=dev", true, false},
		{"missing label not equals", "labels.zzz!=dev", true, false},
		{"gte time", "create_time>=2030-01-01", true, false},
		{"lt time", "create_time<2030-01-01", false, false},
		{"quoted value", `annotations.x="y"`, true, false},
		{"explicit and", "name:db AND labels.env=prod", true, false},
		{"or miss", "name:zzz OR labels.env=dev", false, false},
		{"bare", "prod", true, false},
		{"bare miss", "staging", false, false},
		{"negation keyword", "NOT name:db", false, false},
		{"unterminated quote", `name:"db`, false, true},
		{"unbalanced paren", "(name:db", false, true},
		{"dangling op", "name:", false, true},
		{"trailing paren", "name:db)", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpsecretmanager.CompileFilter(tt.filter, fields)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &gcpsecretmanager.Provider{}

	_, err := p.Init(nil)
	require.ErrorIs(t, err, gcpsecretmanager.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*gcpsecretmanager.Handler)
	require.True(t, ok)
	assert.Equal(t, gcpsecretmanager.DefaultPort, h.Port)
	assert.Equal(t, "GCPSecretManager", h.Name())
	assert.Contains(t, h.GetSupportedOperations(), "AccessSecretVersion")
	assert.False(t, h.RouteMatcher()(nil))
}
