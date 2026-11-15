package azurearm

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	limitsSub  = "/subscriptions/11111111-1111-1111-1111-111111111111"
	limitsMG   = "/providers/Microsoft.Management/managementGroups/mg1"
	limitsRole = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635"
)

func fillAssignments(p *AuthorizationProvider, scope string, n int) {
	for range n {
		name := uuid.NewString()
		p.roleAssigns[authKey(scope, name)] = &storedRoleAssign{
			Name: name, Scope: scope, RoleGUID: limitsRole, PrincipalID: uuid.NewString(),
		}
	}
}

func assignReq(t *testing.T, scope string) authReq {
	t.Helper()

	segs := strings.Split(strings.Trim(scope, "/"), "/")
	sc, ok := parseAuthScope(segs)
	require.True(t, ok)

	return authReq{
		Method: http.MethodPut, Name: uuid.NewString(), Scope: sc,
		Body: map[string]any{fieldProperties: map[string]any{
			"roleDefinitionId":   "/providers/Microsoft.Authorization/roleDefinitions/" + limitsRole,
			authFieldPrincipalID: uuid.NewString(),
		}},
	}
}

func TestRoleAssignmentLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fillAt   string
		putAt    string
		wantCode string
		fill     int
	}{
		{
			name: "subscription below limit", fillAt: limitsSub + "/resourceGroups/a", fill: 4999,
			putAt: limitsSub + "/resourceGroups/b",
		},
		{
			name: "subscription at limit", fillAt: limitsSub + "/resourceGroups/a", fill: 5000,
			putAt: limitsSub + "/resourceGroups/b", wantCode: "RoleAssignmentLimitExceeded",
		},
		{name: "management group assignments do not count", fillAt: limitsMG, fill: 5000, putAt: limitsSub},
		{
			name: "management group at limit", fillAt: limitsMG, fill: 500, putAt: limitsMG,
			wantCode: "RoleAssignmentLimitExceeded",
		},
		{
			name: "other subscription unaffected", fillAt: "/subscriptions/22222222-2222-2222-2222-222222222222",
			fill: 5000, putAt: limitsSub,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := NewAuthorizationProvider("", nil)
			fillAssignments(p, tc.fillAt, tc.fill)

			_, err := p.putRoleAssignment(assignReq(t, tc.putAt))

			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, fmt.Sprint(err), "No more role assignments can be created")
			assert.Equal(t, tc.wantCode, codeOf(err))
		})
	}
}

func TestCustomRoleDefinitionLimit(t *testing.T) {
	t.Parallel()

	p := NewAuthorizationProvider("", nil)

	for range 5000 {
		name := uuid.NewString()
		p.roleDefs[name] = &storedRoleDef{Name: name, RoleName: name}
	}

	sc, _ := parseAuthScope(strings.Split(strings.Trim(limitsSub, "/"), "/"))
	_, err := p.putRoleDefinition(authReq{
		Method: http.MethodPut, Name: uuid.NewString(), Scope: sc,
		Body: map[string]any{fieldProperties: map[string]any{
			"roleName": "limit-role", "assignableScopes": []any{limitsSub},
			"permissions": []any{map[string]any{"actions": []any{"*/read"}}},
		}},
	})

	require.Error(t, err)
	assert.Equal(t, "RoleDefinitionLimitExceeded", codeOf(err))
	assert.Contains(t, fmt.Sprint(err), "Role definition limit exceeded")
}

func codeOf(err error) string {
	e, _ := netErrorDetails(err)

	return e.code
}

func TestRoleAssignmentCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition string
		version   string
		wantVer   string
		wantErr   bool
	}{
		{name: "none"},
		{name: "defaults version", condition: "((!(ActionMatches{'x'})))", wantVer: "2.0"},
		{name: "explicit version", condition: "(@Resource[a:b] StringEquals 'x')", version: "2.0", wantVer: "2.0"},
		{name: "version without condition", version: "2.0", wantErr: true},
		{name: "unsupported version", condition: "(a)", version: "1.0", wantErr: true},
		{name: "unbalanced", condition: "((a)", wantErr: true},
		{name: "unterminated string", condition: "(a 'b)", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cond, ver, err := checkAssignmentCondition(tc.condition, tc.version)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.condition, cond)
			assert.Equal(t, tc.wantVer, ver)
		})
	}
}

func TestAssignableScopesExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scope   string
		wantErr bool
	}{
		{name: "known subscription", scope: limitsSub},
		{name: "unknown subscription", scope: "/subscriptions/22222222-2222-2222-2222-222222222222", wantErr: true},
		{name: "existing group", scope: limitsSub + "/resourceGroups/rg1"},
		{name: "missing group", scope: limitsSub + "/resourceGroups/nope", wantErr: true},
		{name: "management group", scope: limitsMG},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := NewAuthorizationProvider("", nil)
			p.SetScopeChecker("11111111-1111-1111-1111-111111111111", func(n string) bool { return n == "rg1" })

			err := p.assignableScopesExist([]string{tc.scope})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
