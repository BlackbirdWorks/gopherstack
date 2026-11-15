package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizationAllowed(t *testing.T) {
	t.Parallel()

	const (
		dataReader  = "1407120a-92aa-4202-b7e9-c0e197c71c8f"
		dataContrib = "8ebe5a00-799e-43f5-93ac-243d3dce84a7"
		svcContrib  = "7ca78c08-252a-4471-8644-bb5ff32d4ba0"
		docsRead    = "Microsoft.Search/searchServices/indexes/documents/read"
		docsWrite   = "Microsoft.Search/searchServices/indexes/documents/write"
		objects     = "Microsoft.Search/searchServices/indexes/write"
		svc         = authRGScope + "/providers/Microsoft.Search/searchServices/s1"
	)

	s := newAuthStack(t)
	ctx := t.Context()

	for guid, assign := range map[string][2]string{
		guidA: {dataReader, authRGScope}, guidB: {dataContrib, authRGScope}, guidC: {svcContrib, authSubScope},
		guidD: {roleOwner, authRGScope},
	} {
		_, err := s.assigns.Create(ctx, assign[1], guid, assignParams(assign[0], guid, nil), nil)
		require.NoError(t, err)
	}

	authz, ok := s.handler.Registry.ProviderNamed("Microsoft.Authorization")
	require.True(t, ok)

	a, ok := authz.(interface {
		Allowed(principalID, resourceID, action string, data bool) bool
	})
	require.True(t, ok)

	cases := []struct {
		name      string
		principal string
		resource  string
		action    string
		data      bool
		want      bool
	}{
		{"reader reads", guidA, svc, docsRead, true, true},
		{"reader cannot write", guidA, svc, docsWrite, true, false},
		{
			"reader other rg",
			guidA,
			authSubScope + "/resourceGroups/other/providers/Microsoft.Search/searchServices/s2",
			docsRead,
			true,
			false,
		},
		{"contributor inherits rg scope", guidB, svc, docsWrite, true, true},
		{"contributor reads", guidB, svc, docsRead, true, true},
		{"contributor no objects", guidB, svc, objects, false, false},
		{"service contributor objects", guidC, svc, objects, false, true},
		{"service contributor no docs", guidC, svc, docsRead, true, false},
		{"owner objects", guidD, svc, objects, false, true},
		{"owner has no data actions", guidD, svc, docsRead, true, false},
		{"unknown principal", "99999999-9999-9999-9999-999999999999", svc, docsRead, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, a.Allowed(tc.principal, tc.resource, tc.action, tc.data))
		})
	}
}
