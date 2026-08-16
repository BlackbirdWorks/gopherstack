package cloudformation_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestResourceCreator_AppSync_Supplemental_CreateDelete tests AppSync data source,
// resolver, function, and API key via CFN resource types.
func TestResourceCreator_AppSync_Supplemental_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)

	// First create a GraphQL API to use as parent.
	rc := cloudformation.NewResourceCreator(backends)
	apiPhysID, setupErr := rc.Create(t.Context(), "MyAPI", "AWS::AppSync::GraphQLApi", map[string]any{
		"Name":               "cfn-test-api",
		"AuthenticationType": "API_KEY",
	}, nil, nil)
	require.NoError(t, setupErr)
	require.NotEmpty(t, apiPhysID)

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "datasource",
			logicalID:    "MyDS",
			resourceType: "AWS::AppSync::DataSource",
			props: map[string]any{
				"ApiId": apiPhysID,
				"Name":  "cfn-test-ds",
				"Type":  "NONE",
			},
		},
		{
			name:         "api_key",
			logicalID:    "MyKey",
			resourceType: "AWS::AppSync::ApiKey",
			props: map[string]any{
				"ApiId":       apiPhysID,
				"Description": "test key",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends2 := newExtraServiceBackends(t)
			// Re-create API in this backend so each subtest is independent.
			rc2 := cloudformation.NewResourceCreator(backends2)
			api2PhysID, err := rc2.Create(t.Context(), "MyAPI2", "AWS::AppSync::GraphQLApi", map[string]any{
				"Name":               "cfn-test-api-2-" + tt.name,
				"AuthenticationType": "API_KEY",
			}, nil, nil)
			require.NoError(t, err)

			props2 := maps.Clone(tt.props)
			props2["ApiId"] = api2PhysID

			physID, err := rc2.Create(t.Context(), tt.logicalID, tt.resourceType, props2, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc2.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}
