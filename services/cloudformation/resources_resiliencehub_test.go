package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	resiliencehubbackend "github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

// newResilienceHubServiceBackends returns a ServiceBackends wired to a real
// ResilienceHub handler, plus that same handler so tests can assert against
// its backend state directly (ServiceBackends.ResilienceHub is typed as the
// narrow cloudformation.ResilienceHubBackend interface, which has no read
// operations).
func newResilienceHubServiceBackends(t *testing.T) (*cloudformation.ServiceBackends, *resiliencehubbackend.Handler) {
	t.Helper()

	backend := resiliencehubbackend.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	h := resiliencehubbackend.NewHandler(backend)

	backends := &cloudformation.ServiceBackends{
		AccountID:     "000000000000",
		Region:        "us-east-1",
		ResilienceHub: h,
	}

	return backends, h
}

func TestResourceCreator_ResilienceHub_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name: "app", logicalID: "MyApp", resourceType: "AWS::ResilienceHub::App",
			props: map[string]any{"Name": "myapp"},
		},
		{
			name: "resiliency_policy", logicalID: "MyPolicy", resourceType: "AWS::ResilienceHub::ResiliencyPolicy",
			props: map[string]any{"PolicyName": "mypolicy", "Tier": "Critical"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(&cloudformation.ServiceBackends{
				AccountID: "000000000000",
				Region:    "us-east-1",
			})

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			require.NoError(t, rc.Delete(t.Context(), tt.resourceType, physID, tt.props))
		})
	}
}

// TestResourceCreator_ResilienceHub_App verifies AWS::ResilienceHub::App
// creation lands a real App -- with its template body and resource mappings
// -- in the ResilienceHub backend, that Ref resolves to the App's ARN, and
// that delete removes it.
func TestResourceCreator_ResilienceHub_App(t *testing.T) {
	t.Parallel()

	backends, h := newResilienceHubServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	props := map[string]any{
		"Name":                  "my-resilient-app",
		"Description":           "test app",
		"AppAssessmentSchedule": "Daily",
		"AppTemplateBody":       `{"resources":[],"version":2}`,
		"Tags":                  map[string]any{"env": "test"},
		"ResourceMappings": []any{
			map[string]any{
				"MappingType":  "Resource",
				"ResourceName": "lambda",
				"PhysicalResourceId": map[string]any{
					"Type":       "Arn",
					"Identifier": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
				},
			},
		},
	}

	physID, err := rc.Create(ctx, "MyApp", "AWS::ResilienceHub::App", props, nil, nil)
	require.NoError(t, err)
	require.Contains(t, physID, "arn:aws:resiliencehub")

	app, err := h.Backend.GetApp(physID)
	require.NoError(t, err)
	assert.Equal(t, "my-resilient-app", app.Name)
	assert.Equal(t, "test app", app.Description)
	assert.Equal(t, "Daily", app.AssessmentSchedule)
	assert.JSONEq(t, `{"resources":[],"version":2}`, app.Draft.TemplateBody)
	require.Len(t, app.Draft.ResourceMappings, 1)
	mapping := app.Draft.ResourceMappings[0]
	assert.Equal(t, "lambda", mapping.ResourceName)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:my-fn", mapping.PhysicalResourceID.Identifier)

	require.NoError(t, rc.Delete(ctx, "AWS::ResilienceHub::App", physID, props))

	_, err = h.Backend.GetApp(physID)
	require.Error(t, err)
}

// TestResourceCreator_ResilienceHub_ResiliencyPolicy verifies
// AWS::ResilienceHub::ResiliencyPolicy creation lands a real policy in the
// ResilienceHub backend and that delete removes it.
func TestResourceCreator_ResilienceHub_ResiliencyPolicy(t *testing.T) {
	t.Parallel()

	backends, h := newResilienceHubServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	props := map[string]any{
		"PolicyName":             "my-policy",
		"Tier":                   "MissionCritical",
		"PolicyDescription":      "critical workloads",
		"DataLocationConstraint": "AnyLocation",
		"Tags":                   map[string]any{"env": "test"},
		"Policy": map[string]any{
			"Software": map[string]any{"RtoInSecs": float64(3600), "RpoInSecs": float64(3600)},
			"Hardware": map[string]any{"RtoInSecs": float64(3600), "RpoInSecs": float64(3600)},
			"AZ":       map[string]any{"RtoInSecs": float64(3600), "RpoInSecs": float64(3600)},
			"Region":   map[string]any{"RtoInSecs": float64(86400), "RpoInSecs": float64(86400)},
		},
	}

	physID, err := rc.Create(ctx, "MyPolicy", "AWS::ResilienceHub::ResiliencyPolicy", props, nil, nil)
	require.NoError(t, err)
	require.Contains(t, physID, "arn:aws:resiliencehub")

	policy, err := h.Backend.DescribeResiliencyPolicy(physID)
	require.NoError(t, err)
	assert.Equal(t, "my-policy", policy.Name)
	assert.Equal(t, "MissionCritical", policy.Tier)
	assert.Equal(t, "critical workloads", policy.Description)
	assert.Equal(t, "AnyLocation", policy.DataLocationConstraint)
	require.Contains(t, policy.Policy, "Region")
	assert.Equal(t, int32(86400), policy.Policy["Region"].RtoInSecs)

	require.NoError(t, rc.Delete(ctx, "AWS::ResilienceHub::ResiliencyPolicy", physID, props))

	_, err = h.Backend.DescribeResiliencyPolicy(physID)
	require.Error(t, err)
}

// TestResourceCreator_ResilienceHub_ResiliencyPolicy_MissingDisruptionType verifies a Policy map
// missing a required disruption type surfaces as a create error rather than a silently incomplete
// policy.
func TestResourceCreator_ResilienceHub_ResiliencyPolicy_MissingDisruptionType(t *testing.T) {
	t.Parallel()

	backends, _ := newResilienceHubServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"PolicyName": "incomplete-policy",
		"Tier":       "Critical",
		"Policy": map[string]any{
			"Software": map[string]any{"RtoInSecs": float64(60), "RpoInSecs": float64(60)},
		},
	}

	_, err := rc.Create(t.Context(), "MyPolicy", "AWS::ResilienceHub::ResiliencyPolicy", props, nil, nil)
	require.Error(t, err)
}
