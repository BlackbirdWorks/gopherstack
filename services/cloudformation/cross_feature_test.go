package cloudformation_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Resource scan with real stacks (table-driven) --------------------------------

func TestResourceScan_PopulatesFromStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		stacks       []string
		wantContains []string
		wantMinItems int
	}{
		{
			name:         "no stacks yields synthetic S3 resource",
			stacks:       nil,
			wantMinItems: 1,
			wantContains: []string{"AWS::S3::Bucket"},
		},
		{
			name:   "single stack resources appear in scan",
			stacks: []string{simpleTemplate},
			// simpleTemplate has AWS::S3::Bucket so at least 1 real resource.
			wantMinItems: 1,
			wantContains: []string{"AWS::S3::Bucket"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			for i, tmpl := range tc.stacks {
				_, err := b.CreateStack(t.Context(),
					"scan-stack-"+string(rune('a'+i)), tmpl, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			scanID, err := b.StartResourceScan()
			require.NoError(t, err)
			assert.NotEmpty(t, scanID)

			scan, err := b.DescribeResourceScan(scanID)
			require.NoError(t, err)
			assert.Equal(t, "COMPLETE", scan.Status)
			assert.InEpsilon(t, float64(100), scan.PercentageCompleted, 0.001)

			resources, err := b.ListResourceScanResources(scanID, "")
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(resources), tc.wantMinItems)

			types := make([]string, 0, len(resources))
			for _, r := range resources {
				types = append(types, r.ResourceType)
			}
			for _, want := range tc.wantContains {
				found := slices.Contains(types, want)
				assert.True(t, found, "expected resource type %q in scan results", want)
			}
		})
	}
}

// ---- Generated template content (table-driven) -----------------------------------

func TestGeneratedTemplate_Body(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceIDs  []string
		setup        func(*cloudformation.InMemoryBackend)
		wantContains []string
	}{
		{
			name:        "empty resource list generates template from existing stacks",
			resourceIDs: nil,
			setup: func(b *cloudformation.InMemoryBackend) {
				_, _ = b.CreateStack(t.Context(), "gen-tmpl-src", simpleTemplate, nil,
					cloudformation.StackOptions{})
			},
			wantContains: []string{"AWSTemplateFormatVersion", "Resources"},
		},
		{
			name:        "with Type/LogicalID resource IDs",
			resourceIDs: []string{"AWS::SQS::Queue/MyQueue", "AWS::SNS::Topic/MyTopic"},
			wantContains: []string{
				"AWS::SQS::Queue",
				"AWS::SNS::Topic",
				"AWSTemplateFormatVersion",
			},
		},
		{
			name:         "no resource IDs and no stacks yields empty resources",
			resourceIDs:  nil,
			wantContains: []string{"AWSTemplateFormatVersion"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tc.setup != nil {
				tc.setup(b)
			}

			gt, err := b.CreateGeneratedTemplate("test-template", tc.resourceIDs)
			require.NoError(t, err)
			require.NotNil(t, gt)
			assert.NotEmpty(t, gt.GeneratedTemplateID)
			assert.Equal(t, "COMPLETE", gt.Status)

			body, err := b.GetGeneratedTemplate(gt.GeneratedTemplateID)
			require.NoError(t, err)
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want, "expected %q in generated template body", want)
			}
		})
	}
}

// ---- Drift simulation (table-driven) ---------------------------------------------

func TestDriftSimulation(t *testing.T) {
	t.Parallel()

	tmplWithQueue := `{
		"AWSTemplateFormatVersion":"2010-09-09",
		"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}
	}`

	tests := []struct {
		name          string
		wantStatus    string
		simulateDrift bool
		wantDrifted   bool
	}{
		{
			name:          "without drift simulation all resources IN_SYNC",
			simulateDrift: false,
			wantStatus:    "IN_SYNC",
			wantDrifted:   false,
		},
		{
			name:          "after SimulateDrift resources are DRIFTED",
			simulateDrift: true,
			wantStatus:    "DRIFTED",
			wantDrifted:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStack(t.Context(), "drift-sim", tmplWithQueue, nil,
				cloudformation.StackOptions{})
			require.NoError(t, err)

			if tc.simulateDrift {
				err = b.SimulateDrift("drift-sim")
				require.NoError(t, err)
			}

			drifts, err := b.DescribeStackResourceDrifts("drift-sim")
			require.NoError(t, err)

			if tc.wantDrifted {
				require.NotEmpty(t, drifts)
			}
			for _, d := range drifts {
				assert.Equal(t, tc.wantStatus, d.StackResourceDriftStatus)
			}
		})
	}
}

func TestDriftSimulation_StackNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()
	err := b.SimulateDrift("nonexistent")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

// ---- Error variables exported (package-level) ------------------------------------

func TestErrorVariables_Exported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
	}{
		{name: "ErrStackNotFound", err: cloudformation.ErrStackNotFound},
		{name: "ErrChangeSetAlreadyExecuted", err: cloudformation.ErrChangeSetAlreadyExecuted},
		{name: "ErrStackInstanceAlreadyExists", err: cloudformation.ErrStackInstanceAlreadyExists},
		{name: "ErrTypeVersionNotFound", err: cloudformation.ErrTypeVersionNotFound},
		{name: "ErrInvalidRoleARN", err: cloudformation.ErrInvalidRoleARN},
		{name: "ErrInsufficientCapabilities", err: cloudformation.ErrInsufficientCapabilities},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tc.err)
			assert.NotEmpty(t, tc.err.Error())
		})
	}
}
