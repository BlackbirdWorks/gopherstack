package cloudformation_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// deleteTracker records resource types passed to ResourceCreator.Delete.
type deleteTracker struct {
	deleted []string
	mu      sync.Mutex
}

func (dt *deleteTracker) record(resType string) {
	dt.mu.Lock()
	dt.deleted = append(dt.deleted, resType)
	dt.mu.Unlock()
}

func (dt *deleteTracker) snapshot() []string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	out := make([]string, len(dt.deleted))
	copy(out, dt.deleted)

	return out
}

func newTrackedBackend(dt *deleteTracker) *cloudformation.InMemoryBackend {
	b := newBackend()
	b.GetCreator().InjectDeleteHook(dt.record)

	return b
}

// templateWithDeletionPolicy builds a JSON template with one S3 bucket resource
// with the given DeletionPolicy value (empty string omits the field).
func templateWithDeletionPolicy(policy string) string {
	res := map[string]any{
		"Type":       "AWS::S3::Bucket",
		"Properties": map[string]any{},
	}
	if policy != "" {
		res["DeletionPolicy"] = policy
	}
	body, _ := json.Marshal(map[string]any{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources":                map[string]any{"Bucket": res},
	})

	return string(body)
}

// TestDeletionPolicy_Delete_CallsDeleteOnResource verifies that the default
// (no DeletionPolicy) and explicit "Delete" cause Delete to be called.
func TestDeletionPolicy_Delete_CallsDeleteOnResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "no_policy_default_delete", policy: ""},
		{name: "explicit_delete", policy: "Delete"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dt := &deleteTracker{}
			b := newTrackedBackend(dt)

			_, err := b.CreateStack(context.Background(), "test-stack",
				templateWithDeletionPolicy(tc.policy), nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			err = b.DeleteStack(context.Background(), "test-stack")
			require.NoError(t, err)

			assert.Contains(t, dt.snapshot(), "AWS::S3::Bucket",
				"Delete should be called for policy=%q", tc.policy)
		})
	}
}

// TestDeletionPolicy_Retain_SkipsDelete verifies that DeletionPolicy=Retain
// prevents Delete from being called when the stack is deleted, while the stack
// still reaches DELETE_COMPLETE.
func TestDeletionPolicy_Retain_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	_, err := b.CreateStack(context.Background(), "retain-stack",
		templateWithDeletionPolicy("Retain"), nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	err = b.DeleteStack(context.Background(), "retain-stack")
	require.NoError(t, err)

	assert.NotContains(t, dt.snapshot(), "AWS::S3::Bucket",
		"Delete must NOT be called for DeletionPolicy=Retain")
}

// TestDeletionPolicy_Snapshot_SkipsDelete verifies that DeletionPolicy=Snapshot
// (not yet fully emulated) is treated like Retain and does not call Delete.
func TestDeletionPolicy_Snapshot_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	_, err := b.CreateStack(context.Background(), "snap-stack",
		templateWithDeletionPolicy("Snapshot"), nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	err = b.DeleteStack(context.Background(), "snap-stack")
	require.NoError(t, err)

	assert.NotContains(t, dt.snapshot(), "AWS::S3::Bucket",
		"Delete must NOT be called for DeletionPolicy=Snapshot")
}

// TestDeletionPolicy_Retain_OnUpdate_SkipsDelete verifies that when a resource
// with DeletionPolicy=Retain is removed from the template during UpdateStack,
// the underlying Delete call is skipped (while resources with no policy are deleted).
func TestDeletionPolicy_Retain_OnUpdate_SkipsDelete(t *testing.T) {
	t.Parallel()

	dt := &deleteTracker{}
	b := newTrackedBackend(dt)

	initial := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Bucket": {
				"Type": "AWS::S3::Bucket",
				"DeletionPolicy": "Retain",
				"Properties": {}
			},
			"Queue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {}
			}
		}
	}`

	// Update removes Bucket (Retain) and Queue (no policy). Adds Topic.
	updated := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Topic": {
				"Type": "AWS::SNS::Topic",
				"Properties": {}
			}
		}
	}`

	_, err := b.CreateStack(context.Background(), "upd-retain", initial, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	_, err = b.UpdateStack(context.Background(), "upd-retain", updated, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	deleted := dt.snapshot()
	assert.Contains(t, deleted, "AWS::SQS::Queue",
		"Queue (no DeletionPolicy) should be deleted on update")
	assert.NotContains(t, deleted, "AWS::S3::Bucket",
		"Bucket (DeletionPolicy=Retain) must NOT be deleted on update")
}

// TestDeletionPolicy_ParsedFromTemplate verifies DeletionPolicy is parsed from
// both JSON and YAML template bodies.
func TestDeletionPolicy_ParsedFromTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantPolicy string
	}{
		{
			name: "json_retain",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {
						"Type": "AWS::S3::Bucket",
						"DeletionPolicy": "Retain",
						"Properties": {}
					}
				}
			}`,
			wantPolicy: "Retain",
		},
		{
			name: "yaml_retain",
			body: `
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties: {}
`,
			wantPolicy: "Retain",
		},
		{
			name: "json_delete",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {
						"Type": "AWS::S3::Bucket",
						"DeletionPolicy": "Delete",
						"Properties": {}
					}
				}
			}`,
			wantPolicy: "Delete",
		},
		{
			name: "json_omitted",
			body: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
				}
			}`,
			wantPolicy: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tc.body)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPolicy, tmpl.Resources["Bucket"].DeletionPolicy)
		})
	}
}
