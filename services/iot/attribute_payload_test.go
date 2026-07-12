package iot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// Test_UpdateThing_AttributePayloadMergeSemantics asserts UpdateThing's
// AttributePayload.merge behaves like real AWS IoT: unset/false REPLACES the
// stored attributes with the payload's attributes, true MERGES them, and in
// both modes an attribute given an empty string value is removed from the
// result (AWS's documented "how to delete an attribute" mechanism).
func Test_UpdateThing_AttributePayloadMergeSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  *iot.AttributePayload
		existing map[string]string
		want     map[string]string
		name     string
	}{
		{
			name:     "merge_unset_replaces_existing_attributes",
			existing: map[string]string{"keep": "no", "location": "lab"},
			payload: &iot.AttributePayload{
				Attributes: map[string]string{"new": "value"},
			},
			want: map[string]string{"new": "value"},
		},
		{
			name:     "merge_explicit_false_replaces_existing_attributes",
			existing: map[string]string{"keep": "no", "location": "lab"},
			payload: &iot.AttributePayload{
				Attributes: map[string]string{"new": "value"},
				Merge:      new(false),
			},
			want: map[string]string{"new": "value"},
		},
		{
			name:     "merge_true_merges_into_existing_attributes",
			existing: map[string]string{"keep": "yes", "location": "lab"},
			payload: &iot.AttributePayload{
				Attributes: map[string]string{"new": "value"},
				Merge:      new(true),
			},
			want: map[string]string{"keep": "yes", "location": "lab", "new": "value"},
		},
		{
			name:     "empty_value_removes_attribute_in_merge_mode",
			existing: map[string]string{"keep": "yes", "drop": "me"},
			payload: &iot.AttributePayload{
				Attributes: map[string]string{"drop": ""},
				Merge:      new(true),
			},
			want: map[string]string{"keep": "yes"},
		},
		{
			name:     "empty_value_dropped_in_replace_mode",
			existing: map[string]string{"keep": "yes"},
			payload: &iot.AttributePayload{
				Attributes: map[string]string{"keep": "yes", "drop": ""},
			},
			want: map[string]string{"keep": "yes"},
		},
		{
			name:     "nil_attributes_leaves_existing_untouched",
			existing: map[string]string{"keep": "yes"},
			payload: &iot.AttributePayload{
				Merge: new(true),
			},
			want: map[string]string{"keep": "yes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{
				ThingName:        "thing-1",
				AttributePayload: &iot.AttributePayload{Attributes: tt.existing},
			})
			require.NoError(t, err)

			err = b.UpdateThing(&iot.UpdateThingInput{
				ThingName:        "thing-1",
				AttributePayload: tt.payload,
			})
			require.NoError(t, err)

			described, err := b.DescribeThing("thing-1")
			require.NoError(t, err)
			assert.Equal(t, tt.want, described.Attributes)
		})
	}
}

// Test_UpdateThingGroup_AttributePayloadMergeSemantics mirrors
// Test_UpdateThing_AttributePayloadMergeSemantics for UpdateThingGroup, whose
// AttributePayload.merge field carries the identical documented semantics.
func Test_UpdateThingGroup_AttributePayloadMergeSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		merge    *bool
		attrs    map[string]string
		existing map[string]string
		want     map[string]string
		name     string
	}{
		{
			name:     "merge_unset_replaces_existing_attributes",
			existing: map[string]string{"region": "us-east-1"},
			attrs:    map[string]string{"new": "value"},
			want:     map[string]string{"new": "value"},
		},
		{
			name:     "merge_true_merges_into_existing_attributes",
			existing: map[string]string{"region": "us-east-1"},
			attrs:    map[string]string{"new": "value"},
			merge:    new(true),
			want:     map[string]string{"region": "us-east-1", "new": "value"},
		},
		{
			name:     "empty_value_removes_attribute_in_merge_mode",
			existing: map[string]string{"region": "us-east-1", "drop": "me"},
			attrs:    map[string]string{"drop": ""},
			merge:    new(true),
			want:     map[string]string{"region": "us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{
				ThingGroupName: "group-1",
				Attributes:     tt.existing,
			})
			require.NoError(t, err)

			_, err = b.UpdateThingGroup(&iot.UpdateThingGroupInput{
				ThingGroupName: "group-1",
				Attributes:     tt.attrs,
				Merge:          tt.merge,
			})
			require.NoError(t, err)

			described, err := b.DescribeThingGroup("group-1")
			require.NoError(t, err)
			assert.Equal(t, tt.want, described.Attributes)
		})
	}
}

// Test_CreateThing_BillingGroupName asserts CreateThing wires an optional
// billingGroupName through to the thing so a subsequent DescribeThing
// reflects it, matching real AWS IoT's CreateThingInput.BillingGroupName.
func Test_CreateThing_BillingGroupName(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{
		ThingName:        "thing-1",
		BillingGroupName: "my-billing-group",
	})
	require.NoError(t, err)

	described, err := b.DescribeThing("thing-1")
	require.NoError(t, err)
	assert.Equal(t, "my-billing-group", described.BillingGroupName)
}

// Test_DescribeThing_BillingGroupName_ReflectsAddThingToBillingGroup asserts
// that DescribeThing reflects billing-group membership assigned via the
// separate AddThingToBillingGroup operation (the common real-world path),
// not just membership set at CreateThing time.
func Test_DescribeThing_BillingGroupName_ReflectsAddThingToBillingGroup(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "thing-1"})
	require.NoError(t, err)

	described, err := b.DescribeThing("thing-1")
	require.NoError(t, err)
	assert.Empty(t, described.BillingGroupName)

	err = b.AddThingToBillingGroup(&iot.AddThingToBillingGroupInput{
		ThingName:        "thing-1",
		BillingGroupName: "later-group",
	})
	require.NoError(t, err)

	described, err = b.DescribeThing("thing-1")
	require.NoError(t, err)
	assert.Equal(t, "later-group", described.BillingGroupName)
}

// Test_AttachOps_AreIdempotent asserts attaching the same
// policy/principal/target twice does not produce duplicate entries in the
// corresponding list operation, matching AWS IoT's set (not list) attachment
// semantics.
func Test_AttachOps_AreIdempotent(t *testing.T) {
	t.Parallel()

	t.Run("AttachPolicy", func(t *testing.T) {
		t.Parallel()

		b := iot.NewInMemoryBackend()
		_, err := b.CreatePolicy(&iot.CreatePolicyInput{
			PolicyName:     "policy-1",
			PolicyDocument: "{}",
		})
		require.NoError(t, err)

		for range 2 {
			attachErr := b.AttachPolicy(&iot.AttachPolicyInput{PolicyName: "policy-1", Target: "cert-arn"})
			require.NoError(t, attachErr)
		}

		policies, err := b.ListAttachedPolicies(&iot.ListAttachedPoliciesInput{Target: "cert-arn"})
		require.NoError(t, err)
		assert.Len(t, policies, 1)
	})

	t.Run("AttachPrincipalPolicy", func(t *testing.T) {
		t.Parallel()

		b := iot.NewInMemoryBackend()
		_, err := b.CreatePolicy(&iot.CreatePolicyInput{
			PolicyName:     "policy-1",
			PolicyDocument: "{}",
		})
		require.NoError(t, err)

		for range 2 {
			attachErr := b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
				PolicyName: "policy-1",
				Principal:  "cert-arn",
			})
			require.NoError(t, attachErr)
		}

		policies := b.ListPrincipalPolicies("cert-arn")
		assert.Len(t, policies, 1)
	})

	t.Run("AttachThingPrincipal", func(t *testing.T) {
		t.Parallel()

		b := iot.NewInMemoryBackend()
		_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "thing-1"})
		require.NoError(t, err)

		for range 2 {
			attachErr := b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
				ThingName: "thing-1",
				Principal: "cert-arn",
			})
			require.NoError(t, attachErr)
		}

		principals, err := b.ListThingPrincipals("thing-1")
		require.NoError(t, err)
		assert.Len(t, principals, 1)
	})
}
