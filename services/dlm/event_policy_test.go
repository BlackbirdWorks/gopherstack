package dlm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

func eventPolicyDetails(owner, regex, target string) *dlmtypes.PolicyDetails {
	return &dlmtypes.PolicyDetails{
		PolicyType: dlmtypes.PolicyTypeValuesEventBasedPolicy,
		EventSource: &dlmtypes.EventSource{
			Type: dlmtypes.EventSourceValuesManagedCwe,
			Parameters: &dlmtypes.EventParameters{
				EventType:        dlmtypes.EventTypeValuesShareSnapshot,
				SnapshotOwner:    []string{owner},
				DescriptionRegex: aws.String(regex),
			},
		},
		Actions: []dlmtypes.Action{{
			Name: aws.String("copy"),
			CrossRegionCopy: []dlmtypes.CrossRegionCopyAction{{
				Target:                  aws.String(target),
				EncryptionConfiguration: &dlmtypes.EncryptionConfiguration{Encrypted: aws.Bool(false)},
				RetainRule: &dlmtypes.CrossRegionCopyRetainRule{
					Interval: aws.Int32(2), IntervalUnit: dlmtypes.RetentionIntervalUnitValuesDays,
				},
			}},
		}},
	}
}

func TestEventBasedPolicyCopiesSharedSnapshot(t *testing.T) {
	t.Parallel()

	const (
		owner  = "111111111111"
		target = "eu-west-1"
	)

	tests := []struct {
		name      string
		state     dlmtypes.SettablePolicyStateValues
		regex     string
		policyOwn string
		shareWith string
		wantCopy  bool
	}{
		{"matching", dlmtypes.SettablePolicyStateValuesEnabled, "^Share.*$", owner, "000000000000", true},
		{"regex_miss", dlmtypes.SettablePolicyStateValuesEnabled, "^Nope$", owner, "000000000000", false},
		{"owner_miss", dlmtypes.SettablePolicyStateValuesEnabled, "^Share.*$", "222222222222", "000000000000", false},
		{"disabled", dlmtypes.SettablePolicyStateValuesDisabled, "^Share.*$", owner, "000000000000", false},
		{"other_recipient", dlmtypes.SettablePolicyStateValuesEnabled, "^Share.*$", owner, "333333333333", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newExecEnv(t)
			policyID, _ := e.policy(t, tc.state, eventPolicyDetails(tc.policyOwn, tc.regex, target))

			vol := e.volume(t, nil)
			snap, err := e.ec2.CreateSnapshot(vol, "Share me")
			require.NoError(t, err)
			e.ec2.ClaimSnapshotOwner(owner, snap.SnapshotID)

			_, err = e.ec2Client.ModifySnapshotAttribute(t.Context(), &ec2sdk.ModifySnapshotAttributeInput{
				SnapshotId:    aws.String(snap.SnapshotID),
				Attribute:     ec2types.SnapshotAttributeNameCreateVolumePermission,
				OperationType: ec2types.OperationTypeAdd,
				UserIds:       []string{tc.shareWith},
			})
			require.NoError(t, err)

			copies, err := e.ec2H.BackendFor(target).DescribeSnapshots(nil)
			require.NoError(t, err)

			if !tc.wantCopy {
				assert.Empty(t, copies)

				return
			}

			require.Len(t, copies, 1)
			assert.Equal(t, policyID, e.ec2H.BackendFor(target).TagsForResource(copies[0].SnapshotID)[tagPolicy])

			e.dlmBk.RunSchedules(t.Context(), time.Now().Add(3*24*time.Hour))

			copies, err = e.ec2H.BackendFor(target).DescribeSnapshots(nil)
			require.NoError(t, err)
			assert.Empty(t, copies)
		})
	}
}

func TestEventBasedPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regex   string
		wantErr bool
	}{
		{"ok", "^a$", false},
		{"bad_regex", "(", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newExecEnv(t)
			_, err := e.dlmClient.CreateLifecyclePolicy(t.Context(), &dlmsdk.CreateLifecyclePolicyInput{
				Description: aws.String("e"), ExecutionRoleArn: aws.String(execRole),
				State:         dlmtypes.SettablePolicyStateValuesEnabled,
				PolicyDetails: eventPolicyDetails("111111111111", tc.regex, "eu-west-1"),
			})

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

const tagPolicy = "aws:dlm:lifecycle-policy-id"

type deleteOnCopy struct {
	dlm.ComputeService

	onCopy func()
}

func (d deleteOnCopy) CopySnapshot(
	src, dst, id, desc string, enc bool, key string, tags map[string]string,
) (string, error) {
	d.onCopy()

	return d.ComputeService.CopySnapshot(src, dst, id, desc, enc, key, tags)
}

func TestEventPolicyDeletedMidFlightTracksNothing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleteIt   bool
		wantTagged int
	}{
		{"kept", false, 1},
		{"deleted_during_copy", true, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newExecEnv(t)
			policyID, _ := e.policy(t, dlmtypes.SettablePolicyStateValuesEnabled,
				eventPolicyDetails("111111111111", "^Share.*$", "eu-west-1"))

			e.dlmBk.SetCompute(deleteOnCopy{
				ComputeService: dlm.NewEC2Compute(e.ec2H),
				onCopy: func() {
					if tc.deleteIt {
						require.NoError(t, e.dlmBk.DeleteLifecyclePolicy(policyID))
					}
				},
			})

			vol := e.volume(t, nil)
			snap, err := e.ec2.CreateSnapshot(vol, "Share me")
			require.NoError(t, err)
			e.ec2.ClaimSnapshotOwner("111111111111", snap.SnapshotID)

			e.dlmBk.HandleSnapshotShared(t.Context(), time.Now(), ec2backend.SnapshotSharedEvent{
				Region: "us-east-1", SnapshotID: snap.SnapshotID, OwnerID: "111111111111",
				Description: "Share me", SharedWith: []string{"000000000000"},
			})

			assert.Equal(t, tc.wantTagged, dlm.ManagedCount(e.dlmBk))
		})
	}
}
