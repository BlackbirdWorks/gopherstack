package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- EBS volume lifecycle ---- //nolint:godot // existing issue.
func TestModifyVolume(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, setupErr)

	t.Run("resizes volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mod, err := b.ModifyVolume(vol.ID, "gp3", 40, 3000)
		require.NoError(t, err)
		assert.Equal(t, vol.ID, mod.VolumeID)
		assert.Equal(t, "completed", mod.ModificationState)
		assert.Equal(t, "gp3", mod.TargetVolumeType)
		assert.Equal(t, 40, mod.TargetSize)
		assert.Equal(t, int64(100), mod.Progress)
	})

	t.Run("unknown volume returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVolume("vol-doesnotexist", "", 0, 0)
		require.Error(t, err)
	})

	t.Run("empty volume ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVolume("", "gp3", 0, 0)
		require.Error(t, err)
	})
}

func TestDescribeVolumeStatus(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, setupErr)

	t.Run("returns ok status for known volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus([]string{vol.ID})
		require.Len(t, items, 1)
		assert.Equal(t, vol.ID, items[0].VolumeID)
		assert.Equal(t, "ok", items[0].VolumeStatus)
	})

	t.Run("returns all volumes when no filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus(nil)
		assert.NotEmpty(t, items)
	})

	t.Run("filters to known IDs only", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus([]string{"vol-missing"})
		assert.Empty(t, items)
	})
}

func TestDescribeVolumesModifications(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20, "")
	_, _ = b.ModifyVolume(vol.ID, "gp3", 40, 0)

	t.Run("returns modification after ModifyVolume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mods := b.DescribeVolumesModifications([]string{vol.ID})
		require.Len(t, mods, 1)
		assert.Equal(t, "completed", mods[0].ModificationState)
	})

	t.Run("returns empty when no modifications", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vol2, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
		mods := b.DescribeVolumesModifications([]string{vol2.ID})
		assert.Empty(t, mods)
	})
}

// ---- HTTP dispatch integration tests ---- //nolint:godot // existing issue.
func TestHTTP_ModifyVolume(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := ec2.NewHandler(b)

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20, "")

	_, err := ec2.ExportDispatch(h2, url.Values{
		"Action":     []string{"ModifyVolume"},
		"VolumeId":   []string{vol.ID},
		"VolumeType": []string{"gp3"},
		"Size":       []string{"40"},
	})
	require.NoError(t, err)
	_ = h
}

func TestHTTP_DescribeVolumeStatus(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeVolumeStatus"},
	})
	require.NoError(t, err)
}

// ---- EBS encryption ---- //nolint:godot // existing issue.
func TestEbsEncryption(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is false", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.False(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("enable sets true", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.EnableEbsEncryptionByDefault()
		assert.True(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("disable sets false", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DisableEbsEncryptionByDefault()
		assert.False(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("default KMS key is alias/aws/ebs", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Equal(t, "alias/aws/ebs", b.GetEbsDefaultKmsKeyID())
	})

	t.Run("modify KMS key ID", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyEbsDefaultKmsKeyID("alias/my-key"))
		assert.Equal(t, "alias/my-key", b.GetEbsDefaultKmsKeyID())
	})

	t.Run("empty KMS key returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyEbsDefaultKmsKeyID(""))
	})
}

func TestEnableVolumeIO(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, setupErr)

	t.Run("enables IO for known volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableVolumeIO(vol.ID))
	})

	t.Run("unknown volume returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.EnableVolumeIO("vol-missing"))
	})
}

// ---- Snapshot locking ---- //nolint:godot // existing issue.

// ---- CopyVolumes ---- //nolint:godot // existing issue.
func TestCopyVolumes(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	v1, _ := b.CreateVolume("us-east-1a", "gp2", 10, "")
	v2, _ := b.CreateVolume("us-east-1a", "gp3", 20, "")

	t.Run("creates copies", func(t *testing.T) { //nolint:paralleltest // existing issue.
		results, err := b.CopyVolumes([]string{v1.ID, v2.ID}, "us-west-2")
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.NotEqual(t, v1.ID, results[0].DestVolumeID)
	})

	t.Run("empty list returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CopyVolumes(nil, "us-west-2")
		require.Error(t, err)
	})
}

// ---- VPC CIDR disassociation ---- //nolint:godot // existing issue.

// ---- Replace root volume tasks ---- //nolint:godot // existing issue.
func TestReplaceRootVolumeTasks(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	var taskID string

	t.Run("create task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.CreateReplaceRootVolumeTask(inst.ID, "")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ReplaceRootVolumeTaskID)
		assert.Equal(t, "completed", task.TaskState)
		taskID = task.ReplaceRootVolumeTaskID
	})

	t.Run("describe returns task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tasks := b.DescribeReplaceRootVolumeTasks([]string{taskID})
		require.Len(t, tasks, 1)
		assert.Equal(t, inst.ID, tasks[0].InstanceID)
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateReplaceRootVolumeTask("i-missing", "")
		require.Error(t, err)
	})
}

// ---- Address transfers ---- //nolint:godot // existing issue.

// ---- HTTP dispatch tests ---- //nolint:godot // existing issue.
func TestHTTP_GetEbsEncryptionByDefault(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetEbsEncryptionByDefault"},
	})
	require.NoError(t, err)
}

// TestEnableEbsEncryptionByDefault_ResponseField verifies that
// EnableEbsEncryptionByDefault returns an <ebsEncryptionByDefault>true</ebsEncryptionByDefault>
// field, not <return>true</return>. Previously the handler used a stubResponse which
// produced the wrong field name, causing AWS SDK deserialisation to fail.
func TestEnableEbsEncryptionByDefault_ResponseField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		wantField   string
		wantMissing string
	}{
		{
			name:        "enable_returns_ebsEncryptionByDefault_true",
			action:      "EnableEbsEncryptionByDefault",
			wantField:   "<ebsEncryptionByDefault>true</ebsEncryptionByDefault>",
			wantMissing: "<return>",
		},
		{
			name:        "disable_returns_ebsEncryptionByDefault_false",
			action:      "DisableEbsEncryptionByDefault",
			wantField:   "<ebsEncryptionByDefault>false</ebsEncryptionByDefault>",
			wantMissing: "<return>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action": {tt.action},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, tt.wantField,
				"response must contain %s", tt.wantField)
			assert.NotContains(t, resp, tt.wantMissing,
				"response must not contain %s", tt.wantMissing)
		})
	}
}

// TestEnableEbsEncryptionByDefault_RootElement verifies that
// Enable/DisableEbsEncryptionByDefault return the correct XML root element names.

// TestEnableEbsEncryptionByDefault_RootElement verifies that
// Enable/DisableEbsEncryptionByDefault return the correct XML root element names.
func TestEnableEbsEncryptionByDefault_RootElement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantXML string
	}{
		{
			name:    "enable_root_element",
			action:  "EnableEbsEncryptionByDefault",
			wantXML: "EnableEbsEncryptionByDefaultResponse",
		},
		{
			name:    "disable_root_element",
			action:  "DisableEbsEncryptionByDefault",
			wantXML: "DisableEbsEncryptionByDefaultResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action": {tt.action},
			})
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.80s", tt.wantXML, resp)
		})
	}
}

// TestImageBlockPublicAccess_ResponseState verifies that
// EnableImageBlockPublicAccess returns the new state in an <imageBlockPublicAccessState>
// element rather than <return>true</return>, matching AWS EC2 behaviour.
// Similarly DisableImageBlockPublicAccess must return <state>unblocked</state>.

// TestModifyEbsDefaultKmsKeyId_ResponseXMLName verifies that
// ModifyEbsDefaultKmsKeyId returns a response with root element
// "ModifyEbsDefaultKmsKeyIdResponse", not "GetEbsDefaultKmsKeyIdResponse".
// The wrong element name causes AWS SDK deserialisation to fail.
func TestModifyEbsDefaultKmsKeyId_ResponseXMLName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kmsKeyID string
		wantXML  string
		wantErr  bool
	}{
		{
			name:     "valid_key_returns_modify_response_element",
			kmsKeyID: "alias/my-custom-key",
			wantXML:  "ModifyEbsDefaultKmsKeyIdResponse",
		},
		{
			name:     "another_key_alias_returns_modify_response_element",
			kmsKeyID: "arn:aws:kms:us-east-1:000000000000:key/abc-123",
			wantXML:  "ModifyEbsDefaultKmsKeyIdResponse",
		},
		{
			name:    "empty_key_id_returns_error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp, err := ec2.ExportDispatch(h, url.Values{
				"Action":   {"ModifyEbsDefaultKmsKeyId"},
				"KmsKeyId": {tt.kmsKeyID},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.60s", tt.wantXML, resp)
		})
	}
}

// TestGetDefaultCreditSpecification_EchoesInstanceFamily verifies that
// GetDefaultCreditSpecification and ModifyDefaultCreditSpecification echo the requested
// InstanceFamily back in the response, matching AWS EC2 behaviour.
// Previously both handlers hardcoded "t3" regardless of the request parameter.
