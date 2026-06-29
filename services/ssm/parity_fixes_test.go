package ssm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestPerInstanceMockKMSKey verifies that two InMemoryBackend instances use
// distinct encryption keys so that a SecureString encrypted by one backend
// cannot be decrypted by the other (key isolation).
func TestPerInstanceMockKMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "instances use distinct keys"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b1 := ssm.NewInMemoryBackend()
			b2 := ssm.NewInMemoryBackend()

			// Put a SecureString in backend 1.
			_, err := b1.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/sec/key",
				Type:  ssm.SecureStringType,
				Value: "top-secret",
			})
			require.NoError(t, err)

			// Retrieve encrypted storage value via ForceInsertParameter trick:
			// read back with WithDecryption=false to get the raw ciphertext.
			out, err := b1.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/key",
				WithDecryption: false,
			})
			require.NoError(t, err)
			rawCiphertext := out.Parameter.Value

			// Inject the ciphertext from b1 into b2 directly, bypassing encryption.
			b2.ForceInsertParameter(ssm.Parameter{
				Name:  "/sec/key",
				Type:  ssm.SecureStringType,
				Value: rawCiphertext,
			})

			// Decrypting with b2 must fail or return different plaintext because
			// b2 uses a different AES key than b1.
			out2, err := b2.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/key",
				WithDecryption: true,
			})
			if err == nil {
				// If no error, the decrypted value must not equal the original plaintext.
				assert.NotEqual(t, "top-secret", out2.Parameter.Value,
					"ciphertext from b1 must not decrypt successfully under b2's key")
			}
			// An error (e.g. auth tag mismatch) is also acceptable.
		})
	}
}

// TestPerInstanceMockKMSKey_SelfRoundTrip confirms that a single backend can
// encrypt and decrypt its own SecureString values correctly.
func TestPerInstanceMockKMSKey_SelfRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "short value", plaintext: "hello"},
		{name: "empty value", plaintext: ""},
		{name: "unicode value", plaintext: "こんにちは世界"},
		{name: "binary-like value", plaintext: "\x00\x01\x02\x03"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/sec/val",
				Type:  ssm.SecureStringType,
				Value: tc.plaintext,
			})
			require.NoError(t, err)

			out, err := b.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/val",
				WithDecryption: true,
			})
			require.NoError(t, err)
			require.Equal(t, tc.plaintext, out.Parameter.Value)
		})
	}
}

// TestCollectPathParams_LinearScan verifies GetParametersByPath works correctly
// using the linear-scan approach after removal of the sorted-slice index.
func TestCollectPathParams_LinearScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params    []string // parameter names to insert
		name      string
		path      string
		wantNames []string
		recursive bool
	}{
		{
			name:      "non-recursive picks direct children only",
			params:    []string{"/a/b", "/a/b/c", "/a/d"},
			path:      "/a/",
			recursive: false,
			wantNames: []string{"/a/b", "/a/d"},
		},
		{
			name:      "recursive picks all descendants",
			params:    []string{"/x/y", "/x/y/z", "/x/other"},
			path:      "/x/",
			recursive: true,
			wantNames: []string{"/x/other", "/x/y", "/x/y/z"},
		},
		{
			name:      "no match returns empty slice",
			params:    []string{"/a/b"},
			path:      "/z/",
			recursive: true,
			wantNames: []string{},
		},
		{
			name:      "results sorted by name",
			params:    []string{"/p/z", "/p/a", "/p/m"},
			path:      "/p/",
			recursive: false,
			wantNames: []string{"/p/a", "/p/m", "/p/z"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			for _, name := range tc.params {
				_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
					Name:  name,
					Type:  ssm.StringType,
					Value: "v",
				})
				require.NoError(t, err)
			}

			// Path without trailing slash is normalised by GetParametersByPath.
			path := tc.path
			if len(path) > 0 && path[len(path)-1] == '/' {
				path = path[:len(path)-1]
			}

			out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
				Path:      path,
				Recursive: tc.recursive,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Parameters))
			for _, p := range out.Parameters {
				got = append(got, p.Name)
			}

			if len(tc.wantNames) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, tc.wantNames, got)
			}
		})
	}
}

// TestDeleteParameter_RegionCleanup verifies that deleting the last parameter
// in a region removes the empty inner maps so they do not linger indefinitely.
func TestDeleteParameter_RegionCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleteVia  string // "single" or "batch"
		paramCount int
	}{
		{name: "single delete cleans region", deleteVia: "single", paramCount: 1},
		{name: "batch delete cleans region", deleteVia: "batch", paramCount: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			names := make([]string, tc.paramCount)
			for i := range tc.paramCount {
				name := "/cleanup/p"
				if tc.paramCount > 1 {
					name = "/cleanup/p" + string(rune('0'+i))
				}
				names[i] = name
				_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
					Name:  name,
					Type:  ssm.StringType,
					Value: "v",
				})
				require.NoError(t, err)
			}

			switch tc.deleteVia {
			case "single":
				_, err := b.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: names[0]})
				require.NoError(t, err)
			case "batch":
				_, err := b.DeleteParameters(ctx, &ssm.DeleteParametersInput{Names: names})
				require.NoError(t, err)
			}

			// After deleting all params, no tag entry should survive either.
			for _, name := range names {
				assert.False(t, b.HasTagEntry(name),
					"tag entry must be cleaned up after DeleteParameter")
			}

			// HistoryLen must be zero — the history inner map entry was cleaned up.
			for _, name := range names {
				assert.Equal(t, 0, b.HistoryLen(name))
			}
		})
	}
}

// TestSendCommand_NoWritePathExpiry verifies that SendCommand no longer prunes
// expired commands inline. Expiry is deferred to the janitor sweep.
func TestSendCommand_NoWritePathExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expireN int
	}{
		{name: "expired commands survive SendCommand", expireN: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			ids := make([]string, tc.expireN)
			for i := range tc.expireN {
				out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
					DocumentName: "AWS-RunShellScript",
					InstanceIDs:  []string{"i-abc"},
				})
				require.NoError(t, err)
				ids[i] = out.Command.CommandID
			}

			// Force commands into the past.
			for _, id := range ids {
				b.SetCommandExpiresAfter(id, 1)
			}

			// New SendCommand must not prune the expired ones.
			_, err := b.SendCommand(ctx, &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-def"},
			})
			require.NoError(t, err)

			require.Equal(t, tc.expireN+1, b.CommandCount(),
				"SendCommand must not prune on the write path")

			// Janitor sweeps them.
			j := ssm.NewJanitor(b, 0)
			j.SweepOnce(ctx)

			require.Equal(t, 1, b.CommandCount(), "janitor must evict expired commands")
		})
	}
}

// TestGetMaintenanceWindowExecution_FullOutput verifies that the three MW
// execution Get operations return timing and status fields populated from
// the stored window and task data (not just a bare Status field).
func TestGetMaintenanceWindowExecution_FullOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration int32
	}{
		{name: "1-hour window", duration: 1},
		{name: "4-hour window", duration: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			win, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
				Name:     "test-win",
				Schedule: "rate(7 days)",
				Duration: tc.duration,
			})
			require.NoError(t, err)

			execID := "mwexec-" + win.WindowID

			out, err := b.GetMaintenanceWindowExecution(
				ctx,
				&ssm.GetMaintenanceWindowExecutionInput{
					WindowID:          win.WindowID,
					WindowExecutionID: execID,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, win.WindowID, out.WindowID)
			assert.Equal(t, execID, out.WindowExecutionID)
			assert.Equal(t, "Success", out.Status)
			assert.NotEmpty(t, out.StatusDetails)
			assert.False(t, out.StartTime.IsZero(), "StartTime must be populated")
			assert.NotNil(t, out.EndTime, "EndTime must be populated")

			taskOut, err := b.GetMaintenanceWindowExecutionTask(
				ctx,
				&ssm.GetMaintenanceWindowExecutionTaskInput{
					WindowExecutionID: execID,
					TaskExecutionID:   "taskexec-some-task",
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "Success", taskOut.Status)
			assert.NotEmpty(t, taskOut.StatusDetails)
			assert.False(t, taskOut.StartTime.IsZero())

			invOut, err := b.GetMaintenanceWindowExecutionTaskInvocation(
				ctx,
				&ssm.GetMaintenanceWindowExecutionTaskInvocationInput{
					WindowExecutionID: execID,
					TaskExecutionID:   "taskexec-some-task",
					InvocationID:      "inv-001",
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "Success", invOut.Status)
			assert.False(t, invOut.StartTime.IsZero())
		})
	}
}

// TestOtherMapsRegionCleanup verifies that delete operations on resources
// other than parameters also clean up their per-region inner maps.
func TestOtherMapsRegionCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string // which resource type to create and delete
	}{
		{name: "activation cleanup", resource: "activation"},
		{name: "patch baseline cleanup", resource: "patchbaseline"},
		{name: "maintenance window cleanup", resource: "maintenancewindow"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			switch tc.resource {
			case "activation":
				act, err := b.CreateActivation(ctx, &ssm.CreateActivationInput{
					IamRole:           "arn:aws:iam::000000000000:role/SSMRole",
					RegistrationLimit: 1,
				})
				require.NoError(t, err)
				_, err = b.DeleteActivation(
					ctx,
					&ssm.DeleteActivationInput{ActivationID: act.ActivationID},
				)
				require.NoError(t, err)
				assert.Zero(t, b.ActivationCount(), "activation map must be cleaned up")

			case "patchbaseline":
				pb, err := b.CreatePatchBaseline(
					ctx,
					&ssm.CreatePatchBaselineInput{Name: "test-baseline"},
				)
				require.NoError(t, err)
				_, err = b.DeletePatchBaseline(
					ctx,
					&ssm.DeletePatchBaselineInput{BaselineID: pb.BaselineID},
				)
				require.NoError(t, err)
				assert.Zero(t, b.PatchBaselineCount(), "patch baseline map must be cleaned up")

			case "maintenancewindow":
				win, err := b.CreateMaintenanceWindow(ctx, &ssm.CreateMaintenanceWindowInput{
					Name: "test-win", Schedule: "rate(7 days)", Duration: 1,
				})
				require.NoError(t, err)
				_, err = b.DeleteMaintenanceWindow(
					ctx,
					&ssm.DeleteMaintenanceWindowInput{WindowID: win.WindowID},
				)
				require.NoError(t, err)
				assert.Zero(
					t,
					b.MaintenanceWindowCount(),
					"maintenance window map must be cleaned up",
				)
			}
		})
	}
}
