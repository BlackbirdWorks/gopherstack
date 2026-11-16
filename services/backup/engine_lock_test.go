package backup_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

type lockProbeStacks struct {
	b            *backup.InMemoryBackend
	describeHeld bool
	createHeld   bool
}

func (f *lockProbeStacks) DescribeStack(string) (backup.StackInfo, bool) {
	f.describeHeld = f.b.LockHeld()

	return backup.StackInfo{Name: "s", Template: "{}"}, true
}

func (f *lockProbeStacks) CreateStack(context.Context, backup.StackInfo) (string, error) {
	f.createHeld = f.b.LockHeld()

	return "stack-id", nil
}

func TestEngines_CallOtherBackendsWithoutBackupLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cloudformation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			probe := &lockProbeStacks{b: b}
			b.SetStackService(probe)

			v := mustVault(t, b, "vault")
			stackArn := "arn:aws:cloudformation:us-east-1:123456789012:stack/s/abc"
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, stackArn, "CloudFormation")

			job, err := b.StartRestoreJob(rp, testRole, "CloudFormation", map[string]string{"stackName": "r"})
			require.NoError(t, err)

			assert.Equal(t, "COMPLETED", job.Status)
			assert.False(t, probe.describeHeld, "capture ran under the backup lock")
			assert.False(t, probe.createHeld, "restore ran under the backup lock")
		})
	}
}

type fakeFileSystems struct{ created []backup.FileSystemInfo }

func (f *fakeFileSystems) DescribeFileSystem(context.Context, string) (backup.FileSystemInfo, bool) {
	return backup.FileSystemInfo{PerformanceMode: "generalPurpose", ThroughputMode: "bursting"}, true
}

func (f *fakeFileSystems) CreateFileSystem(_ context.Context, info backup.FileSystemInfo) (string, error) {
	f.created = append(f.created, info)

	return "fs-new", nil
}

func TestEFSRestore_DocumentedMetadataKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		md       map[string]string
		name     string
		wantPerf string
		wantKMS  string
	}{
		{
			name: "pascal", md: map[string]string{"PerformanceMode": "maxIO", "KmsKeyId": "k1"},
			wantPerf: "maxIO", wantKMS: "k1",
		},
		{name: "camel", md: map[string]string{"performanceMode": "maxIO"}, wantPerf: "maxIO"},
		{name: "absent", md: map[string]string{"newFileSystem": "true"}, wantPerf: "generalPurpose"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			fs := &fakeFileSystems{}
			b.SetFileSystemService(fs, "us-east-1", "123456789012")

			v := mustVault(t, b, "vault")
			fsArn := "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-1"
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, fsArn, "EFS")

			job, err := b.StartRestoreJob(rp, testRole, "EFS", tt.md)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", job.Status)
			require.Len(t, fs.created, 1)

			assert.Equal(t, tt.wantPerf, fs.created[0].PerformanceMode)
			assert.Equal(t, tt.wantKMS, fs.created[0].KmsKeyID)
		})
	}
}
