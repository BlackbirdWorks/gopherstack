package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

const (
	testRole      = "arn:aws:iam::123456789012:role/r"
	testVolumeArn = "arn:aws:ec2:us-east-1:123456789012:volume/vol-1"
)

var (
	errCapacity = errors.New("InsufficientVolumeCapacity")
	errDenied   = errors.New("AccessDenied")
)

type fakeVolumes struct {
	createErr error
	created   []backup.VolumeInfo
}

func (f *fakeVolumes) DescribeVolume(id string) (backup.VolumeInfo, bool) {
	if id != "vol-1" {
		return backup.VolumeInfo{}, false
	}

	return backup.VolumeInfo{AZ: "us-east-1a", VolumeType: "gp3", Size: 8}, true
}

func (f *fakeVolumes) CreateVolume(info backup.VolumeInfo) (string, error) {
	if f.createErr != nil {
		return "", f.createErr
	}
	f.created = append(f.created, info)

	return "vol-restored", nil
}

type fakeObjects struct {
	putErr  error
	objects map[string][]byte
	mu      sync.Mutex
}

func newFakeObjects() *fakeObjects { return &fakeObjects{objects: map[string][]byte{}} }

func (f *fakeObjects) HeadBucket(context.Context, *sdk_s3.HeadBucketInput) (*sdk_s3.HeadBucketOutput, error) {
	return &sdk_s3.HeadBucketOutput{}, nil
}

func (f *fakeObjects) CreateBucket(context.Context, *sdk_s3.CreateBucketInput) (*sdk_s3.CreateBucketOutput, error) {
	return &sdk_s3.CreateBucketOutput{}, nil
}

func (f *fakeObjects) PutObject(_ context.Context, in *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	body, _ := io.ReadAll(in.Body)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = body

	return &sdk_s3.PutObjectOutput{}, nil
}

func (f *fakeObjects) GetObject(_ context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &sdk_s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)])),
	}, nil
}

func (f *fakeObjects) ListObjectsV2(
	_ context.Context, in *sdk_s3.ListObjectsV2Input,
) (*sdk_s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	prefix := aws.ToString(in.Bucket) + "/"
	var keys []string
	for k := range f.objects {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k[len(prefix):])
		}
	}
	sort.Strings(keys)

	out := &sdk_s3.ListObjectsV2Output{}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k)})
	}

	return out, nil
}

func (f *fakeObjects) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

func completedRecoveryPoint(t *testing.T, b *backup.InMemoryBackend, vault, resourceArn, resourceType string) string {
	t.Helper()

	job, err := b.StartBackupJob(vault, resourceArn, testRole, resourceType, nil, 0)
	require.NoError(t, err)
	require.NoError(t, b.CompleteBackupJob(job.BackupJobID))

	done, err := b.DescribeBackupJob(job.BackupJobID)
	require.NoError(t, err)

	return done.RecoveryPointArn
}

func TestRestoreJob_EBSVolumeRecreated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createErr  error
		name       string
		wantStatus string
		wantArn    string
		wantMsg    string
		metadata   map[string]string
		wantAZ     string
	}{
		{
			name: "recreated", metadata: map[string]string{"volumeType": "io1"}, wantStatus: "COMPLETED",
			wantArn: "arn:aws:ec2:us-east-1:123456789012:volume/vol-restored", wantAZ: "us-east-1a",
		},
		{
			name: "metadata_az_override", metadata: map[string]string{"availabilityZone": "us-east-1b"},
			wantStatus: "COMPLETED", wantArn: "arn:aws:ec2:us-east-1:123456789012:volume/vol-restored",
			wantAZ: "us-east-1b",
		},
		{
			name: "ec2_failure_fails_job", metadata: map[string]string{"volumeType": "gp3"},
			createErr: errCapacity, wantStatus: "FAILED",
			wantMsg: "InsufficientVolumeCapacity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			vols := &fakeVolumes{createErr: tt.createErr}
			b.SetVolumeService(vols, "us-east-1", "123456789012")
			v := mustVault(t, b, "vault")
			rp := completedRecoveryPoint(t, b, v.BackupVaultName, testVolumeArn, "EBS")

			job, err := b.StartRestoreJob(rp, testRole, "EBS", tt.metadata)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, job.Status)
			assert.Equal(t, tt.wantArn, job.CreatedResourceArn)
			assert.Contains(t, job.StatusMessage, tt.wantMsg)

			if tt.wantAZ != "" {
				require.Len(t, vols.created, 1)
				assert.Equal(t, tt.wantAZ, vols.created[0].AZ)
			}
		})
	}
}

func TestRestoreJob_S3BucketRecreated(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	store := newFakeObjects()
	store.objects["src/a.txt"] = []byte("alpha")
	b.SetObjectStore(store)

	v := mustVault(t, b, "vault")
	rp := completedRecoveryPoint(t, b, v.BackupVaultName, "arn:aws:s3:::src", "S3")

	delete(store.objects, "src/a.txt")

	job, err := b.StartRestoreJob(rp, testRole, "S3", map[string]string{"DestinationBucketName": "dst"})
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:s3:::dst", job.CreatedResourceArn)
	assert.Equal(t, []string{"dst/a.txt"}, store.keys())
}

func TestJobs_LifecycleDelay(t *testing.T) {
	t.Parallel()

	type probe struct {
		restore, copy string
	}

	tests := []struct {
		name  string
		want  []probe
		delay time.Duration
	}{
		{name: "zero_is_synchronous", delay: 0, want: []probe{{"COMPLETED", "COMPLETED"}}},
		{
			name: "delayed", delay: 10 * time.Second,
			want: []probe{{"PENDING", "CREATED"}, {"RUNNING", "RUNNING"}, {"COMPLETED", "COMPLETED"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend(t)
				b.SetLifecycleDelay(tt.delay)
				src := mustVault(t, b, "src")
				dst := mustVault(t, b, "dst")
				rp := completedRecoveryPoint(t, b, src.BackupVaultName, testVolumeArn, "EBS")

				rj, err := b.StartRestoreJob(rp, testRole, "EBS", map[string]string{"k": "v"})
				require.NoError(t, err)
				cj, err := b.StartCopyJob(rp, src.BackupVaultName, dst.BackupVaultArn, testRole)
				require.NoError(t, err)

				steps := []time.Duration{0, 6 * time.Second, 5 * time.Second}
				for i, want := range tt.want {
					if i > 0 {
						time.Sleep(steps[i])
					}

					gotR, descErr := b.DescribeRestoreJob(rj.RestoreJobID)
					require.NoError(t, descErr)
					gotC, descErr := b.DescribeCopyJob(cj.CopyJobID)
					require.NoError(t, descErr)

					assert.Equal(t, want.restore, gotR.Status, "restore step %d", i)
					assert.Equal(t, want.copy, gotC.State, "copy step %d", i)

					settled := want.restore == "COMPLETED"
					assert.Equal(t, settled, gotR.CompletionDate != nil, "restore completion step %d", i)
					assert.Equal(t, settled, gotR.CreatedResourceArn != "", "created arn step %d", i)

					rps, _ := b.ListRecoveryPointsByBackupVault(dst.BackupVaultName)
					assert.Equal(t, settled, len(rps) == 1, "copied recovery point step %d", i)
				}
			})
		})
	}
}

func TestReportJob_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		putErr     error
		name       string
		template   string
		wantStatus string
		formats    []string
		wantKeys   int
	}{
		{name: "default_csv", template: "COPY_JOB_REPORT", wantStatus: "COMPLETED", wantKeys: 1},
		{
			name: "csv_json", template: "RESTORE_JOB_REPORT", formats: []string{"CSV", "JSON"},
			wantStatus: "COMPLETED", wantKeys: 2,
		},
		{
			name: "resource_report", template: "RESOURCE_COMPLIANCE_REPORT", formats: []string{"JSON"},
			wantStatus: "COMPLETED", wantKeys: 1,
		},
		{
			name: "s3_failure_fails_job", template: "BACKUP_JOB_REPORT", putErr: errDenied,
			wantStatus: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			store := newFakeObjects()
			store.putErr = tt.putErr
			b.SetObjectStore(store)

			_, err := b.CreateReportPlan("p", "",
				&backup.ReportDeliveryChannel{S3BucketName: "rep", S3KeyPrefix: "/pre/", Formats: tt.formats},
				&backup.ReportSetting{ReportTemplate: tt.template})
			require.NoError(t, err)

			job := b.StartReportJob("p")
			assert.Equal(t, tt.wantStatus, job.Status)
			assert.Equal(t, tt.template, job.ReportTemplate)

			got, err := b.DescribeReportJob(job.ReportJobID)
			require.NoError(t, err)
			require.NotNil(t, got.ReportDestination)
			assert.Equal(t, "rep", got.ReportDestination.S3BucketName)
			assert.Len(t, got.ReportDestination.S3Keys, tt.wantKeys)

			if tt.putErr != nil {
				assert.Contains(t, got.StatusMessage, "AccessDenied")

				return
			}

			assert.Len(t, store.keys(), tt.wantKeys)
			for _, k := range got.ReportDestination.S3Keys {
				assert.Contains(t, store.keys(), "rep/"+k)
				assert.Contains(t, k, "pre/Backup/us-east-1/")
			}
		})
	}
}

func TestReportJob_JSONContent(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	store := newFakeObjects()
	b.SetObjectStore(store)
	v := mustVault(t, b, "vault")
	completedRecoveryPoint(t, b, v.BackupVaultName, testVolumeArn, "EBS")

	_, err := b.CreateReportPlan("p", "",
		&backup.ReportDeliveryChannel{S3BucketName: "rep", Formats: []string{"JSON"}},
		&backup.ReportSetting{ReportTemplate: "BACKUP_JOB_REPORT"})
	require.NoError(t, err)

	job := b.StartReportJob("p")
	got, err := b.DescribeReportJob(job.ReportJobID)
	require.NoError(t, err)
	require.Len(t, got.ReportDestination.S3Keys, 1)

	var rows []map[string]string
	require.NoError(t, json.Unmarshal(store.objects["rep/"+got.ReportDestination.S3Keys[0]], &rows))
	require.Len(t, rows, 1)
	assert.Equal(t, testVolumeArn, rows[0]["Resource ARN"])
	assert.Equal(t, "COMPLETED", rows[0]["Job Status"])
}
