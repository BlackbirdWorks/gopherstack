package s3files_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/s3files"
)

type fakeNetwork struct {
	enis map[string][]string
	reqs []s3files.ENIRequest
	seq  int
	mu   sync.Mutex
}

func newFakeNetwork() *fakeNetwork { return &fakeNetwork{enis: map[string][]string{}} }

func (n *fakeNetwork) CreateENI(req s3files.ENIRequest) (s3files.ENI, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, sg := range req.SecurityGroups {
		if sg == "sg-missing" {
			return s3files.ENI{}, awserr.New("no such group", s3files.ErrNotFound)
		}

		if sg == "sg-other-vpc" {
			return s3files.ENI{}, awserr.New("wrong vpc", s3files.ErrValidation)
		}
	}

	n.seq++
	n.reqs = append(n.reqs, req)
	id := "eni-" + string(rune('a'+n.seq))

	sgs := req.SecurityGroups
	if len(sgs) == 0 {
		sgs = []string{"sg-default"}
	}

	n.enis[id] = sgs

	return s3files.ENI{ID: id, PrivateIP: "10.0.1.200", SecurityGroups: sgs}, nil
}

func (n *fakeNetwork) SetENISecurityGroups(id string, sgs []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.enis[id] = sgs

	return nil
}

func (n *fakeNetwork) DeleteENI(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.enis, id)
}

func (n *fakeNetwork) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return len(n.enis)
}

var (
	errKeyMissing  = errors.New("KMS key does not exist")
	errKeyDisabled = errors.New("KMS key is not enabled")
)

type fakeRoles map[string]bool

func (f fakeRoles) RoleExists(arn string) bool { return f[arn] }

type fakeKeys map[string]error

func (f fakeKeys) CheckKey(_ context.Context, id string) error {
	if err, ok := f[id]; ok {
		return err
	}

	return errKeyMissing
}

func TestMountTargetENILifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		sgs      []string
		wantSGs  []string
	}{
		{name: "default group", wantSGs: []string{"sg-default"}},
		{name: "given group", sgs: []string{"sg-1"}, wantSGs: []string{"sg-1"}},
		{name: "missing group", sgs: []string{"sg-missing"}, wantCode: "ResourceNotFoundException"},
		{name: "other vpc", sgs: []string{"sg-other-vpc"}, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3files.NewInMemoryBackend(testAccount, testRegion)
			net := newFakeNetwork()
			backend.SetNetworkResolver(net)
			f := newFixtureWith(t, backend, testRegion)
			fsID := f.fileSystem(t)

			mt, err := f.client.CreateMountTarget(t.Context(), &s3filessdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet), SecurityGroups: tt.sgs,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))
				assert.Zero(t, net.count())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSGs, mt.SecurityGroups)
			assert.Equal(t, "eni-b", aws.ToString(mt.NetworkInterfaceId))
			assert.Equal(t, "10.0.1.200", aws.ToString(mt.Ipv4Address))
			assert.Contains(t, net.reqs[0].Description, fsID)
			assert.Contains(t, net.reqs[0].Description, aws.ToString(mt.MountTargetId))

			up, err := f.client.UpdateMountTarget(t.Context(), &s3filessdk.UpdateMountTargetInput{
				MountTargetId: mt.MountTargetId, SecurityGroups: []string{"sg-new"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"sg-new"}, up.SecurityGroups)
			assert.Equal(t, []string{"sg-new"}, net.enis["eni-b"])

			_, err = f.client.DeleteMountTarget(t.Context(), &s3filessdk.DeleteMountTargetInput{
				MountTargetId: mt.MountTargetId,
			})
			require.NoError(t, err)
			assert.Zero(t, net.count())
		})
	}
}

func TestForceDeleteFileSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		force    bool
		wantCode string
	}{
		{name: "without force", wantCode: "ConflictException"},
		{name: "with force", force: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3files.NewInMemoryBackend(testAccount, testRegion)
			net := newFakeNetwork()
			backend.SetNetworkResolver(net)
			f := newFixtureWith(t, backend, testRegion)
			fsID := f.fileSystem(t)

			mt, err := f.client.CreateMountTarget(t.Context(), &s3filessdk.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet),
			})
			require.NoError(t, err)

			ap, err := f.client.CreateAccessPoint(t.Context(), &s3filessdk.CreateAccessPointInput{
				FileSystemId: aws.String(fsID),
			})
			require.NoError(t, err)

			_, err = f.client.DeleteFileSystem(t.Context(), &s3filessdk.DeleteFileSystemInput{
				FileSystemId: aws.String(fsID), ForceDelete: aws.Bool(tt.force),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))
				assert.Equal(t, 1, net.count())

				return
			}

			require.NoError(t, err)
			assert.Zero(t, net.count())

			_, err = f.client.GetMountTarget(t.Context(), &s3filessdk.GetMountTargetInput{
				MountTargetId: mt.MountTargetId,
			})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.GetAccessPoint(t.Context(), &s3filessdk.GetAccessPointInput{AccessPointId: ap.AccessPointId})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.GetFileSystem(t.Context(), &s3filessdk.GetFileSystemInput{FileSystemId: aws.String(fsID)})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		})
	}
}

func TestCreateFileSystemRoleAndKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		role     string
		key      string
		wantCode string
	}{
		{name: "ok", role: testRole, key: "key-ok"},
		{name: "no key", role: testRole},
		{name: "unknown role", role: "arn:aws:iam::123456789012:role/nope", wantCode: "ValidationException"},
		{name: "unknown key", role: testRole, key: "key-gone", wantCode: "ValidationException"},
		{name: "disabled key", role: testRole, key: "key-off", wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3files.NewInMemoryBackend(testAccount, testRegion)
			backend.SetRoleResolver(fakeRoles{testRole: true})
			backend.SetKeyResolver(fakeKeys{"key-ok": nil, "key-off": errKeyDisabled})
			f := newFixtureWith(t, backend, testRegion)

			_, err := f.client.CreateFileSystem(t.Context(), &s3filessdk.CreateFileSystemInput{
				Bucket: aws.String(testBucket), RoleArn: aws.String(tt.role), KmsKeyId: nilIfEmpty(tt.key),
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}
