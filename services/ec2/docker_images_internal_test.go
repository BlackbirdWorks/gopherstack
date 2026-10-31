package ec2

import (
	"maps"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	amiAL2       = "ami-0c55b159cbfafe1f0"
	amiUbuntu22  = "ami-0eb260c4d5475b901"
	amiWindows   = "ami-09d3b3274b6c5d4aa"
	amiAL2023    = "ami-0f713cec7ef8c2ed1"
	amiUbuntu24  = "ami-0663e6754638bbff3"
	amiUbuntu20  = "ami-057f486f0e508831b"
	amiBottle    = "ami-0a66ee52e0b6c6cdb"
	amiDiscover  = "ami-0123456789abcdef0"
	richProvider = "ctr-1"
)

// newDockerHandler returns a handler whose backend launches instances through a DockerCompute over a fake.
func newDockerHandler(t *testing.T, cfg DockerComputeConfig) (*Handler, *InMemoryBackend, *richDockerAPI) {
	t.Helper()

	base := &fakeDockerAPI{containerID: richProvider, containerIP: "172.17.0.9"}
	api := newRichDockerAPI(base)
	b := NewInMemoryBackend("000000000000", "us-east-1")
	b.WithCompute(newDockerComputeWithAPI(api, cfg))

	h := NewHandler(b)
	h.AccountID = "000000000000"

	return h, b, api
}

func runVals(imageID string, extra url.Values) url.Values {
	v := url.Values{"ImageId": {imageID}, "InstanceType": {"t3.micro"}, "MinCount": {"1"}, "MaxCount": {"1"}}
	maps.Copy(v, extra)

	return v
}

func TestSeededDockerImageMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		imageID   string
		want      string
		wantKnown bool
	}{
		{name: "al2", imageID: amiAL2, want: "amazonlinux:2", wantKnown: true},
		{name: "al2023", imageID: amiAL2023, want: "amazonlinux:2023", wantKnown: true},
		{name: "ubuntu_22_04_stub", imageID: amiUbuntu22, want: "ubuntu:22.04", wantKnown: true},
		{name: "ubuntu_20_04", imageID: amiUbuntu20, want: "ubuntu:20.04", wantKnown: true},
		{name: "ubuntu_24_04", imageID: amiUbuntu24, want: "ubuntu:24.04", wantKnown: true},
		{name: "windows_has_no_container", imageID: amiWindows, want: "", wantKnown: true},
		{name: "bottlerocket_has_no_container", imageID: amiBottle, want: "", wantKnown: true},
		{name: "unknown", imageID: "ami-00000000deadbeef0", want: "", wantKnown: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			got, known := b.DockerImageFor(tt.imageID)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantKnown, known)
		})
	}
}

func TestRunInstancesChoosesImagePerAMI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(t *testing.T, h *Handler, api *richDockerAPI) string
		name      string
		wantImage string
	}{
		{
			name:      "seeded_al2023",
			setup:     func(*testing.T, *Handler, *richDockerAPI) string { return amiAL2023 },
			wantImage: "amazonlinux:2023",
		},
		{
			name:      "seeded_ubuntu",
			setup:     func(*testing.T, *Handler, *richDockerAPI) string { return amiUbuntu24 },
			wantImage: "ubuntu:24.04",
		},
		{
			name:      "windows_falls_back_to_configured_default",
			setup:     func(*testing.T, *Handler, *richDockerAPI) string { return amiWindows },
			wantImage: "alpine:3.20",
		},
		{
			name: "register_image_docker_location",
			setup: func(t *testing.T, h *Handler, _ *richDockerAPI) string {
				t.Helper()

				resp, err := h.handleRegisterImage(url.Values{
					"Name": {"custom"}, "ImageLocation": {"docker://busybox:1.36"},
				}, "r")
				require.NoError(t, err)

				return resp.(*registerImageResponse).ImageID
			},
			wantImage: "busybox:1.36",
		},
		{
			name: "register_image_without_binding_uses_default",
			setup: func(t *testing.T, h *Handler, _ *richDockerAPI) string {
				t.Helper()

				resp, err := h.handleRegisterImage(url.Values{"Name": {"plain"}}, "r")
				require.NoError(t, err)

				return resp.(*registerImageResponse).ImageID
			},
			wantImage: "alpine:3.20",
		},
		{
			name: "copy_image_inherits_binding",
			setup: func(t *testing.T, h *Handler, _ *richDockerAPI) string {
				t.Helper()

				resp, err := h.handleCopyImage(url.Values{"SourceImageId": {amiUbuntu22}, "Name": {"u-copy"}}, "r")
				require.NoError(t, err)

				return resp.(*copyImageResponse).ImageID
			},
			wantImage: "ubuntu:22.04",
		},
		{
			name:    "unknown_ami",
			setup:   func(*testing.T, *Handler, *richDockerAPI) string { return "ami-00000000deadbeef0" },
			wantErr: ErrImageNotFound,
		},
		{
			name: "discovered_local_image",
			setup: func(_ *testing.T, _ *Handler, api *richDockerAPI) string {
				api.images["gopherstack-ec2/web:"+amiDiscover] = "x"

				return amiDiscover
			},
			wantImage: "gopherstack-ec2/web:" + amiDiscover,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, api := newDockerHandler(t, DockerComputeConfig{Image: "alpine:3.20"})
			imageID := tt.setup(t, h, api)

			_, err := h.handleRunInstances(runVals(imageID, nil), "r")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, api.created)

				return
			}

			require.NoError(t, err)
			require.Len(t, api.created, 1)
			assert.Equal(t, tt.wantImage, api.created[0].Config.Image)
			assert.Equal(t, imageID, api.created[0].Config.Labels[dockerLabelPrefix+"image"])
		})
	}
}

func TestLocalImagesAreNeverPulled(t *testing.T) {
	t.Parallel()

	h, _, api := newDockerHandler(t, DockerComputeConfig{})
	api.images["gopherstack-ec2/web:"+amiDiscover] = "x"

	_, err := h.handleRunInstances(runVals(amiDiscover, nil), "r")
	require.NoError(t, err)
	assert.NotContains(t, api.pulled, "gopherstack-ec2/web:"+amiDiscover)
}

func TestDiscoveredImagesAppearInDescribeImages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tag      string
		wantName string
		wantID   string
		wantSeen bool
	}{
		{
			name:     "convention",
			tag:      "gopherstack-ec2/web:" + amiDiscover,
			wantName: "web",
			wantID:   amiDiscover,
			wantSeen: true,
		},
		{
			name: "nested_name", tag: "gopherstack-ec2/team/db:ami-0fedcba987654321a",
			wantName: "team/db", wantID: "ami-0fedcba987654321a", wantSeen: true,
		},
		{name: "non_ami_tag", tag: "gopherstack-ec2/web:latest"},
		{name: "other_repo", tag: "someone/web:" + amiDiscover},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b, api := newDockerHandler(t, DockerComputeConfig{})
			api.images[tt.tag] = "x"

			_, err := h.handleDescribeImages(url.Values{}, "r")
			require.NoError(t, err)

			var found *AMIStub

			for _, img := range b.DescribeImages() {
				if img.Name == tt.wantName && tt.wantName != "" {
					found = &img
				}
			}

			if !tt.wantSeen {
				assert.Nil(t, found)

				return
			}

			require.NotNil(t, found)
			assert.Equal(t, tt.wantID, found.ImageID)
			assert.Equal(t, "000000000000", found.OwnerID)
			assert.Equal(t, tt.tag, found.DockerImage)
		})
	}
}

func TestCreateImageCommitsContainer(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	instances := b.DescribeInstances(nil, "")
	require.Len(t, instances, 1)

	resp, err := h.handleCreateImage(url.Values{"InstanceId": {instances[0].ID}, "Name": {"My Golden Image"}}, "r")
	require.NoError(t, err)

	amiID := resp.(*createImageResponse).ImageID
	require.Len(t, api.commits, 1)
	assert.Equal(t, "gopherstack-ec2/my-golden-image:"+amiID, api.commits[0].Reference)

	ref, known := b.DockerImageFor(amiID)
	assert.True(t, known)
	assert.Equal(t, api.commits[0].Reference, ref)

	_, err = h.handleRunInstances(runVals(amiID, nil), "r")
	require.NoError(t, err)
	require.Len(t, api.created, 2)
	assert.Equal(t, ref, api.created[1].Config.Image)
}

func TestCreateImageCommitFailureRollsBack(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	api.commitErr = errFakeBoom
	before := len(b.DescribeImages())

	_, err = h.handleCreateImage(url.Values{"InstanceId": {b.DescribeInstances(nil, "")[0].ID}, "Name": {"x"}}, "r")
	require.ErrorIs(t, err, errFakeBoom)
	assert.Len(t, b.DescribeImages(), before, "failed commit must not leave an AMI behind")
}

func TestParseLocalImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tag      string
		wantName string
		wantID   string
		wantOK   bool
	}{
		{
			tag:      "gopherstack-ec2/web:ami-0123456789abcdef0",
			wantName: "web",
			wantID:   "ami-0123456789abcdef0",
			wantOK:   true,
		},
		{tag: "gopherstack-ec2/web", wantOK: false},
		{tag: "gopherstack-ec2/:ami-0123456789abcdef0", wantOK: false},
		{tag: "gopherstack-ec2/web:ami-XYZ", wantOK: false},
		{tag: "x/web:ami-0123456789abcdef0", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			t.Parallel()

			name, id, ok := parseLocalImageTag(tt.tag)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantID, id)
		})
	}
}

func TestSanitizeImageName(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"My Image 1", "my-image-1"},
		{"  ", "image"},
		{"a/b", "a-b"},
		{"UPPER_case.v2", "upper_case.v2"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, sanitizeImageName(tt.in))
		})
	}
}

func TestDockerImageBindingSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	img, err := b.RegisterImage("persisted", "", "")
	require.NoError(t, err)
	require.NoError(t, b.BindDockerImage(img.ImageID, "busybox:1.36"))

	restored := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), b.Snapshot(t.Context())))

	ref, known := restored.DockerImageFor(img.ImageID)
	assert.True(t, known)
	assert.Equal(t, "busybox:1.36", ref)
}
