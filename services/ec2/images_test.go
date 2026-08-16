package ec2_test

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCancelImageLaunchPermission(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	require.NoError(t, b.ModifyImageAttribute("ami-testimg", "launchPermission", "all"))
	require.NoError(t, b.CancelImageLaunchPermission("ami-testimg"))
	// Idempotent: calling again on an image with no stored permission is fine.
	require.NoError(t, b.CancelImageLaunchPermission("ami-testimg"))

	require.ErrorIs(t, b.CancelImageLaunchPermission(""), ec2.ErrInvalidParameter)
}

func TestDescribeImageReferences(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-refimg", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	_, err = b.CreateLaunchTemplate("lt-name", "ami-refimg", "t3.micro", nil)
	require.NoError(t, err)

	refs := b.DescribeImageReferences([]string{"ami-refimg"})
	require.Len(t, refs, 2)

	var sawInstance, sawLaunchTemplate bool

	for _, r := range refs {
		assert.Equal(t, "ami-refimg", r.ImageID)
		assert.NotEmpty(t, r.Arn)

		switch r.ResourceType {
		case "ec2:Instance":
			sawInstance = true
		case "ec2:LaunchTemplate":
			sawLaunchTemplate = true
		}
	}

	assert.True(t, sawInstance)
	assert.True(t, sawLaunchTemplate)

	assert.Empty(t, b.DescribeImageReferences([]string{"ami-unreferenced"}))
}

func TestGetImageAncestry(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	root, err := b.CreateImage(instances[0].ID, "root", "root image")
	require.NoError(t, err)

	child, err := b.CopyImage(root.ImageID, "child", "child image")
	require.NoError(t, err)

	grandchild, err := b.CopyImage(child.ImageID, "grandchild", "grandchild image")
	require.NoError(t, err)

	entries, err := b.GetImageAncestry(grandchild.ImageID)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, grandchild.ImageID, entries[0].ImageID)
	assert.Equal(t, child.ImageID, entries[0].SourceImageID)
	assert.Equal(t, child.ImageID, entries[1].ImageID)
	assert.Equal(t, root.ImageID, entries[1].SourceImageID)
	assert.Equal(t, root.ImageID, entries[2].ImageID)
	assert.Empty(t, entries[2].SourceImageID)

	_, err = b.GetImageAncestry("ami-missing")
	require.ErrorIs(t, err, ec2.ErrImageNotFound)
}

// TestCopyImage verifies image copying.
func TestCopyImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sourceImageID string
		newName       string
		wantErr       bool
	}{
		{
			name:          "copy_stub_ami",
			sourceImageID: "ami-0c55b159cbfafe1f0",
			newName:       "my-copy",
			wantErr:       false,
		},
		{
			name:          "missing_source",
			sourceImageID: "",
			wantErr:       true,
		},
		{
			name:          "nonexistent_source",
			sourceImageID: "ami-nonexistent",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			copied, err := b.CopyImage(tt.sourceImageID, tt.newName, "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEqual(t, tt.sourceImageID, copied.ImageID)
			assert.Equal(t, tt.newName, copied.Name)
		})
	}
}

// TestDeregisterImage verifies AMI deregistration.

// TestDeregisterImage verifies AMI deregistration.
func TestDeregisterImage(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.RunInstances("ami-0c55b159cbfafe1f0", "t3.micro", "", 1)
	require.NoError(t, err)

	img, err := b.CreateImage("", "test", "")
	require.Error(t, err, "should fail on empty instance ID")
	assert.Nil(t, img)

	// Create image from a real instance
	insts := b.DescribeInstances(nil, "")
	require.NotEmpty(t, insts)

	created, err := b.CreateImage(insts[0].ID, "to-deregister", "desc")
	require.NoError(t, err)

	require.NoError(t, b.DeregisterImage(created.ImageID))

	// verify gone
	assert.Error(t, b.DeregisterImage(created.ImageID))
}

// TestModifyVpcAttribute verifies VPC attribute modification.

// TestHTTP_CopyImage verifies the HTTP handler for CopyImage.
func TestHTTP_CopyImage(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(
		t,
		h,
		"Action=CopyImage&Version=2016-11-15&SourceImageId=ami-0c55b159cbfafe1f0&Name=my-copy",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ami-")
}

// TestHTTP_ModifyVpcAttribute verifies the HTTP handler for ModifyVpcAttribute.

// TestDescribeImages_Pagination verifies that DescribeImages supports
// MaxResults and NextToken pagination (previously missing).
// Note: the backend always includes 3 built-in stub AMIs; numImages is extra.
func TestDescribeImages_Pagination(t *testing.T) {
	t.Parallel()

	const stubAMICount = 3 // number of pre-seeded stub AMIs in InMemoryBackend

	tests := []struct {
		name       string
		numImages  int // extra images added beyond stubs
		maxResults int
		wantPages  int
	}{
		{
			// 3 stubs + 3 extra = 6 total; maxResults=2 → 3 pages.
			name:       "paginates across three pages",
			numImages:  3,
			maxResults: 2,
			wantPages:  3,
		},
		{
			// 3 stubs; maxResults=10 → single page.
			name:       "single page when all fit",
			numImages:  0,
			maxResults: 10,
			wantPages:  1,
		},
		{
			// 3 stubs; maxResults=3 → single page exactly.
			name:       "exact boundary no second page",
			numImages:  0,
			maxResults: stubAMICount,
			wantPages:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			for i := range tc.numImages {
				b.AddImageForTest(ec2.AMIStub{
					ImageID:      "ami-parity-" + string(rune('a'+i)),
					Name:         "test-ami-" + string(rune('a'+i)),
					Architecture: "x86_64",
				})
			}

			pages := 0
			nextToken := ""

			for {
				vals := url.Values{
					"Action":     {"DescribeImages"},
					"Version":    {"2016-11-15"},
					"MaxResults": {strconv.Itoa(tc.maxResults)},
				}
				if nextToken != "" {
					vals.Set("NextToken", nextToken)
				}

				resp, err := dispatchHandler(h, vals)
				require.NoError(t, err)
				pages++

				tok := accuracyExtractXMLValue(resp, "nextToken")
				if tok == "" {
					break
				}

				// Token must be valid base64.
				_, decErr := base64.StdEncoding.DecodeString(tok)
				require.NoError(t, decErr, "NextToken must be valid base64")

				nextToken = tok
			}

			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

// TestDescribeImages_MaxResultsValidation verifies that invalid MaxResults
// values are rejected.

// TestDescribeImages_MaxResultsValidation verifies that invalid MaxResults
// values are rejected.
func TestDescribeImages_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantErr    bool
	}{
		{"zero is invalid", "0", true},
		{"negative is invalid", "-1", true},
		{"over 1000 is invalid", "1001", true},
		{"1 is valid", "1", false},
		{"1000 is valid", "1000", false},
		{"not a number is invalid", "abc", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vals := url.Values{
				"Action":     {"DescribeImages"},
				"Version":    {"2016-11-15"},
				"MaxResults": {tc.maxResults},
			}

			_, err := dispatchHandler(h, vals)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDescribeImages(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	amis := b.DescribeImages()
	assert.NotEmpty(t, amis)
	assert.Equal(t, "ami-0c55b159cbfafe1f0", amis[0].ImageID)
}
