package ec2_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Image lifecycle ---- //nolint:godot // existing issue.
func TestImageLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	imageID := "ami-testimg"

	t.Run("disable and enable image", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableImage(imageID))
		require.NoError(t, b.EnableImage(imageID))
	})

	t.Run("enable/disable image block public access", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Equal(t, "unblocked", b.GetImageBlockPublicAccessState())
		require.NoError(t, b.EnableImageBlockPublicAccess("block-new-sharing"))
		assert.Equal(t, "block-new-sharing", b.GetImageBlockPublicAccessState())
		b.DisableImageBlockPublicAccess()
		assert.Equal(t, "unblocked", b.GetImageBlockPublicAccessState())
	})

	t.Run("enable/disable image deprecation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableImageDeprecation(imageID, "2027-01-01T00:00:00Z"))
		require.NoError(t, b.DisableImageDeprecation(imageID))
	})

	t.Run("enable/disable deregistration protection", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableImageDeregistrationProtection(imageID))
		require.NoError(t, b.DisableImageDeregistrationProtection(imageID))
	})

	t.Run("modify and reset image attribute", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyImageAttribute(imageID, "description", "my new desc"))
		require.NoError(t, b.ResetImageAttribute(imageID, "description"))
	})

	t.Run("empty image ID returns error for disable", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DisableImage(""))
	})
}

func TestDescribeInstanceImageMetadata(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	t.Run("returns metadata for instances", func(t *testing.T) {
		items := b.DescribeInstanceImageMetadata([]string{insts[0].ID})
		require.Len(t, items, 1)
		assert.Equal(t, insts[0].ID, items[0].InstanceID)
		assert.Equal(t, "available", items[0].ImageState)
	})
}

// ---- Serial console ---- //nolint:godot // existing issue.

func TestHTTP_GetImageBlockPublicAccessState(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetImageBlockPublicAccessState"},
	})
	require.NoError(t, err)
}

// ---- Image lifecycle ---- //nolint:godot // existing issue.
func TestRegisterImage(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("registers new AMI", func(t *testing.T) { //nolint:paralleltest // existing issue.
		img, err := b.RegisterImage("test-ami", "test AMI", "x86_64")
		require.NoError(t, err)
		assert.NotEmpty(t, img.ImageID)
		assert.Equal(t, "test-ami", img.Name)
	})

	t.Run("empty name returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.RegisterImage("", "", "")
		require.Error(t, err)
	})
}

// TestHTTP_RegisterImage verifies RegisterImage's response includes the
// real ImageId instead of a boolean-only Return field.

// TestHTTP_RegisterImage verifies RegisterImage's response includes the
// real ImageId instead of a boolean-only Return field.
func TestHTTP_RegisterImage(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	out, err := ec2.ExportDispatch(h, url.Values{
		"Action":       []string{"RegisterImage"},
		"Name":         []string{"wire-shape-ami"},
		"Architecture": []string{"x86_64"},
	})
	require.NoError(t, err)
	assert.Contains(t, out, "<imageId>ami-")

	var registered *ec2.AMIStub

	for _, img := range b.DescribeImages() {
		if img.Name == "wire-shape-ami" {
			img := img
			registered = &img

			break
		}
	}

	require.NotNil(t, registered, "registered image must be discoverable via DescribeImages")
	assert.Contains(t, out, registered.ImageID)
}

func TestImportExportImage(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("import image creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.ImportImage("test import", "x86_64", "Linux/UNIX")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ImportTaskID)
		assert.Equal(t, "completed", task.Status)
	})

	t.Run("describe import tasks returns all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tasks := b.DescribeImportImageTasks(nil)
		require.Len(t, tasks, 1)
	})

	t.Run("export image creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.ExportImage("ami-test", "test export", "vmdk", "my-export-bucket", "exports/", "")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ExportImageTaskID)
		assert.Equal(t, "active", task.Status)
	})

	t.Run("describe export image tasks settle", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tasks := b.DescribeExportImageTasks(nil)
		require.Len(t, tasks, 1)
		assert.Equal(t, "completed", tasks[0].Status)
	})
}

// ---- Snapshot recycle bin ---- //nolint:godot // existing issue.

// ---- Fast launch / snapshot restores ---- //nolint:godot // existing issue.
func TestFastLaunch(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	imageID := "ami-testfast"

	t.Run("enable fast launch", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableFastLaunch(imageID))
		items := b.DescribeFastLaunchImages([]string{imageID})
		require.Len(t, items, 1)
		assert.Equal(t, "enabled", items[0].State)
	})

	t.Run("disable fast launch", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableFastLaunch(imageID))
		items := b.DescribeFastLaunchImages([]string{imageID})
		assert.Empty(t, items)
	})
}

// TestImageBlockPublicAccess_ResponseState verifies that
// EnableImageBlockPublicAccess returns the new state directly in the
// <imageBlockPublicAccessState> element's text, rather than <return>true</return>
// or a nested <state> child -- the real deserializer (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentEnableImageBlockPublicAccessOutput)
// reads it as a flat scalar and hard-errors on a nested element.
func TestImageBlockPublicAccess_ResponseState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		stateParam  string
		wantState   string
		wantMissing string
	}{
		{
			name:        "enable_block_new_sharing_returns_state",
			action:      "EnableImageBlockPublicAccess",
			stateParam:  "block-new-sharing",
			wantState:   "block-new-sharing",
			wantMissing: "<return>",
		},
		{
			name:        "enable_default_state_returns_block_new_sharing",
			action:      "EnableImageBlockPublicAccess",
			stateParam:  "",
			wantState:   "block-new-sharing",
			wantMissing: "<return>",
		},
		{
			name:        "disable_returns_unblocked",
			action:      "DisableImageBlockPublicAccess",
			stateParam:  "",
			wantState:   "unblocked",
			wantMissing: "<return>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{"Action": {tt.action}}
			if tt.stateParam != "" {
				vals.Set("ImageBlockPublicAccessState", tt.stateParam)
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.Contains(t, resp, "<imageBlockPublicAccessState>"+tt.wantState+"</imageBlockPublicAccessState>",
				"response must contain <imageBlockPublicAccessState>%s</imageBlockPublicAccessState>", tt.wantState)
			assert.NotContains(t, resp, "<state>",
				"response must not nest the state under a <state> child element")
			assert.NotContains(t, resp, tt.wantMissing,
				"response must not contain %s", tt.wantMissing)
		})
	}
}

// TestImageBlockPublicAccess_RootElement verifies that
// Enable/DisableImageBlockPublicAccess return the correct XML root element names.

// TestImageBlockPublicAccess_RootElement verifies that
// Enable/DisableImageBlockPublicAccess return the correct XML root element names.
func TestImageBlockPublicAccess_RootElement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		action  string
		wantXML string
	}{
		{
			name:    "enable_root_element",
			action:  "EnableImageBlockPublicAccess",
			wantXML: "EnableImageBlockPublicAccessResponse",
		},
		{
			name:    "disable_root_element",
			action:  "DisableImageBlockPublicAccess",
			wantXML: "DisableImageBlockPublicAccessResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{"Action": {tt.action}}
			if tt.action == "EnableImageBlockPublicAccess" {
				vals.Set("ImageBlockPublicAccessState", "block-new-sharing")
			}

			resp, err := ec2.ExportDispatch(h, vals)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(resp, "<"+tt.wantXML+">"),
				"response XML root must be %s, got prefix: %.80s", tt.wantXML, resp)
		})
	}
}

// TestGetImageBlockPublicAccessState_DefaultUnblocked verifies that the
// account-level image block public access state defaults to "unblocked", matching AWS EC2
// behaviour. Previously the backend incorrectly defaulted to "block-new-sharing".
func TestGetImageBlockPublicAccessState_DefaultUnblocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setState  string // empty = don't call Enable
		wantState string
	}{
		{
			name:      "fresh_backend_defaults_to_unblocked",
			wantState: "unblocked",
		},
		{
			name:      "after_enable_block_new_sharing",
			setState:  "block-new-sharing",
			wantState: "block-new-sharing",
		},
		{
			name:      "after_disable_returns_unblocked",
			setState:  "disable",
			wantState: "unblocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			switch tt.setState {
			case "disable":
				_ = b.EnableImageBlockPublicAccess("block-new-sharing")
				b.DisableImageBlockPublicAccess()
			case "":
				// no-op — test default
			default:
				require.NoError(t, b.EnableImageBlockPublicAccess(tt.setState))
			}

			got := b.GetImageBlockPublicAccessState()
			assert.Equal(t, tt.wantState, got)
		})
	}
}

// TestModifyEbsDefaultKmsKeyId_ResponseXMLName verifies that
// ModifyEbsDefaultKmsKeyId returns a response with root element
// "ModifyEbsDefaultKmsKeyIdResponse", not "GetEbsDefaultKmsKeyIdResponse".
// The wrong element name causes AWS SDK deserialisation to fail.

func TestCancelImageLaunchPermissionHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"CancelImageLaunchPermission"},
		"ImageId": {"ami-testimg"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<CancelImageLaunchPermissionResponse>")
	assert.Contains(t, resp, "<return>true</return>")
}

func TestDescribeImageReferencesHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-refimg", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":    {"DescribeImageReferences"},
		"ImageId.1": {"ami-refimg"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeImageReferencesResponse")
	assert.Contains(t, resp, "<imageId>ami-refimg</imageId>")
	assert.Contains(t, resp, "<resourceType>ec2:Instance</resourceType>")
}

func TestGetImageAncestryHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	root, err := h.Backend.CreateImage(instances[0].ID, "root", "root image")
	require.NoError(t, err)
	child, err := h.Backend.CopyImage(root.ImageID, "child", "child image")
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":  {"GetImageAncestry"},
		"ImageId": {child.ImageID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetImageAncestryResponse")
	assert.Contains(t, resp, "<sourceImageId>"+root.ImageID+"</sourceImageId>")
}

// TestHandlerDeregisterImage covers handleDeregisterImage.
func TestHandlerDeregisterImage(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	// Create instance and image.
	insts, err := b.RunInstances("ami-base", "t3.micro", "", 1)
	require.NoError(t, err)

	img, err := b.CreateImage(insts[0].ID, "my-ami", "test image")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=DeregisterImage&Version=2016-11-15&ImageId="+img.ImageID)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found.
	rec = postForm(t, h, "Action=DeregisterImage&Version=2016-11-15&ImageId=ami-notfound")
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestHandlerModifySubnetAttribute covers handleModifySubnetAttribute.
