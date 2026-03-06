//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3controlbackend "github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestS3ControlDashboard verifies the S3 Control dashboard UI renders public access block configs.
func TestS3ControlDashboard(t *testing.T) {
	stack := newStack(t)

	stack.S3ControlHandler.Backend.PutPublicAccessBlock(s3controlbackend.PublicAccessBlock{
		AccountID:             "000000000000",
		BlockPublicAcls:       true,
		IgnorePublicAcls:      true,
		BlockPublicPolicy:     true,
		RestrictPublicBuckets: true,
	})

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestS3ControlDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3control")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('S3 Control')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "S3 Control")
}

// TestS3ControlDashboard_Empty verifies the empty state renders correctly.
func TestS3ControlDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestS3ControlDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3control")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('S3 Control')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "S3 Control")
}
