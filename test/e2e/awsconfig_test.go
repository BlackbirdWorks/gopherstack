//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAWSConfigDashboard verifies the AWS Config dashboard UI renders configuration recorders.
func TestAWSConfigDashboard(t *testing.T) {
	stack := newStack(t)

	err := stack.AWSConfigHandler.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config-role")
	require.NoError(t, err)

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
			saveScreenshot(t, page, "TestAWSConfigDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/awsconfig")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS Config Recorders')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "default")
}

// TestAWSConfigDashboard_Empty verifies the empty state renders correctly.
func TestAWSConfigDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestAWSConfigDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/awsconfig")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS Config Recorders')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AWS Config Recorders")
}
