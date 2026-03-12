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

// TestLakeFormationDashboard verifies the Lake Formation dashboard UI renders resource data.
func TestLakeFormationDashboard(t *testing.T) {
	stack := newStack(t)

	err := stack.LakeFormationHandler.Backend.RegisterResource(
		"arn:aws:s3:::e2e-test-bucket",
		"arn:aws:iam::000000000000:role/e2e-test-role",
	)
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
			saveScreenshot(t, page, "TestLakeFormationDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/lakeformation")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Lake Formation')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-bucket")
	assert.Contains(t, content, "Register Resource")
}

// TestLakeFormationDashboard_Empty verifies the Lake Formation dashboard renders correctly with no data.
func TestLakeFormationDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestLakeFormationDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/lakeformation")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Lake Formation')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No resources registered")
}
