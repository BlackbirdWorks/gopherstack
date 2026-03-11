//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/emr"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEMRDashboard verifies the EMR dashboard UI renders clusters.
func TestEMRDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EMRHandler.Backend.RunJobFlow("e2e-test-cluster", "emr-6.0.0", []emr.Tag{})
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
			saveScreenshot(t, page, "TestEMRDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emr")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon EMR')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-cluster")
}

// TestEMRDashboard_Empty verifies the EMR dashboard empty state renders correctly.
func TestEMRDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestEMRDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/emr")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon EMR')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon EMR")
}
