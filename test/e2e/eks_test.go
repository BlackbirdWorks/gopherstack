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

// TestEKSDashboard verifies the EKS dashboard UI renders clusters.
func TestEKSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EKSHandler.Backend.CreateCluster("e2e-test-cluster", "1.32", "", nil)
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
			saveScreenshot(t, page, "TestEKSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/eks")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EKS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-cluster")
}

// TestEKSDashboard_Empty verifies the EKS dashboard renders correctly when there are no clusters.
func TestEKSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestEKSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/eks")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EKS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "EKS Clusters")
}
