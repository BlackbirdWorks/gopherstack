//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestECSDashboard verifies the ECS dashboard UI: clusters render correctly.
func TestECSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ECSHandler.Backend.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "demo-cluster"})
	require.NoError(t, err)

	_, err = stack.ECSHandler.Backend.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "production-cluster"})
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
			saveScreenshot(t, page, "TestECSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecs")
	require.NoError(t, err)

	// Wait for cluster names to be rendered in the DOM (loaded asynchronously).
	err = page.Locator("text=demo-cluster").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "demo-cluster")
	assert.Contains(t, content, "production-cluster")
}

// TestECSDashboard_Empty verifies the ECS dashboard empty state renders correctly.
func TestECSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestECSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ECS Clusters")
	assert.Contains(t, content, "No ECS clusters found")
}

// TestECSDashboard_CapacityProvidersTab verifies the Capacity Providers tab renders.
func TestECSDashboard_CapacityProvidersTab(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ECSHandler.Backend.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "cap-cluster"})
	require.NoError(t, err)

	_, err = stack.ECSHandler.Backend.CreateCapacityProvider(ecsbackend.CreateCapacityProviderInput{Name: "my-cp"})
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
			saveScreenshot(t, page, "TestECSDashboard_CapacityProvidersTab")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecs")
	require.NoError(t, err)

	// Wait for cluster to appear
	err = page.Locator("text=cap-cluster").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Click the Capacity tab
	err = page.Locator("text=Capacity").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	err = page.Locator("text=Capacity").First().Click()
	require.NoError(t, err)
}

// Note: Create/Delete UI flow via dashboard is not currently implemented.
func TestECSDashboard_CreateAndDeleteCluster(t *testing.T) {
	stack := newStack(t)

	// Create cluster via SDK
	_, err := stack.ECSHandler.Backend.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "e2e-test-cluster"})
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
			saveScreenshot(t, page, "TestECSDashboard_CreateAndDeleteCluster")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("text=e2e-test-cluster").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
