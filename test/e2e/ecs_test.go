//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
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

	err = page.Locator("h1:has-text('ECS')").WaitFor(playwright.LocatorWaitForOptions{
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

	err = page.Locator("h1:has-text('ECS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Clusters")
	assert.Contains(t, content, "No clusters")
}

// TestECSDashboard_CreateAndDeleteCluster verifies the create and delete cluster UI flows.
func TestECSDashboard_CreateAndDeleteCluster(t *testing.T) {
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
			saveScreenshot(t, page, "TestECSDashboard_CreateAndDeleteCluster")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ecs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('ECS')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal
	err = page.Locator("button:has-text('+ Create Cluster')").Click()
	require.NoError(t, err)

	// Fill in cluster name
	err = page.Locator("input[name='name']").Fill("e2e-test-cluster")
	require.NoError(t, err)

	// Submit form (use Last() since "Create Cluster" text appears in both the header button and submit button)
	err = page.Locator("button:has-text('Create Cluster')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect and verify
	err = page.Locator("td:has-text('e2e-test-cluster')").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-cluster")

	// Delete the cluster
	err = page.Locator("form[action='/dashboard/ecs/cluster/delete'] button").Click()
	require.NoError(t, err)

	err = page.Locator("td:has-text('No clusters')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
