//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
)

// TestELBDashboard verifies the ELB dashboard UI renders load balancers.
func TestELBDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ELBHandler.Backend.CreateLoadBalancer(elbbackend.CreateLoadBalancerInput{
		LoadBalancerName: "e2e-test-lb",
		Scheme:           "internet-facing",
	})
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
			saveScreenshot(t, page, "TestELBDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elb")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Load Balancers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-lb")
}

// TestELBDashboard_Empty verifies the ELB dashboard renders with no load balancers.
func TestELBDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestELBDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elb")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Load Balancers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No load balancers")
}

// TestELBDashboard_CreateAndDelete verifies the ELB dashboard create and delete flows.
func TestELBDashboard_CreateAndDelete(t *testing.T) {
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
			saveScreenshot(t, page, "TestELBDashboard_CreateAndDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elb")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Load Balancers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Create Load Balancer')").Click()
	require.NoError(t, err)

	err = page.Locator("#name").Fill("e2e-create-lb")
	require.NoError(t, err)

	err = page.Locator("button[type='submit']:has-text('Create')").Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Load Balancers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-create-lb")

	err = page.Locator("button:has-text('Delete')").First().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Load Balancers')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
