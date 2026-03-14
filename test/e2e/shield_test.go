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

// TestShieldDashboard verifies the Shield Advanced dashboard UI renders protection data.
func TestShieldDashboard(t *testing.T) {
	stack := newStack(t)

	err := stack.ShieldHandler.Backend.CreateSubscription()
	require.NoError(t, err)

	_, err = stack.ShieldHandler.Backend.CreateProtection(
		"e2e-test-protection",
		"arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-e2e00001",
		map[string]string{"Environment": "test"},
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
			saveScreenshot(t, page, "TestShieldDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/shield")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Shield Advanced')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-protection")
	assert.Contains(t, content, "Subscription Active")
	assert.Contains(t, content, "+ Add Protection")
}

// TestShieldDashboard_NoSubscription verifies the Shield Advanced dashboard renders correctly with no subscription.
func TestShieldDashboard_NoSubscription(t *testing.T) {
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
			saveScreenshot(t, page, "TestShieldDashboard_NoSubscription")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/shield")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Shield Advanced')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Enable Shield Advanced")
}
