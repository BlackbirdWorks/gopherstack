//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
)

// TestEventBridgeDashboard verifies the EventBridge dashboard UI renders event buses.
func TestEventBridgeDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EventBridgeHandler.Backend.CreateEventBus("test-bus", "a test event bus")
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
			saveScreenshot(t, page, "TestEventBridgeDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/eventbridge")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EventBridge')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-bus")
}

// TestEventBridgeDashboard_Rules verifies the EventBridge rules page.
func TestEventBridgeDashboard_Rules(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EventBridgeHandler.Backend.CreateEventBus("rules-bus", "")
	require.NoError(t, err)

	_, err = stack.EventBridgeHandler.Backend.PutRule(ebbackend.PutRuleInput{
		Name:         "test-rule",
		EventBusName: "rules-bus",
		State:        "ENABLED",
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
			saveScreenshot(t, page, "TestEventBridgeDashboard_Rules")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/eventbridge/rules?bus=rules-bus")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EventBridge')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-rule")
}
