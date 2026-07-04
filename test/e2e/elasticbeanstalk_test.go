//go:build e2e
// +build e2e

package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// TestElasticbeanstalkDashboard verifies the Elastic Beanstalk dashboard UI renders applications and environments.
func TestElasticbeanstalkDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ElasticbeanstalkHandler.Backend.CreateApplication(
		context.Background(),
		"e2e-app",
		"E2E test application",
		map[string]string{"env": "e2e"},
	)
	require.NoError(t, err)

	_, err = stack.ElasticbeanstalkHandler.Backend.CreateEnvironment(
		context.Background(),
		"e2e-app", "e2e-env",
		"64bit Amazon Linux 2023 v4.0.0 running Python 3.11",
		"E2E test environment",
		map[string]string{"env": "e2e"},
		elasticbeanstalk.CreateEnvironmentParams{},
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
			saveScreenshot(t, page, "TestElasticbeanstalkDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elasticbeanstalk")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Wait for the app and env to appear.
	err = page.Locator("text=e2e-app").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Environments')").Click()
	require.NoError(t, err)

	err = page.Locator("text=e2e-env").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}

// TestElasticbeanstalkDashboard_Empty verifies the Elastic Beanstalk dashboard empty state renders correctly.
func TestElasticbeanstalkDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestElasticbeanstalkDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elasticbeanstalk")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No applications found")

	err = page.Locator("button:has-text('Environments')").Click()
	require.NoError(t, err)

	err = page.Locator("text=No environments found").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
