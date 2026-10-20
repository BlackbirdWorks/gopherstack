//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCognitoIdentityDashboard verifies the Cognito Identity dashboard UI renders identity pools.
func TestCognitoIdentityDashboard(t *testing.T) {
	t.Parallel()
	stack := newStack(t)

	_, err := stack.CognitoIdentityHandler.Backend.CreateIdentityPool(
		t.Context(),
		"e2e-test-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
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
			saveScreenshot(t, page, "TestCognitoIdentityDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cognitoidentity")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content := pageContentContaining(t, page, "e2e-test-pool")
	assert.Contains(t, content, "e2e-test-pool")
}

// TestCognitoIdentityDashboard_Empty verifies the empty state renders correctly.
func TestCognitoIdentityDashboard_Empty(t *testing.T) {
	t.Parallel()
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
			saveScreenshot(t, page, "TestCognitoIdentityDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cognitoidentity")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content := pageContentContaining(t, page, "Cognito Identity", "No identity pools found")
	assert.Contains(t, content, "Cognito Identity")
	assert.Contains(t, content, "No identity pools found")
}
