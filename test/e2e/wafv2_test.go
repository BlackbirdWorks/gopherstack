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

// TestWafv2Dashboard verifies the WAFv2 dashboard UI renders Web ACL data.
func TestWafv2Dashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.Wafv2Handler.Backend.CreateWebACL(
		"e2e-test-acl",
		"REGIONAL",
		"test web ACL",
		"ALLOW",
		"",
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
			saveScreenshot(t, page, "TestWafv2Dashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/wafv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('WAFv2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-acl")
	assert.Contains(t, content, "REGIONAL")
}

// TestWafv2Dashboard_Empty verifies the WAFv2 dashboard renders correctly with no Web ACLs.
func TestWafv2Dashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestWafv2Dashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/wafv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('WAFv2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Create Web ACL")
}
