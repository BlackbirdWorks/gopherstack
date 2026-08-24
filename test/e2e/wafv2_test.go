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
)

// TestWafv2Dashboard verifies the WAFv2 dashboard UI renders Web ACL data.
func TestWafv2Dashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.Wafv2Handler.Backend.CreateWebACL(
		context.Background(),
		"e2e-test-acl",
		"REGIONAL",
		"test web ACL",
		[]byte(`{"Allow":{}}`),
		[]byte(`{"SampledRequestsEnabled":true,"CloudWatchMetricsEnabled":true,"MetricName":"e2e-test-acl"}`),
		nil,
		nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
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

	_, err = page.Goto(server.URL + "/dashboard/waf")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
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

	_, err = page.Goto(server.URL + "/dashboard/waf")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No Web ACLs found")
}
