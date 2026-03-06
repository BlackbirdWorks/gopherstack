//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"

	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestOpenSearchDashboard verifies the OpenSearch dashboard UI renders domains.
func TestOpenSearchDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.OpenSearchHandler.Backend.CreateDomain("test-domain", "OpenSearch_2.11", opensearchbackend.ClusterConfig{})
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestOpenSearchDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/opensearch")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('OpenSearch')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	require.Contains(t, content, "test-domain")
}
