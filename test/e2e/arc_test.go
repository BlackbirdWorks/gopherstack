//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestARCDashboards verifies each Application Recovery Controller dashboard page renders its header and tabs.
func TestARCDashboards(t *testing.T) {
	tests := []struct {
		name    string
		route   string
		heading string
		tab     string
	}{
		{"recovery_control", "route53recoverycontrolconfig", "Route 53 Recovery Control", "Safety Rules"},
		{"recovery_cluster", "route53recoverycluster", "Route 53 Recovery Cluster", ""},
		{"recovery_readiness", "route53recoveryreadiness", "Route 53 Recovery Readiness", "Readiness Checks"},
		{"zonal_shift", "arczonalshift", "ARC Zonal Shift", "Practice Runs"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stack := newStack(t)

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
					saveScreenshot(t, page, "TestARCDashboards_"+tc.name)
				}
			}()

			_, err = page.Goto(server.URL + "/dashboard/" + tc.route)
			require.NoError(t, err)

			visible := playwright.LocatorWaitForOptions{
				State:   playwright.WaitForSelectorStateVisible,
				Timeout: playwright.Float(60000),
			}

			require.NoError(t, page.Locator("h1:has-text('"+tc.heading+"')").WaitFor(visible))

			if tc.tab != "" {
				require.NoError(t, page.Locator("[role=tab]:has-text('"+tc.tab+"')").WaitFor(visible))
			}
		})
	}
}
