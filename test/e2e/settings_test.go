//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

type e2eConfigManager struct {
	settings dashboard.Settings
}

func (m *e2eConfigManager) GetSettings() dashboard.Settings {
	return m.settings
}

func (m *e2eConfigManager) UpdateSettings(s dashboard.Settings) {
	m.settings = s
}

func (m *e2eConfigManager) SaveConfig() error {
	return nil
}

// testServerSettings returns a set of server-owned settings values that are
// deliberately distinct from the UI's browser-local defaults, so assertions
// against them prove the value flowed server -> UI rather than matching by
// coincidence.
func testServerSettings() dashboard.Settings {
	return dashboard.Settings{
		AccountID:         "112233445566",
		Region:            "eu-central-1",
		LatencyMs:         42,
		EnforceIAM:        true,
		AutoPurgeTTL:      17 * time.Minute,
		PortRangeStart:    6000,
		PortRangeEnd:      6100,
		InitScriptTimeout: 45 * time.Second,
		Persist:           true,
		Demo:              false,
	}
}

// assertReadOnlyField waits for the given locator to become visible, then
// asserts it is disabled (there is deliberately no write path for
// server-owned settings) and returns its current value for further
// assertions by the caller.
func assertReadOnlyField(t *testing.T, locator playwright.Locator, label string) string {
	t.Helper()

	require.NoError(t, locator.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	isDisabled, err := locator.IsDisabled()
	require.NoError(t, err)
	assert.True(t, isDisabled, "%s should be disabled: server-owned settings have no write path", label)

	value, err := locator.InputValue()
	require.NoError(t, err)

	return value
}

func TestE2E_SettingsPage(t *testing.T) {
	stack := newStack(t)

	want := testServerSettings()
	stack.Dashboard.ConfigManager = &e2eConfigManager{settings: want}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the settings page.
	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	require.NoError(t, page.Locator("h1:has-text('Settings')").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	// Every server-owned field must reflect the value the test's
	// e2eConfigManager reported, and must not be editable from the page.
	accountValue := assertReadOnlyField(t, page.Locator("#accountID"), "account ID")
	assert.Equal(t, want.AccountID, accountValue, "expected account ID value on settings page")

	regionValue := assertReadOnlyField(t, page.Locator("#region"), "region")
	assert.Equal(t, want.Region, regionValue, "expected region value on settings page")

	portStartValue := assertReadOnlyField(t, page.Locator("#portRangeStart"), "port range start")
	assert.Equal(t, strconv.Itoa(want.PortRangeStart), portStartValue)

	portEndValue := assertReadOnlyField(t, page.Locator("#portRangeEnd"), "port range end")
	assert.Equal(t, strconv.Itoa(want.PortRangeEnd), portEndValue)

	latencyValue := assertReadOnlyField(t, page.Locator("#latencyMs"), "latency")
	assert.Equal(t, strconv.Itoa(want.LatencyMs), latencyValue)

	// Duration fields are serialized via time.Duration.String() ("10m0s",
	// "30s", ...). Parse rather than string-compare so the test doesn't
	// hardcode that serialization detail.
	autoPurgeValue := assertReadOnlyField(t, page.Locator("#autoPurgeTTL"), "auto-purge TTL")
	gotAutoPurge, err := time.ParseDuration(autoPurgeValue)
	require.NoError(t, err)
	assert.Equal(t, want.AutoPurgeTTL, gotAutoPurge)

	initTimeoutValue := assertReadOnlyField(t, page.Locator("#initScriptTimeout"), "init script timeout")
	gotInitTimeout, err := time.ParseDuration(initTimeoutValue)
	require.NoError(t, err)
	assert.Equal(t, want.InitScriptTimeout, gotInitTimeout)

	enforceIAMCheckbox := page.Locator("#enforceIAM")
	require.NoError(t, enforceIAMCheckbox.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	isDisabled, err := enforceIAMCheckbox.IsDisabled()
	require.NoError(t, err)
	assert.True(t, isDisabled, "IAM Execution checkbox should be disabled: server-owned settings have no write path")
	isChecked, err := enforceIAMCheckbox.IsChecked()
	require.NoError(t, err)
	assert.Equal(t, want.EnforceIAM, isChecked)
}

func TestE2E_SettingsPage_NavbarIcon(t *testing.T) {
	stack := newStack(t)

	want := testServerSettings()
	stack.Dashboard.ConfigManager = &e2eConfigManager{settings: want}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Navigate to the main dashboard (redirects to /dynamodb).
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// The settings icon link should be present in the navbar.
	settingsLink := page.Locator("#nav-settings").First()
	require.NoError(t, settingsLink.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))

	// Click the settings icon to navigate to the settings page.
	require.NoError(t, settingsLink.Click())

	err = page.WaitForURL("**/dashboard/settings", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "There is no write path from this page")

	// autoPurgeTTL is a server-owned field: it renders read-only with the
	// server's effective value and there is no save action for it anymore.
	autoPurgeValue := assertReadOnlyField(t, page.Locator("#autoPurgeTTL"), "auto-purge TTL")
	gotAutoPurge, err := time.ParseDuration(autoPurgeValue)
	require.NoError(t, err)
	assert.Equal(t, want.AutoPurgeTTL, gotAutoPurge)

	// Since server-owned fields cannot be edited, merely arriving at the page
	// must not enable the save button (it only governs browser-local prefs).
	saveBtn := page.Locator("#save-settings-btn")
	require.NoError(t, saveBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	isDisabled, err := saveBtn.IsDisabled()
	require.NoError(t, err)
	require.True(t, isDisabled, "save button should stay disabled until a browser-local preference is changed")
}

// TestE2E_SettingsPage_ServerSettingsUnavailable pins the case where the
// endpoint responds successfully but the server has no ConfigManager wired
// up (the default in this test harness unless a test opts in), so the
// response has none of the expected fields.
func TestE2E_SettingsPage_ServerSettingsUnavailable(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	// Deliberately leave stack.Dashboard.ConfigManager nil.

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	notice := page.Locator("text=no configuration manager wired up")
	require.NoError(t, notice.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	// No server-owned fields should render in this state.
	count, err := page.Locator("#accountID").Count()
	require.NoError(t, err)
	assert.Zero(t, count, "no account ID field should render when the server has no config manager")
}

// TestE2E_SettingsPage_ServerSettingsUnreachable pins the case where the
// settings fetch itself fails (older server binary, network error): the
// page must show an explicit warning rather than silently rendering nothing
// or stale data.
func TestE2E_SettingsPage_ServerSettingsUnreachable(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	stack.Dashboard.ConfigManager = &e2eConfigManager{settings: testServerSettings()}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// Force the settings fetch to fail even though the config manager is
	// wired up, simulating a network error / unreachable server. The route
	// handler may run on a goroutine other than the test's own, so avoid
	// require/t.Fatal here (only safe from the test's goroutine) - if Abort
	// fails, the assertions below will simply fail instead.
	require.NoError(t, page.Route("**/dashboard/api/system/settings", func(route playwright.Route) {
		_ = route.Abort()
	}))

	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	notice := page.Locator("text=Could not reach")
	require.NoError(t, notice.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	count, err := page.Locator("#accountID").Count()
	require.NoError(t, err)
	assert.Zero(t, count, "no account ID field should render when the settings endpoint is unreachable")
}

// TestE2E_SettingsPage_SaveLocalPrefs_PersistsAfterRefresh replaces the old
// autoPurgeTTL save-then-persist test. autoPurgeTTL is now a server-owned,
// read-only field with no write path (see TestE2E_SettingsPage and
// TestE2E_SettingsPage_NavbarIcon), so that round trip no longer exists. The
// only fields this page can still save are the browser-local preferences
// (autoRefresh, refreshInterval, maxConsoleEntries) under the "Service
// Settings" tab, which persist to localStorage. This test repoints the
// save-then-persist coverage at refreshInterval to keep that localStorage
// round trip under test.
func TestE2E_SettingsPage_SaveLocalPrefs_PersistsAfterRefresh(t *testing.T) {
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

	_, err = page.Goto(server.URL + "/dashboard/settings")
	require.NoError(t, err)

	serviceTab := page.Locator("button:has-text('Service Settings')")
	require.NoError(t, serviceTab.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, serviceTab.Click())

	refreshIntervalInput := page.Locator("#refreshInterval")
	require.NoError(t, refreshIntervalInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	currentValue, err := refreshIntervalInput.InputValue()
	require.NoError(t, err)

	newValue := "37"
	if currentValue == newValue {
		newValue = "38"
	}

	require.NoError(t, refreshIntervalInput.Fill(newValue))

	saveBtn := page.Locator("#save-settings-btn")
	require.NoError(t, saveBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	isDisabled, err := saveBtn.IsDisabled()
	require.NoError(t, err)
	require.False(t, isDisabled, "save button should be enabled after changing refreshInterval")

	require.NoError(t, saveBtn.Click())

	successToast := page.Locator("#toast-container:has-text('updated')")
	require.NoError(t, successToast.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))

	_, err = page.Reload()
	require.NoError(t, err)

	require.NoError(t, serviceTab.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, serviceTab.Click())

	require.NoError(t, refreshIntervalInput.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))

	persistedValue, err := refreshIntervalInput.InputValue()
	require.NoError(t, err)

	wantInterval, err := strconv.Atoi(newValue)
	require.NoError(t, err)

	gotInterval, err := strconv.Atoi(persistedValue)
	require.NoError(t, err)
	assert.Equal(t, wantInterval, gotInterval)
}
