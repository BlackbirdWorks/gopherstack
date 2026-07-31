//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// credentialScopeRe extracts the region and service segments from a SigV4
// Authorization header of the form:
//
//	AWS4-HMAC-SHA256 Credential=test/20260730/us-east-1/redshift/aws4_request, SignedHeaders=..., Signature=...
//
// The region segment is fed straight from the region Provider passed into
// the AWS SDK client config (see clientConfig() in ui/src/lib/aws-client.ts),
// so it is the decisive signal for whether a request actually honored the
// dashboard's region selector. (X-Amz-Region is not sent by the JS SDK; the
// credential scope is the only place the resolved signing region shows up on
// the wire.)
var credentialScopeRe = regexp.MustCompile(`Credential=[^/]+/\d{8}/([a-z0-9-]+)/([a-z0-9-]+)/aws4_request`)

// signedRequest is a snapshot of one signed AWS API call the browser issued,
// as observed from its outgoing Authorization header.
type signedRequest struct {
	region  string
	service string
}

// requestCapture collects signed AWS API requests for a single service as a
// dashboard page issues them. Assertions poll it after triggering a UI
// action instead of racing the network directly.
type requestCapture struct {
	service  string
	requests []signedRequest
	mu       sync.Mutex
}

func newRequestCapture(service string) *requestCapture {
	return &requestCapture{service: service}
}

// onRequest is registered via Page.OnRequest. It is invoked on Playwright's
// internal goroutine, so it must only touch the mutex-guarded slice.
func (c *requestCapture) onRequest(req playwright.Request) {
	auth, ok := req.Headers()["authorization"]
	if !ok {
		return
	}

	m := credentialScopeRe.FindStringSubmatch(auth)
	if m == nil || m[2] != c.service {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, signedRequest{region: m[1], service: m[2]})
}

func (c *requestCapture) snapshot() []signedRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]signedRequest, len(c.requests))
	copy(out, c.requests)

	return out
}

// waitForRequestCount polls the capture until at least n signed requests to
// its service have been observed, or fails the test after a timeout. It
// returns the requests seen at that point.
func waitForRequestCount(t *testing.T, c *requestCapture, n int, what string) []signedRequest {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		got := c.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "timed out waiting for signed requests",
				"%s: wanted at least %d signed %s request(s), saw %d", what, n, c.service, len(got))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForRedshiftPage waits for the Redshift dashboard page to finish
// mounting after a navigation.
func waitForRedshiftPage(t *testing.T, page playwright.Page) {
	t.Helper()

	require.NoError(t, page.Locator("h1:has-text('Amazon Redshift')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))
}

// switchRegion drives the header's region dropdown exactly as a user would:
// open it and click the target region option. This is preferred over poking
// localStorage directly because it exercises the real user path (dropdown ->
// setStoredRegion -> regionProvider) end to end.
func switchRegion(t *testing.T, page playwright.Page, region string) {
	t.Helper()

	regionButton := page.Locator("button[title='Switch region']")
	require.NoError(t, regionButton.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, regionButton.Click())

	option := page.Locator("button", playwright.PageLocatorOptions{HasText: region}).First()
	require.NoError(t, option.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	}))
	require.NoError(t, option.Click())

	// The button's own label reflects the active region, so waiting for it
	// to update is a reliable signal that setStoredRegion has taken effect
	// (and thus localStorage is persisted) before the caller proceeds.
	require.NoError(t, page.Locator("button[title='Switch region']:has-text('"+region+"')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(5000),
		},
	))
}

// TestE2E_Region_ClientConstructionHonorsSelector pins commit 8ecdb9127
// ("make the region selector actually reach every service page"):
// clientConfig() in ui/src/lib/aws-client.ts now passes
// `region: region ?? regionProvider`, where regionProvider reads the region
// persisted by the header dropdown (ui/src/lib/region.svelte.ts). Before
// that fix, clientConfig() hardcoded region to the literal string
// "us-east-1", so every one of the ~150 getXClient() factories that call it
// with no explicit region argument stayed pinned to us-east-1 forever,
// regardless of what the header dropdown said — even on a page visited for
// the very first time after picking a different region.
//
// The Redshift dashboard page is used as the probe because:
//   - it is not one of S3/DynamoDB/DAX, the five pages that already read the
//     region correctly before this fix (they would pass even if the fix
//     were reverted, so they don't pin anything);
//   - unlike GuardDuty, its backend is actually wired into the e2e test
//     stack (see internal/teststack/teststack.go) — GuardDuty has no
//     handler registered there and the page would just fail to load;
//   - its data load is a single DescribeClustersCommand issued once on
//     mount, the simplest "list on load" shape available among the
//     remaining candidates.
//
// The decisive signal is the SigV4 credential scope in the request's
// Authorization header
// (Credential=test/<date>/<region>/<service>/aws4_request): that region
// segment is what the Provider fed the signer for that request. A real
// captured header looks like:
//
//	AWS4-HMAC-SHA256 Credential=test/20260731/us-east-1/redshift/aws4_request,
//	SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-length;content-type;
//	host;x-amz-content-sha256;x-amz-date;x-amz-user-agent,
//	Signature=da36181ae7f0de0e7fe7d5e58085fa9d49f17e348924e2f96a8ddf3f10e99df1
//
// Region switching is driven through the real header dropdown (the
// preferred path per the task, since it exercises dropdown -> localStorage
// -> regionProvider end to end), then proven by re-navigating to the same
// route with a real browser navigation rather than clicking the page's own
// "Refresh" button. That deviates from "without a manual refresh click", and
// the reason is a genuine, separate finding from writing this test (see
// below), not a shortcut: clicking Refresh reuses the SAME already-signed
// client instance the page created once at module scope
// (`const redshift = getRedshiftClient();`), and re-navigating constructs a
// brand new one, which is what actually exercises the code path this commit
// touched (clientConfig() reading the region at *construction* time).
//
// KNOWN GAP FOUND WHILE WRITING THIS TEST (not fixed here, reported
// separately): switching the region on an already-mounted page and then
// triggering a refetch on the SAME client instance — via its own Refresh
// button, or automatically via onRegionChange — does NOT pick up the new
// region. This reproduces identically on dynamodb/+page.svelte, one of the
// five pages this test deliberately avoids because they're assumed to
// "already work". The root cause lives three layers into the vendored AWS
// SDK, not in this repo's code:
// ui/node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/resolveAwsSdkSigV4Config.js,
// in the `signer` closure: `config.signingRegion = config.signingRegion ||
// signingRegion;` — this mutates the shared, per-client `config` object and
// only ever sets `signingRegion` on the FIRST signed request; every
// subsequent request on that same client instance keeps reusing that first
// value no matter what the (correctly, freshly re-evaluated) region
// Provider now returns. In other words: a client's *first* request always
// honors whatever region was selected at that moment (which is what this
// test proves and what was actually broken), but the same client is not
// truly "live-reactive" to later in-place region switches the way the
// commit message describes — only re-mounting the page (a fresh
// getXClient() call) picks up a later change. This test does not assert the
// broken in-place-refetch behavior (that would pin a bug as if it were a
// spec), and instead sticks to the part of the fix that is real and
// verified.
func TestE2E_Region_ClientConstructionHonorsSelector(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	capture := newRequestCapture("redshift")
	page.OnRequest(capture.onRequest)

	_, err = page.Goto(server.URL + "/dashboard/redshift")
	require.NoError(t, err)
	waitForRedshiftPage(t, page)

	initial := waitForRequestCount(t, capture, 1, "initial page load, before any region selection")
	assert.Equal(t, "us-east-1", initial[0].region,
		"the default region (no selection made yet) must be us-east-1")

	switchRegion(t, page, "eu-west-2")

	// Re-navigate to the same route with a real browser navigation. This
	// tears down the page's JS realm and reconstructs it, so
	// `const redshift = getRedshiftClient();` at module scope runs again,
	// exercising clientConfig() reading the *currently selected* region at
	// construction time instead of a hardcoded "us-east-1" literal.
	_, err = page.Goto(server.URL + "/dashboard/redshift")
	require.NoError(t, err)
	waitForRedshiftPage(t, page)

	afterNav := waitForRequestCount(t, capture, 2, "after re-navigating with eu-west-2 selected")
	last := afterNav[len(afterNav)-1]
	assert.Equal(t, "eu-west-2", last.region,
		"a freshly constructed Redshift client must sign for the currently selected region, not a hardcoded us-east-1")
}

// waitForDynamoDBPage waits for the DynamoDB dashboard page's table-list view
// (the `{:else}` branch of the top-level `{#if showTableDetails}`) to finish
// mounting after a navigation.
func waitForDynamoDBPage(t *testing.T, page playwright.Page) {
	t.Helper()

	require.NoError(t, page.Locator("h1:has-text('DynamoDB Tables')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))
}

// TestE2E_Region_MidSessionSwitchRefetchesWithNewRegion closes the gap that
// TestE2E_Region_ClientConstructionHonorsSelector's doc comment deliberately
// left open and flagged as a known, separately-tracked bug (gopherstack-ks2s.6):
// switching the region on an already-mounted page and letting the page's OWN
// refetch run — via `onRegionChange`, with NO page reload and no manual
// "Refresh" click — must sign that refetch's request for the newly selected
// region, not the region the page's client was first constructed with.
//
// Root cause (verified against the vendored SDK, not just inferred): in
// ui/node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/resolveAwsSdkSigV4Config.js,
// the `signer` closure re-evaluates the region Provider correctly on every
// call (`signingRegion: await normalizeProvider(config.region)()`), but then
// does `config.signingRegion = config.signingRegion || signingRegion;` and
// hands the *signer* `region: config.signingRegion` — reading back the
// memoized field it just conditionally wrote, not the freshly computed local
// value. `config.signingRegion` is set once, on a client's first signed
// request, and every later request on that SAME client instance keeps
// signing for that first region no matter what the Provider now returns.
// (The sibling `regionInfoProvider` branch a few lines up has the identical
// bug shape, but is dead code here: no `aws-client.ts` factory sets
// `regionInfoProvider`.) The fix lives in
// ui/src/lib/region-effect.svelte.ts's `regionalClient()`: it rebuilds the
// client (via a `$derived`, so the rebuild happens lazily on next access)
// whenever the region changes, instead of letting a page keep signing
// requests on one long-lived client instance forever.
//
// DynamoDB is the probe page because:
//   - it is explicitly named in TestE2E_Region_ClientConstructionHonorsSelector's
//     comment as where this was reproduced live, so this test closes that
//     exact loop;
//   - its backend is wired into the e2e test stack (unlike DAX, which has no
//     handler registered in internal/teststack/teststack.go, and unlike
//     GuardDuty per the sibling test's comment);
//   - a fresh stack starts with zero tables, so `loadTables()` issues exactly
//     one ListTables request per mount/refetch (no per-table DescribeTable
//     fan-out to complicate request counting).
func TestE2E_Region_MidSessionSwitchRefetchesWithNewRegion(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	capture := newRequestCapture("dynamodb")
	page.OnRequest(capture.onRequest)

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForDynamoDBPage(t, page)

	initial := waitForRequestCount(t, capture, 1, "initial page load, before any region selection")
	assert.Equal(t, "us-east-1", initial[0].region,
		"the default region (no selection made yet) must be us-east-1")

	// Switch region via the header dropdown WITHOUT navigating away and
	// without clicking any in-page "Refresh" control. The DynamoDB page
	// already wires `onRegionChange(loadTables)`, so the switch alone must
	// be enough to trigger a second ListTables request.
	switchRegion(t, page, "eu-west-2")

	afterSwitch := waitForRequestCount(t, capture, 2, "after switching to eu-west-2 with no reload")
	last := afterSwitch[len(afterSwitch)-1]
	assert.Equal(t, "eu-west-2", last.region,
		"the page's own onRegionChange refetch must sign for the newly selected region, "+
			"not the region its client was first constructed with")
}
