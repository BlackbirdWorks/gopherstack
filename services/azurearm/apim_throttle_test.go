package azurearm_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMGateway_RateLimitByKey(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<rate-limit-by-key calls="2" renewal-period="60"
		counter-key="@(context.Request.Headers.GetValueOrDefault(&quot;X-User&quot;, &quot;anon&quot;))"
		remaining-calls-header-name="X-Left" total-calls-header-name="X-Total"
		retry-after-header-name="X-Retry" />`, "", ""), http.StatusCreated)

	call := func(user string) (int, http.Header) {
		status, _, hdr := e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-User": user})

		return status, hdr
	}

	status, hdr := call("alice")
	assert.Equal(t, 200, status)
	assert.Equal(t, "1", hdr.Get("X-Left"))
	assert.Equal(t, "2", hdr.Get("X-Total"))

	call("alice")
	e.clock.Advance(20 * time.Second)

	status, hdr = call("alice")
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.Equal(t, "40", hdr.Get("X-Retry"))

	status, _ = call("bob")
	assert.Equal(t, 200, status, "counters are per key")

	e.clock.Advance(41 * time.Second)

	status, _ = call("alice")
	assert.Equal(t, 200, status, "window renewed")
}

func TestAPIMGateway_RateLimitByKeyPostponedIncrement(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<rate-limit-by-key calls="1" renewal-period="60" counter-key="k"
		increment-condition="@(context.Response.StatusCode == 200)" />`, "", ""), http.StatusCreated)

	status, _, _ := e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, 200, status)

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, http.StatusTooManyRequests, status)
}

func TestAPIMGateway_QuotaByKey(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<quota-by-key calls="2" renewal-period="3600"
		counter-key="@(context.Request.Headers.GetValueOrDefault(&quot;X-User&quot;, &quot;anon&quot;))"
		increment-condition="@(context.Request.Headers.GetValueOrDefault(&quot;X-Count&quot;, &quot;1&quot;) ==
			&quot;1&quot;)"
		first-period-start="2026-01-01T00:00:00Z" />`, "", ""), http.StatusCreated)

	call := func(user, count string) (int, string, http.Header) {
		return e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-User": user, "X-Count": count})
	}

	for range 3 {
		status, _, _ := call("u", "0")
		require.Equal(t, 200, status, "uncounted calls never exhaust the quota")
	}

	call("u", "1")
	call("u", "1")

	e.clock.Advance(30 * time.Minute)

	status, body, hdr := call("u", "1")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "Out of call volume quota. Quota will be replenished in 00:")
	assert.NotEmpty(t, hdr.Get("Retry-After"))

	status, _, _ = call("other", "1")
	assert.Equal(t, 200, status)

	e.clock.Advance(time.Hour)

	status, _, _ = call("u", "1")
	assert.Equal(t, 200, status, "the fixed window renewed")
}

func TestAPIMGateway_QuotaByKeyBandwidth(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<quota-by-key bandwidth="1" renewal-period="0" counter-key="bw" />`, "", ""),
		http.StatusCreated)

	status, _, _ := e.call(t, "POST", "/echo/items", strings.Repeat("x", 2048), nil)
	require.Equal(t, 200, status)

	status, body, hdr := e.call(t, "POST", "/echo/items", "x", nil)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, `"message":"Out of bandwidth quota."`)
	assert.Empty(t, hdr.Get("Retry-After"), "an infinite period has no renewal")
}

func TestAPIMGateway_QuotaProductScope(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putProduct(t, "gold", true)
	e.linkProductAPI(t, "gold", "echo")
	k1 := e.productSubscription(t, "s1", "gold")
	k2 := e.productSubscription(t, "s2", "gold")

	e.policy(t, "/products/gold", policyDoc(`<quota calls="3" renewal-period="3600">
		<api id="echo" calls="1" renewal-period="3600" /></quota>`, "", ""), http.StatusCreated)

	status, _, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Ocp-Apim-Subscription-Key": k1})
	require.Equal(t, 200, status)

	status, body, hdr := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Ocp-Apim-Subscription-Key": k1})
	assert.Equal(t, http.StatusForbidden, status, "the nested api quota of 1 call applies independently")
	assert.Contains(t, body, "Out of call volume quota. Quota will be replenished in 0")
	assert.NotEmpty(t, hdr.Get("Retry-After"))

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", map[string]string{"Ocp-Apim-Subscription-Key": k2})
	assert.Equal(t, 200, status, "quotas are per subscription")
}

func TestAPIMThrottle_Validation(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putProduct(t, "gold", true)

	tests := []struct {
		name, scope, stmt, want string
	}{
		{"quota at api scope", "/apis/echo", `<quota calls="1" renewal-period="60" />`, "product scope only"},
		{"quota no limit", "/products/gold", `<quota renewal-period="60" />`, "Either 'calls' or 'bandwidth'"},
		{"quota no period", "/products/gold", `<quota calls="1" />`, "'renewal-period' is required"},
		{
			"quota-by-key short period",
			"/apis/echo",
			`<quota-by-key calls="1" renewal-period="100" counter-key="k" />`,
			"'renewal-period'",
		},
		{
			"quota-by-key no key",
			"/apis/echo",
			`<quota-by-key calls="1" renewal-period="300" />`,
			"'counter-key' is required",
		},
		{
			"rlbk long period",
			"/apis/echo",
			`<rate-limit-by-key calls="1" renewal-period="301" counter-key="k" />`,
			"'renewal-period'",
		},
		{
			"rlbk bad expression",
			"/apis/echo",
			`<rate-limit-by-key calls="1" renewal-period="1" counter-key="@(nope)" />`,
			"'nope'",
		},
		{
			"quota api no id",
			"/products/gold",
			`<quota calls="1" renewal-period="60"><api calls="1" renewal-period="1" /></quota>`,
			"'name' or 'id'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := e.policy(t, tc.scope, policyDoc(tc.stmt, "", ""), http.StatusBadRequest)
			assert.Contains(t, body, tc.want)
		})
	}
}
