package azurearm_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func headerPolicy(name, expr string) string {
	return `<set-header name="` + name + `" exists-action="override"><value>` + expr + `</value></set-header>`
}

func TestAPIMExpr_Evaluator(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	cases := []struct{ name, expr, want string }{
		{"method", `@(context.Request.Method)`, "GET"},
		{"ternary", `@(context.Request.Method == "GET" ? "yes" : "no")`, "yes"},
		{"concat", `@("a" + "b" + 1)`, "ab1"},
		{"header default", `@(context.Request.Headers.GetValueOrDefault("X-In", "none"))`, "hello"},
		{"header missing", `@(context.Request.Headers.GetValueOrDefault("X-Missing", "none"))`, "none"},
		{"path", `@(context.Request.Url.Path)`, "/echo/items/7"},
		{"matched", `@(context.Request.MatchedParameters["id"])`, "7"},
		{"query", `@(context.Request.Url.Query.GetValueOrDefault("q", "d"))`, "z"},
		{"upper", `@(context.Request.Headers.GetValueOrDefault("X-In", "").ToUpper())`, "HELLO"},
		{"startswith", `@(context.Request.Headers.GetValueOrDefault("X-In", "").StartsWith("he"))`, "True"},
		{
			"boolean ops",
			`@(context.Request.Headers.GetValueOrDefault("X-In", "").EndsWith("lo") && ` +
				`!context.Request.Headers.GetValueOrDefault("X-In", "").Contains("zz"))`,
			"True",
		},
		{"int parse", `@(int.Parse(context.Request.MatchedParameters["id"]) + 5)`, "12"},
		{
			"ignore case",
			`@(context.Request.Url.Path.Equals("/ECHO/items/7", StringComparison.OrdinalIgnoreCase))`,
			"True",
		},
		{"api", `@(context.Api.Id)`, "echo"},
		{"operation", `@(context.Operation.Id)`, "get-item"},
		{"subscription", `@(context.Subscription.Name)`, "k1"},
		{"ip", `@(context.Request.IpAddress)`, "127.0.0.1"},
		{"contains key", `@(context.Variables.ContainsKey("nope"))`, "False"},
		{"clock", `@(DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ").StartsWith("2026-01-01T01:"))`, "True"},
		{"guid", `@(Guid.NewGuid().ToString().Length)`, "36"},
		{"variable", `@((string)context.Variables["v"])`, "abc"},
		{"typed variable", `@((int)context.Variables["n"] + 1)`, "43"},
		{"generic default", `@(context.Variables.GetValueOrDefault<int>("zz", 9))`, "9"},
		{"null coalesce", `@(context.Variables.GetValueOrDefault<string>("zz", null) ?? "fallback")`, "fallback"},
	}

	var in strings.Builder

	in.WriteString(`<set-variable name="v" value="abc" /><set-variable name="n" value="@(41 + 1)" />`)

	for i, c := range cases {
		in.WriteString(headerPolicy("X-E"+string(rune('a'+i)), c.expr))
	}

	e.policy(t, "/apis/echo", policyDoc(in.String(), "", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/7?q=z", "", map[string]string{"X-In": "hello"})
	require.Equal(t, http.StatusOK, status, body)

	got := decodeEcho(t, body)

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, []string{c.want}, got.Header["X-E"+string(rune('a'+i))], c.expr)
		})
	}
}

func TestAPIMExpr_RejectedAtPut(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	tests := []struct{ name, expr, want string }{
		{"foreign type", `@(System.IO.File.ReadAllText("x"))`, "'ReadAllText'"},
		{"multi statement", `@{ return "x"; }`, "Multi-statement"},
		{"body", `@(context.Request.Body.As<string>())`, "context.Request.Body"},
		{"unknown context member", `@(context.Foo)`, "'Foo'"},
		{"syntax", `@(1 +)`, "unexpected token"},
		{"new", `@(new Object())`, "unexpected token"},
		{"regex", `@(Regex.IsMatch("a", "b"))`, "'IsMatch'"},
		{"unknown method", `@(context.Request.Headers.Foo())`, "'Foo'"},
		{"too deep", `@(` + strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100) + `)`, "nested too deeply"},
		{"too long", `@("` + strings.Repeat("a", 5000) + `")`, "longer than"},
		{"unterminated", `@(context.Request.Method`, "Multi-statement"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := e.policy(t, "/apis/echo", policyDoc(headerPolicy("X-A", tc.expr), "", ""), http.StatusBadRequest)
			assert.Contains(t, body, "ValidationError")
			assert.Contains(t, body, tc.want)
		})
	}
}

func TestAPIMPolicy_ChooseValidation(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	tests := []struct{ name, doc, want string }{
		{"no when", `<choose />`, "at least one when"},
		{
			"otherwise first",
			`<choose><otherwise /><when condition="true" /></choose>`,
			"followed by at most one otherwise",
		},
		{"bad condition", `<choose><when condition="maybe" /></choose>`, "boolean expression or constant"},
		{"missing condition", `<choose><when /></choose>`, "'condition' is required"},
		{"base inside", `<choose><when condition="true"><base /></when></choose>`, "not allowed inside choose"},
		{"nested unsupported", `<choose><when condition="true"><json-to-xml /></when></choose>`, "json-to-xml"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := e.policy(t, "/apis/echo", policyDoc(tc.doc, "", ""), http.StatusBadRequest)
			assert.Contains(t, body, tc.want)
		})
	}
}

func TestAPIMGateway_Choose(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	doc := policyDoc(`
		<set-variable name="mobile"
			value="@(context.Request.Headers.GetValueOrDefault("User-Agent", "").Contains("iPhone"))" />
		<choose>
			<when condition="@(context.Variables.GetValueOrDefault<bool>("mobile"))">`+headerPolicy("X-Kind", "mobile")+`</when>
			<when condition="@(context.Request.Url.Query.ContainsKey("beta"))">
				<choose><when condition="true">`+headerPolicy("X-Kind", "beta")+`</when></choose>
			</when>
			<otherwise>`+headerPolicy("X-Kind", "desktop")+`</otherwise>
		</choose>`, "", "")
	e.policy(t, "/apis/echo", doc, http.StatusCreated)

	tests := []struct {
		name, path, ua, want string
	}{
		{"mobile", "/echo/items/1", "iPhone", "mobile"},
		{"nested beta", "/echo/items/1?beta=1", "curl", "beta"},
		{"otherwise", "/echo/items/1", "curl", "desktop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			status, body, _ := e.call(t, "GET", tc.path, "", map[string]string{"User-Agent": tc.ua})
			require.Equal(t, http.StatusOK, status, body)
			assert.Equal(t, []string{tc.want}, decodeEcho(t, body).Header["X-Kind"])
		})
	}
}

func TestAPIMGateway_ChooseReturnResponse(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<choose><when condition="@(context.Request.Url.Query.ContainsKey("deny"))">
		<return-response><set-status code="403" reason="Forbidden" /><set-body>nope</set-body></return-response>
	</when></choose>`, "", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/1?deny=1", "", nil)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Equal(t, "nope", body)

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, http.StatusOK, status)
}

func TestAPIMGateway_OnError(t *testing.T) {
	t.Parallel()

	onErr := `<set-header name="X-Source" exists-action="override"><value>@(context.LastError.Source)</value></set-header>
		<set-header name="X-Reason" exists-action="override"><value>@(context.LastError.Reason)</value></set-header>
		<set-header name="X-Section" exists-action="override"><value>@(context.LastError.Section)</value></set-header>
		<set-header name="X-Scope" exists-action="override"><value>@(context.LastError.Scope)</value></set-header>
		<set-header name="X-Status" exists-action="override">
			<value>@(context.Response.StatusCode.ToString())</value></set-header>`

	t.Run("rate limit", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		e.policy(
			t,
			"/apis/echo",
			"<policies><inbound><base /><rate-limit calls=\"1\" renewal-period=\"60\" /></inbound>"+
				"<backend><base /></backend><outbound><base /></outbound><on-error><base />"+onErr+"</on-error></policies>",
			http.StatusCreated,
		)

		status, _, _ := e.call(t, "GET", "/echo/items/1", "", nil)
		require.Equal(t, http.StatusOK, status)

		status, body, hdr := e.call(t, "GET", "/echo/items/1", "", nil)
		assert.Equal(t, http.StatusTooManyRequests, status, body)
		assert.Equal(t, "rate-limit", hdr.Get("X-Source"))
		assert.Equal(t, "RateLimitExceeded", hdr.Get("X-Reason"))
		assert.Equal(t, "inbound", hdr.Get("X-Section"))
		assert.Equal(t, "api", hdr.Get("X-Scope"))
		assert.Equal(t, "429", hdr.Get("X-Status"))
	})

	t.Run("expression failure", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		e.policy(
			t,
			"/apis/echo",
			"<policies><inbound><base /><set-variable name=\"x\" value=\"@(context.Variables[\"missing\"])\" /></inbound>"+
				"<backend><base /></backend><outbound><base /></outbound><on-error><base />"+onErr+"</on-error></policies>",
			http.StatusCreated,
		)

		status, body, hdr := e.call(t, "GET", "/echo/items/1", "", nil)
		assert.Equal(t, http.StatusInternalServerError, status, body)
		assert.Equal(t, "set-variable", hdr.Get("X-Source"))
		assert.Equal(t, "ExpressionValueEvaluationFailure", hdr.Get("X-Reason"))
	})

	t.Run("return response replaces error", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		e.policy(
			t,
			"/apis/echo",
			"<policies><inbound><base /><rate-limit calls=\"1\" renewal-period=\"60\" /></inbound>"+
				"<backend><base /></backend><outbound><base /></outbound><on-error>"+
				`<return-response><set-status code="299" reason="x" />`+
				`<set-body>@("handled:" + context.LastError.Reason)</set-body></return-response>`+
				"</on-error></policies>",
			http.StatusCreated,
		)

		e.call(t, "GET", "/echo/items/1", "", nil)

		status, body, _ := e.call(t, "GET", "/echo/items/1", "", nil)
		assert.Equal(t, 299, status)
		assert.Equal(t, "handled:RateLimitExceeded", body)
	})

	t.Run("backend failure", func(t *testing.T) {
		t.Parallel()

		e := newGWEnv(t)
		e.putAPI(t, "dead", "dead", "http://example.invalid")
		e.putOp(t, "dead", "g", "get", "/x")
		e.policy(
			t,
			"/apis/dead",
			"<policies><inbound><base /></inbound><backend><base /></backend><outbound><base /></outbound>"+
				"<on-error><base />"+onErr+"</on-error></policies>",
			http.StatusCreated,
		)

		status, _, hdr := e.call(t, "GET", "/dead/x", "", nil)
		assert.Equal(t, http.StatusBadGateway, status)
		assert.Equal(t, "forward-request", hdr.Get("X-Source"))
		assert.Equal(t, "BackendConnectionFailure", hdr.Get("X-Reason"))
		assert.Equal(t, "backend", hdr.Get("X-Section"))
	})
}

func TestAPIMGateway_ProductScopePolicy(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putProduct(t, "gold", false)
	e.linkProductAPI(t, "gold", "echo")
	key := e.productSubscription(t, "goldsub", "gold")

	e.policy(t, "", policyDoc(headerPolicy("X-Global", "g"), "", ""), http.StatusCreated)
	e.policy(
		t,
		"/products/gold",
		policyDoc(headerPolicy("X-Product", `@(context.Product.Id)`), "", ""),
		http.StatusCreated,
	)
	e.policy(t, "/apis/echo", policyDoc(headerPolicy("X-Api", "a"), "", ""), http.StatusCreated)
	e.policy(t, "/apis/echo/operations/get-item", policyDoc(headerPolicy("X-Op", "o"), "", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Ocp-Apim-Subscription-Key": key})
	require.Equal(t, http.StatusOK, status, body)

	h := decodeEcho(t, body).Header
	assert.Equal(t, []string{"g"}, h["X-Global"])
	assert.Equal(t, []string{"gold"}, h["X-Product"])
	assert.Equal(t, []string{"a"}, h["X-Api"])
	assert.Equal(t, []string{"o"}, h["X-Op"])

	status, body, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Empty(t, decodeEcho(t, body).Header["X-Product"], "the all-APIs subscription belongs to no product")
}

func (e *gwEnv) putProduct(t *testing.T, id string, subscriptionRequired bool) {
	t.Helper()

	_, err := e.f.NewProductClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, id, armapimanagement.ProductContract{
		Properties: &armapimanagement.ProductContractProperties{
			DisplayName: new(id), SubscriptionRequired: new(subscriptionRequired),
			State: new(armapimanagement.ProductStatePublished),
		},
	}, nil)
	require.NoError(t, err)
}

func (e *gwEnv) linkProductAPI(t *testing.T, product, api string) {
	t.Helper()

	_, err := e.f.NewProductAPIClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, product, api, nil)
	require.NoError(t, err)
}

func (e *gwEnv) productSubscription(t *testing.T, id, product string) string {
	t.Helper()

	_, err := e.f.NewSubscriptionClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, id,
		armapimanagement.SubscriptionCreateParameters{
			Properties: &armapimanagement.SubscriptionCreateParameterProperties{
				DisplayName: new(id), Scope: new("/products/" + product),
				State: new(armapimanagement.SubscriptionStateActive),
			},
		}, nil)
	require.NoError(t, err)

	sec, err := e.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, id, nil)
	require.NoError(t, err)

	return *sec.PrimaryKey
}
