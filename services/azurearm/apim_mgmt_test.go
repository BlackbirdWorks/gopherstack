package azurearm_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"software.sslmate.com/src/go-pkcs12"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func testCert(t *testing.T, cn string, notAfter time.Time) ([]byte, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: cn},
		NotBefore: notAfter.Add(-time.Hour), NotAfter: notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, cert, nil, "pw")
	require.NoError(t, err)

	return der, pfx
}

func TestAPIMBuiltins(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putAPI(t, "stp", "stp", e.backend.URL)
	e.putOp(t, "stp", "g", "get", "/x")
	e.linkProductAPI(t, "starter", "stp")

	groups := map[string]bool{}
	pager := e.f.NewGroupClient().NewListByServicePager(apimRG, apimSvc, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, g := range page.Value {
			assert.Equal(t, armapimanagement.GroupTypeSystem, *g.Properties.Type)
			assert.True(t, *g.Properties.BuiltIn)

			groups[*g.Name] = true
		}
	}

	assert.Equal(t, map[string]bool{"administrators": true, "developers": true, "guests": true}, groups)

	prods := map[string]string{}
	pp := e.f.NewProductClient().NewListByServicePager(apimRG, apimSvc, nil)

	for pp.More() {
		page, err := pp.NextPage(t.Context())
		require.NoError(t, err)

		for _, p := range page.Value {
			prods[*p.Name] = *p.Properties.DisplayName
			assert.Equal(t, armapimanagement.ProductStatePublished, *p.Properties.State)
		}
	}

	assert.Equal(t, map[string]string{"starter": "Starter", "unlimited": "Unlimited"}, prods)

	master, err := e.f.NewSubscriptionClient().Get(t.Context(), apimRG, apimSvc, "master", nil)
	require.NoError(t, err)
	assert.Equal(t, "Built-in all-access subscription", *master.Properties.DisplayName)
	assert.Equal(t, armapimanagement.SubscriptionStateActive, *master.Properties.State)
	assert.True(t, strings.HasSuffix(*master.Properties.Scope, "/"+apimSvc+"/"), *master.Properties.Scope)

	sec, err := e.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, "master", nil)
	require.NoError(t, err)

	status, body, _ := e.call(t, "GET", "/stp/x", "", map[string]string{"Ocp-Apim-Subscription-Key": *sec.PrimaryKey})
	assert.Equal(t, http.StatusOK, status, body)

	status, body, _ = e.raw(
		t,
		http.MethodDelete,
		e.armPath("/subscriptions/master"),
		"",
		map[string]string{"If-Match": "*"},
	)
	assert.Equal(t, http.StatusBadRequest, status, body)

	status, body, _ = e.raw(t, http.MethodDelete, e.armPath("/groups/guests"), "", map[string]string{"If-Match": "*"})
	assert.Equal(t, http.StatusBadRequest, status, body)
}

func TestAPIMBuiltins_StarterPolicyEnforced(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.putAPI(t, "stp", "stp", e.backend.URL)
	e.putOp(t, "stp", "g", "get", "/x")
	e.linkProductAPI(t, "starter", "stp")

	key := e.productSubscription(t, "ss", "starter")

	for i := range 5 {
		status, body, _ := e.call(t, "GET", "/stp/x", "", map[string]string{"Ocp-Apim-Subscription-Key": key})
		require.Equal(t, http.StatusOK, status, "call %d: %s", i, body)
	}

	status, body, _ := e.call(t, "GET", "/stp/x", "", map[string]string{"Ocp-Apim-Subscription-Key": key})
	assert.Equal(t, http.StatusTooManyRequests, status, "the Starter product allows 5 calls a minute: %s", body)
}

func TestAPIMBuiltins_ConsumptionHasNoClassicProducts(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.createService(t, apimSvc, armapimanagement.SKUTypeConsumption, 0)

	n := 0
	pager := s.f.NewProductClient().NewListByServicePager(apimRG, apimSvc, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		n += len(page.Value)
	}

	assert.Zero(t, n)

	_, err := s.f.NewSubscriptionClient().Get(t.Context(), apimRG, apimSvc, "master", nil)
	require.NoError(t, err)
}

func TestAPIMCertificates(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	notAfter := time.Date(2030, 5, 6, 7, 8, 9, 0, time.UTC)
	der, pfx := testCert(t, "gw.example", notAfter)
	sum := sha1.Sum(der)
	wantThumb := strings.ToUpper(hex.EncodeToString(sum[:]))

	cc := e.f.NewCertificateClient()
	put := func(id, data, pw string) (armapimanagement.CertificateClientCreateOrUpdateResponse, error) {
		return cc.CreateOrUpdate(t.Context(), apimRG, apimSvc, id, armapimanagement.CertificateCreateOrUpdateParameters{
			Properties: &armapimanagement.CertificateCreateOrUpdateProperties{Data: &data, Password: &pw},
		}, nil)
	}

	tests := []struct {
		name, id, data, pw string
		wantErr            bool
	}{
		{"pfx", "c1", base64.StdEncoding.EncodeToString(pfx), "pw", false},
		{"cer", "c2", base64.StdEncoding.EncodeToString(der), "", false},
		{"wrong password", "c3", base64.StdEncoding.EncodeToString(pfx), "nope", true},
		{"not base64", "c4", "!!!", "", true},
		{"garbage", "c5", base64.StdEncoding.EncodeToString([]byte("not a certificate")), "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res, err := put(tc.id, tc.data, tc.pw)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, wantThumb, *res.Properties.Thumbprint)
			assert.Equal(t, "CN=gw.example", *res.Properties.Subject)
			assert.True(t, notAfter.Equal(*res.Properties.ExpirationDate), "%v", res.Properties.ExpirationDate)
		})
	}
}

func TestAPIMCertificates_NeverEchoed(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	_, pfx := testCert(t, "gw.example", time.Date(2030, 5, 6, 7, 8, 9, 0, time.UTC))
	data := base64.StdEncoding.EncodeToString(pfx)

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/certificates/c1"),
		`{"properties":{"data":"`+data+`","password":"pw"}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	status, body, _ = e.raw(t, http.MethodGet, e.armPath("/certificates/c1"), "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.NotContains(t, body, data, "the uploaded data is never echoed")
	assert.NotContains(t, body, `"password"`)

	status, body, _ = e.raw(t, http.MethodPut, e.armPath("/certificates/c1"),
		`{"properties":{"keyVault":{"secretIdentifier":"https://v/secrets/s"}}}`, nil)
	assert.Equal(t, http.StatusBadRequest, status, body)
}

func TestAPIMDiagnostics(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/loggers/ai"),
		`{"properties":{"loggerType":"applicationInsights","credentials":{"instrumentationKey":"k"}}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	tests := []struct {
		name, path, body string
		want             int
	}{
		{
			"service ok",
			"/diagnostics/applicationinsights",
			`{"properties":{"loggerId":"/loggers/ai","alwaysLog":"allErrors",` +
				`"sampling":{"samplingType":"fixed","percentage":50},"verbosity":"error","logClientIp":true,` +
				`"httpCorrelationProtocol":"W3C","operationNameFormat":"Url",` +
				`"frontend":{"request":{"headers":["a"],"body":{"bytes":100}}}}}`,
			http.StatusCreated,
		},
		{
			"api ok",
			"/apis/echo/diagnostics/applicationinsights",
			`{"properties":{"loggerId":"/loggers/ai"}}`,
			http.StatusCreated,
		},
		{"no logger id", "/diagnostics/d1", `{"properties":{}}`, http.StatusBadRequest},
		{"unknown logger", "/diagnostics/d2", `{"properties":{"loggerId":"/loggers/ghost"}}`, http.StatusBadRequest},
		{
			"bad sampling",
			"/diagnostics/d3",
			`{"properties":{"loggerId":"/loggers/ai","sampling":{"percentage":101}}}`,
			http.StatusBadRequest,
		},
		{
			"bad verbosity",
			"/diagnostics/d4",
			`{"properties":{"loggerId":"/loggers/ai","verbosity":"loud"}}`,
			http.StatusBadRequest,
		},
		{
			"bad body bytes",
			"/diagnostics/d5",
			`{"properties":{"loggerId":"/loggers/ai","backend":{"response":{"body":{"bytes":9000}}}}}`,
			http.StatusBadRequest,
		},
		{
			"api unknown",
			"/apis/ghost/diagnostics/applicationinsights",
			`{"properties":{"loggerId":"/loggers/ai"}}`,
			http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st, resp, _ := e.raw(t, http.MethodPut, e.armPath(tc.path), tc.body, nil)
			assert.Equal(t, tc.want, st, resp)
		})
	}
}

func TestAPIMSchemas(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	tests := []struct {
		name, path, body string
		want             int
	}{
		{
			"global json",
			"/schemas/s1",
			`{"properties":{"schemaType":"json","value":"{\"type\":\"object\"}"}}`,
			http.StatusCreated,
		},
		{
			"global xml",
			"/schemas/s2",
			`{"properties":{"schemaType":"xml","value":"<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"/>"}}`,
			http.StatusCreated,
		},
		{
			"global bad json",
			"/schemas/s3",
			`{"properties":{"schemaType":"json","value":"{nope"}}`,
			http.StatusBadRequest,
		},
		{"global bad xml", "/schemas/s4", `{"properties":{"schemaType":"xml","value":"<a>"}}`, http.StatusBadRequest},
		{"global bad type", "/schemas/s5", `{"properties":{"schemaType":"yaml","value":"x"}}`, http.StatusBadRequest},
		{"global no body", "/schemas/s6", `{"properties":{"schemaType":"json"}}`, http.StatusBadRequest},
		{
			"api openapi",
			"/apis/echo/schemas/a1",
			`{"properties":{"contentType":"application/vnd.oai.openapi.components+json",` +
				`"document":{"components":{"schemas":{"X":{"type":"string"}}}}}}`,
			http.StatusCreated,
		},
		{
			"api graphql",
			"/apis/echo/schemas/a2",
			`{"properties":{"contentType":"application/vnd.ms-azure-apim.graphql.schema",` +
				`"document":{"value":"type Query { a: Int }"}}}`,
			http.StatusCreated,
		},
		{
			"api no document",
			"/apis/echo/schemas/a3",
			`{"properties":{"contentType":"application/json"}}`,
			http.StatusBadRequest,
		},
		{
			"api bad content type",
			"/apis/echo/schemas/a4",
			`{"properties":{"contentType":"nonsense","document":{"value":"x"}}}`,
			http.StatusBadRequest,
		},
		{
			"api bad xml",
			"/apis/echo/schemas/a5",
			`{"properties":{"contentType":"application/vnd.ms-azure-apim.xsd+xml","document":{"value":"<a"}}}`,
			http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st, resp, _ := e.raw(t, http.MethodPut, e.armPath(tc.path), tc.body, nil)
			assert.Equal(t, tc.want, st, resp)
		})
	}
}

func TestAPIMRevisionsAndReleases(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	cb := newCountingBackend(t)

	e.putAPI(t, "echo;rev=2", "echo", cb.srv.URL)
	e.putOp(t, "echo;rev=2", "get-item", "get", "/items/{id}")

	var revs []armapimanagement.APIRevisionContract

	list := func() {
		revs = nil
		pager := e.f.NewAPIRevisionClient().NewListByServicePager(apimRG, apimSvc, "echo", nil)

		for pager.More() {
			page, err := pager.NextPage(t.Context())
			require.NoError(t, err)

			for _, r := range page.Value {
				revs = append(revs, *r)
			}
		}
	}

	list()
	require.Len(t, revs, 2)
	assert.Equal(t, "1", *revs[0].APIRevision)
	assert.True(t, *revs[0].IsCurrent)
	assert.Equal(t, "2", *revs[1].APIRevision)
	assert.False(t, *revs[1].IsCurrent)
	assert.Equal(t, "/apis/echo;rev=2", *revs[1].APIID)

	status, body, _ := e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "/base/items/1", decodeEcho(t, body).Path, "the current revision serves the traffic")

	rel := e.f.NewAPIReleaseClient()
	_, err := rel.CreateOrUpdate(t.Context(), apimRG, apimSvc, "echo", "r1", armapimanagement.APIReleaseContract{
		Properties: &armapimanagement.APIReleaseContractProperties{
			APIID: new("/apis/echo;rev=2"),
			Notes: new("go live"),
		},
	}, nil)
	require.NoError(t, err)

	list()
	assert.False(t, *revs[0].IsCurrent)
	assert.True(t, *revs[1].IsCurrent)

	status, body, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "hit=1", body, "the released revision serves the traffic")

	got, err := rel.Get(t.Context(), apimRG, apimSvc, "echo", "r1", nil)
	require.NoError(t, err)
	assert.Equal(t, "go live", *got.Properties.Notes)

	_, err = rel.CreateOrUpdate(t.Context(), apimRG, apimSvc, "echo", "r2", armapimanagement.APIReleaseContract{
		Properties: &armapimanagement.APIReleaseContractProperties{APIID: new("/apis/other;rev=2")},
	}, nil)
	require.Error(t, err)

	status, body, _ = e.raw(t, http.MethodPut, e.armPath("/apis/orphan;rev=2"),
		`{"properties":{"displayName":"x","path":"orphan","protocols":["https"]}}`, nil)
	assert.Equal(t, http.StatusBadRequest, status, body)

	status, body, _ = e.raw(t, http.MethodPut, e.armPath("/apis/echo;rev=zero"),
		`{"properties":{"displayName":"x","path":"echo","protocols":["https"]}}`, nil)
	assert.Equal(t, http.StatusBadRequest, status, body)
}

func TestAPIMGateway_IncludeFragment(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	frag := func(name, doc string) (int, string) {
		status, body, _ := e.raw(t, http.MethodPut, e.armPath("/policyFragments/"+name),
			`{"properties":{"format":"xml","value":`+jsonString(doc)+`}}`, nil)

		return status, body
	}

	status, body := frag(
		"hdr",
		`<fragment><set-header name="X-Frag" exists-action="override"><value>f1</value></set-header>
		<choose><when condition="true"><set-header name="X-Frag2" exists-action="override">
			<value>f2</value></set-header></when></choose></fragment>`,
	)
	require.Less(t, status, http.StatusBadRequest, body)

	e.policy(t, "/apis/echo", policyDoc(`<include-fragment fragment-id="hdr" />`, "", ""), http.StatusCreated)

	status, body, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status, body)

	h := decodeEcho(t, body).Header
	assert.Equal(t, []string{"f1"}, h["X-Frag"])
	assert.Equal(t, []string{"f2"}, h["X-Frag2"])

	body = e.policy(
		t,
		"/apis/echo",
		policyDoc(`<include-fragment fragment-id="ghost" />`, "", ""),
		http.StatusBadRequest,
	)
	assert.Contains(t, body, "'ghost'")

	for name, doc := range map[string]string{
		"wrong root": `<policies />`, "nested": `<fragment><include-fragment fragment-id="hdr" /></fragment>`,
		"unsupported": `<fragment><json-to-xml /></fragment>`, "base": `<fragment><base /></fragment>`,
	} {
		status, body = frag("bad", doc)
		assert.Equal(t, http.StatusBadRequest, status, name+": "+body)
	}
}

func jsonString(s string) string {
	var sb strings.Builder

	sb.WriteByte('"')

	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n', '\t':
			sb.WriteByte(' ')
		default:
			sb.WriteRune(r)
		}
	}

	sb.WriteByte('"')

	return sb.String()
}

func TestAPIMMgmtPersistence(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	der, _ := testCert(t, "persist.example", time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC))

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/certificates/pc"),
		`{"properties":{"data":"`+base64.StdEncoding.EncodeToString(der)+`"}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	for _, w := range []struct{ path, body string }{
		{"/loggers/ai", `{"properties":{"loggerType":"applicationInsights",` +
			`"credentials":{"instrumentationKey":"k"}}}`},
		{"/diagnostics/applicationinsights", `{"properties":{"loggerId":"/loggers/ai"}}`},
		{"/schemas/gs", `{"properties":{"schemaType":"json","value":"{}"}}`},
		{"/apis/echo/schemas/as", `{"properties":{"contentType":"application/json","document":{"value":"{}"}}}`},
		{"/products/starter/policies/policy", `{"properties":{"format":"xml",` +
			`"value":"<policies><inbound><base /></inbound></policies>"}}`},
	} {
		path, b := w.path, w.body
		status, body, _ = e.raw(t, http.MethodPut, e.armPath(path), b, nil)
		require.Less(t, status, http.StatusBadRequest, path+": "+body)
	}

	fresh := newAPIMStack(t)
	require.NoError(t, fresh.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))

	for _, path := range []string{
		"/certificates/pc", "/diagnostics/applicationinsights", "/schemas/gs", "/apis/echo/schemas/as",
		"/products/starter/policies/policy", "/groups/guests", "/subscriptions/master", "/products/unlimited",
	} {
		status, body, _ = fresh.raw(t, http.MethodGet, fresh.armPath(path), "", nil)
		assert.Equal(t, http.StatusOK, status, path+": "+body)
	}

	status, body, _ = fresh.raw(t, http.MethodGet, fresh.armPath("/certificates/pc"), "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "CN=persist.example")
}

func rsaCertPFX(t *testing.T, cn string) (*rsa.PrivateKey, []byte, []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(9), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, cert, nil, "pw")
	require.NoError(t, err)

	return key, der, pfx
}

func TestAPIMGateway_AuthenticationCertificate(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	_, der, pfx := rsaCertPFX(t, "client.example")
	sum := sha1.Sum(der)
	thumb := strings.ToUpper(hex.EncodeToString(sum[:]))

	cn := make(chan string, 4)
	be := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := "none"
		if len(r.TLS.PeerCertificates) > 0 {
			name = r.TLS.PeerCertificates[0].Subject.CommonName
		}

		cn <- name

		_, _ = io.WriteString(w, "secure")
	}))
	be.TLS = &tls.Config{ClientAuth: tls.RequestClientCert, MinVersion: tls.VersionTLS12}
	be.StartTLS()
	t.Cleanup(be.Close)

	pool := x509.NewCertPool()
	pool.AddCert(be.Certificate())
	e.prov.SetBackendRootCAs(pool)

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/certificates/cl"),
		`{"properties":{"data":"`+base64.StdEncoding.EncodeToString(pfx)+`","password":"pw"}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	e.putAPI(t, "sec", "sec", be.URL)
	e.putOp(t, "sec", "g", "get", "/x")

	tests := []struct {
		name, attrs, wantCN string
	}{
		{"thumbprint", `thumbprint="` + thumb + `"`, "client.example"},
		{"certificate-id", `certificate-id="cl"`, "client.example"},
		{"no policy", "", "none"},
	}

	for _, tc := range tests {
		stmt := ""
		if tc.attrs != "" {
			stmt = `<authentication-certificate ` + tc.attrs + ` />`
		}

		pb, err := json.Marshal(
			map[string]any{"properties": map[string]any{"format": "xml", "value": policyDoc(stmt, "", "")}},
			jsonx.V1(),
		)
		require.NoError(t, err)

		status, body, _ = e.raw(t, http.MethodPut, e.armPath("/apis/sec/policies/policy"), string(pb), nil)
		require.Less(t, status, http.StatusBadRequest, tc.name+body)

		status, body, _ = e.call(t, "GET", "/sec/x", "", nil)
		require.Equal(t, http.StatusOK, status, tc.name+body)
		assert.Equal(t, tc.wantCN, <-cn, tc.name)
	}

	bads := []string{
		`thumbprint="DEADBEEF"`,
		`certificate-id="ghost"`,
		``,
		`thumbprint="` + thumb + `" certificate-id="cl"`,
	}

	for _, bad := range bads {
		resp := e.policy(
			t,
			"/apis/sec",
			policyDoc(`<authentication-certificate `+bad+` />`, "", ""),
			http.StatusBadRequest,
		)
		assert.Contains(t, resp, "ValidationError")
	}
}

func TestAPIMGateway_ValidateJWTCertificate(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	key, der, _ := rsaCertPFX(t, "signer")

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/certificates/signer"),
		`{"properties":{"data":"`+base64.StdEncoding.EncodeToString(der)+`"}}`, nil)
	require.Less(t, status, http.StatusBadRequest, body)

	e.policy(
		t,
		"/apis/echo",
		jwtPolicy(
			`<validate-jwt header-name="X-T"><issuer-signing-keys><key certificate-id="signer" />`+
				`</issuer-signing-keys></validate-jwt>`,
		),
		http.StatusCreated,
	)

	tok, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": e.clock.Now().Add(time.Hour).Unix()}).
		SignedString(key)
	require.NoError(t, err)

	status, body, _ = e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": tok})
	assert.Equal(t, http.StatusOK, status, body)

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	bad, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": e.clock.Now().Add(time.Hour).Unix()}).
		SignedString(other)
	require.NoError(t, err)

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", map[string]string{"X-T": bad})
	assert.Equal(t, http.StatusUnauthorized, status)

	resp := e.policy(
		t,
		"/apis/echo",
		jwtPolicy(
			`<validate-jwt header-name="X-T"><issuer-signing-keys><key certificate-id="ghost" />`+
				`</issuer-signing-keys></validate-jwt>`,
		),
		http.StatusBadRequest,
	)
	assert.Contains(t, resp, "'ghost'")
}
