package opensearch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/labstack/echo/v5"
	osgo "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	awsv2 "github.com/opensearch-project/opensearch-go/v4/signer/awsv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

const (
	fgacMasterPass = "Sup3r-Secret!"
	fgacDomain     = "secured"
	fgacRegion     = "us-east-1"
)

var credentialAccessKey = regexp.MustCompile(`Credential=([A-Z0-9]+)/`)

type fgacEnv struct {
	handler    *opensearch.Handler
	sdk        *opensearchsdk.Client
	principals map[string]string
	url        string
}

// newFGACEnv serves the emulator behind a router that resolves SigV4 access keys to IAM principals.
func newFGACEnv(t *testing.T, principals map[string]string) *fgacEnv {
	t.Helper()

	reg, err := (&opensearch.Provider{}).Init(&service.AppContext{
		Config: strategyConfig{cfg: endpointstrategy.Config{OpenSearch: "path"}},
	})
	require.NoError(t, err)

	handler, ok := reg.(*opensearch.Handler)
	require.True(t, ok)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(reg))

	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			meta := awsmeta.FromRequest(req, fgacRegion)

			if m := credentialAccessKey.FindStringSubmatch(req.Header.Get("Authorization")); m != nil {
				if arn, found := principals[m[1]]; found {
					meta.Principal = &awsmeta.Principal{Arn: arn, AccountID: "000000000000"}
				}
			}

			c.SetRequest(req.WithContext(awsmeta.Set(req.Context(), meta)))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion(fgacRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &fgacEnv{
		url:        srv.URL,
		handler:    handler,
		principals: principals,
		sdk: opensearchsdk.NewFromConfig(awsCfg, func(o *opensearchsdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		}),
	}
}

func (e *fgacEnv) createFGACDomain(t *testing.T, name string, aso *types.AdvancedSecurityOptionsInput) {
	t.Helper()

	_, err := e.sdk.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName:                  aws.String(name),
		AdvancedSecurityOptions:     aso,
		EncryptionAtRestOptions:     &types.EncryptionAtRestOptions{Enabled: aws.Bool(true)},
		NodeToNodeEncryptionOptions: &types.NodeToNodeEncryptionOptions{Enabled: aws.Bool(true)},
		DomainEndpointOptions:       &types.DomainEndpointOptions{EnforceHTTPS: aws.Bool(true)},
	})
	require.NoError(t, err)
}

func masterOptions() *types.AdvancedSecurityOptionsInput {
	return &types.AdvancedSecurityOptionsInput{
		Enabled:                     aws.Bool(true),
		InternalUserDatabaseEnabled: aws.Bool(true),
		MasterUserOptions: &types.MasterUserOptions{
			MasterUserName:     aws.String("admin"),
			MasterUserPassword: aws.String(fgacMasterPass),
		},
	}
}

// client returns an opensearch-go client for the domain, authenticating with basic auth or SigV4.
func (e *fgacEnv) client(t *testing.T, domain, user, pass, signAs string) *opensearchapi.Client {
	t.Helper()

	cfg := osgo.Config{
		Addresses: []string{e.url + "/opensearch/" + fgacRegion + "/" + domain},
		Username:  user,
		Password:  pass,
	}

	if signAs != "" {
		awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion(fgacRegion),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(signAs, "secret", "")))
		require.NoError(t, err)

		signer, err := awsv2.NewSignerWithService(awsCfg, "es")
		require.NoError(t, err)

		cfg.Signer = signer
	}

	c, err := opensearchapi.NewClient(opensearchapi.Config{Client: cfg})
	require.NoError(t, err)

	if closer, ok := c.Client.Transport.(io.Closer); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}

	return c
}

// secAPI performs a security plugin REST call and returns the status and decoded JSON body.
func secAPI(t *testing.T, c *opensearchapi.Client, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var rdr io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		rdr = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, path, rdr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	_ = json.Unmarshal(raw, &out)

	return resp.StatusCode, out
}

func secStatus(t *testing.T, c *opensearchapi.Client, method, path string, body any, want int) map[string]any {
	t.Helper()

	code, out := secAPI(t, c, method, path, body)
	require.Equal(t, want, code, "%s %s: %v", method, path, out)

	return out
}

func structErr(t *testing.T, err error) *osgo.StructError {
	t.Helper()

	var se *osgo.StructError

	require.ErrorAs(t, err, &se)

	return se
}

func jsonBody(t *testing.T, v any) io.Reader {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return bytes.NewReader(b)
}

func indexDoc(t *testing.T, c *opensearchapi.Client, index, id string, doc any) {
	t.Helper()

	_, err := c.Index(t.Context(), opensearchapi.IndexReq{Index: index, DocumentID: id, Body: jsonBody(t, doc)})
	require.NoError(t, err)
}

func searchAll(ctx context.Context, c *opensearchapi.Client, index string) (*opensearchapi.SearchResp, error) {
	return c.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{index}, Body: strings.NewReader(`{"query":{"match_all":{}}}`),
	})
}

func hitSource(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()

	var src map[string]any
	require.NoError(t, json.Unmarshal(raw, &src))

	return src
}

// seedFGACDomain creates the domain, two indices and the documents the access tests read.
func seedFGACDomain(t *testing.T, env *fgacEnv) *opensearchapi.Client {
	t.Helper()

	env.createFGACDomain(t, fgacDomain, masterOptions())
	admin := env.client(t, fgacDomain, "admin", fgacMasterPass, "")

	for _, idx := range []string{"logs-1", "secret-1"} {
		_, err := admin.Indices.Create(t.Context(), opensearchapi.IndicesCreateReq{Index: idx})
		require.NoError(t, err)
	}

	indexDoc(t, admin, "logs-1", "1", map[string]any{
		"owner": "alice", "level": "info", "message": "hello", "ssn": "123-45-6789",
	})
	indexDoc(t, admin, "logs-1", "2", map[string]any{
		"owner": "bob", "level": "warn", "message": "careful", "ssn": "987-65-4321",
	})
	indexDoc(t, admin, "secret-1", "1", map[string]any{"owner": "alice", "secret": "s3cr3t"})

	return admin
}

func TestFGAC_ReadOnlyUserAndFieldLevelSecurity(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	admin := seedFGACDomain(t, env)

	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/roles/logs_reader", map[string]any{
		"cluster_permissions": []string{"cluster_composite_ops_ro"},
		"index_permissions": []map[string]any{{
			"index_patterns":  []string{"logs-*"},
			"allowed_actions": []string{"read"},
			"fls":             []string{"owner", "level", "message"},
		}},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/internalusers/bob", map[string]any{
		"password": "Bob-Passw0rd!", "backend_roles": []string{},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/logs_reader", map[string]any{
		"users": []string{"bob"},
	}, http.StatusCreated)

	bob := env.client(t, fgacDomain, "bob", "Bob-Passw0rd!", "")

	t.Run("search allowed index hides fls field", func(t *testing.T) {
		t.Parallel()

		resp, err := searchAll(t.Context(), bob, "logs-1")
		require.NoError(t, err)
		require.Len(t, resp.Hits.Hits, 2)

		for _, h := range resp.Hits.Hits {
			src := hitSource(t, h.Source)
			assert.NotContains(t, src, "ssn")
			assert.Contains(t, src, "message")
		}
	})

	t.Run("search other index is forbidden", func(t *testing.T) {
		t.Parallel()

		_, err := searchAll(t.Context(), bob, "secret-1")
		se := structErr(t, err)
		assert.Equal(t, http.StatusForbidden, se.Status)
		assert.Equal(t, "security_exception", se.Err.Type)
		assert.Equal(t,
			"no permissions for [indices:data/read/search] and User [name=bob, backend_roles=[], requestedTenant=null]",
			se.Err.Reason)
	})

	t.Run("write is forbidden", func(t *testing.T) {
		t.Parallel()

		_, err := bob.Index(t.Context(), opensearchapi.IndexReq{
			Index: "logs-1", DocumentID: "9", Body: strings.NewReader(`{"owner":"bob"}`),
		})
		se := structErr(t, err)
		assert.Equal(t, http.StatusForbidden, se.Status)
		assert.Contains(t, se.Err.Reason, "indices:data/write/index")
	})

	t.Run("get applies fls", func(t *testing.T) {
		t.Parallel()

		doc, err := bob.Document.Get(t.Context(), opensearchapi.DocumentGetReq{Index: "logs-1", DocumentID: "1"})
		require.NoError(t, err)
		assert.True(t, doc.Found)
		assert.NotContains(t, hitSource(t, doc.Source), "ssn")
	})

	t.Run("wrong password is unauthorized", func(t *testing.T) {
		t.Parallel()

		_, err := searchAll(t.Context(), env.client(t, fgacDomain, "bob", "nope", ""), "logs-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("no credentials is unauthorized", func(t *testing.T) {
		t.Parallel()

		_, err := searchAll(t.Context(), env.client(t, fgacDomain, "", "", ""), "logs-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("non-admin cannot use the security api", func(t *testing.T) {
		t.Parallel()

		code, body := secAPI(t, bob, http.MethodGet, "/_plugins/_security/api/internalusers", nil)
		assert.Equal(t, http.StatusForbidden, code)
		assert.Equal(t, "FORBIDDEN", body["status"])
	})

	t.Run("master sees everything", func(t *testing.T) {
		t.Parallel()

		resp, err := searchAll(t.Context(), admin, "secret-1")
		require.NoError(t, err)
		assert.Len(t, resp.Hits.Hits, 1)
	})
}

func TestFGAC_DocumentLevelSecurityAndMasking(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	admin := seedFGACDomain(t, env)

	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/roles/alice_only", map[string]any{
		"index_permissions": []map[string]any{{
			"index_patterns":  []string{"logs-*"},
			"allowed_actions": []string{"read"},
			"dls":             `{"term":{"owner":"alice"}}`,
			"masked_fields":   []string{"ssn"},
		}},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/internalusers/carol", map[string]any{
		"password": "Carol-Passw0rd!",
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/alice_only", map[string]any{
		"users": []string{"carol"},
	}, http.StatusCreated)

	carol := env.client(t, fgacDomain, "carol", "Carol-Passw0rd!", "")

	t.Run("search only returns permitted documents with masked field", func(t *testing.T) {
		t.Parallel()

		resp, err := searchAll(t.Context(), carol, "logs-1")
		require.NoError(t, err)
		require.Len(t, resp.Hits.Hits, 1)
		assert.Equal(t, 1, resp.Hits.Total.Value)

		src := hitSource(t, resp.Hits.Hits[0].Source)
		assert.Equal(t, "alice", src["owner"])
		assert.NotEqual(t, "123-45-6789", src["ssn"])
		assert.Len(t, src["ssn"], 128)
	})

	t.Run("get of a hidden document is not found", func(t *testing.T) {
		t.Parallel()

		doc, err := carol.Document.Get(t.Context(), opensearchapi.DocumentGetReq{Index: "logs-1", DocumentID: "2"})
		if err == nil {
			assert.False(t, doc.Found)

			return
		}

		assert.Equal(t, http.StatusNotFound, structErr(t, err).Status)
	})

	t.Run("count honours dls", func(t *testing.T) {
		t.Parallel()

		resp, err := carol.Indices.Count(t.Context(), &opensearchapi.IndicesCountReq{Indices: []string{"logs-1"}})
		require.NoError(t, err)
		assert.Equal(t, 1, resp.Count)
	})
}

func TestFGAC_SigV4PrincipalMappedToBackendRole(t *testing.T) {
	t.Parallel()

	const (
		readerARN = "arn:aws:iam::000000000000:role/Reader"
		otherARN  = "arn:aws:iam::000000000000:role/Other"
	)

	env := newFGACEnv(t, map[string]string{
		"AKIAREADER": "arn:aws:sts::000000000000:assumed-role/Reader/session-1",
		"AKIAOTHER":  otherARN,
	})
	admin := seedFGACDomain(t, env)

	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/readall", map[string]any{
		"backend_roles": []string{readerARN},
	}, http.StatusCreated)

	reader := env.client(t, fgacDomain, "", "", "AKIAREADER")
	resp, err := searchAll(t.Context(), reader, "secret-1")
	require.NoError(t, err)
	assert.Len(t, resp.Hits.Hits, 1)

	other := env.client(t, fgacDomain, "", "", "AKIAOTHER")
	_, err = searchAll(t.Context(), other, "secret-1")
	se := structErr(t, err)
	assert.Equal(t, http.StatusForbidden, se.Status)
	assert.Contains(t, se.Err.Reason, "User [name="+otherARN)
}

func TestFGAC_IAMMasterUser(t *testing.T) {
	t.Parallel()

	const masterARN = "arn:aws:iam::000000000000:user/boss"

	env := newFGACEnv(t, map[string]string{"AKIABOSS": masterARN, "AKIAOTHER": "arn:aws:iam::000000000000:user/x"})
	env.createFGACDomain(t, "iamdom", &types.AdvancedSecurityOptionsInput{
		Enabled:                     aws.Bool(true),
		InternalUserDatabaseEnabled: aws.Bool(false),
		MasterUserOptions:           &types.MasterUserOptions{MasterUserARN: aws.String(masterARN)},
	})

	boss := env.client(t, "iamdom", "", "", "AKIABOSS")
	out := secStatus(t, boss, http.MethodGet, "/_plugins/_security/api/rolesmapping/all_access", nil, http.StatusOK)

	mapping, ok := out["all_access"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, mapping["backend_roles"], masterARN)

	info := secStatus(t, boss, http.MethodGet, "/_plugins/_security/authinfo", nil, http.StatusOK)
	assert.Equal(t, masterARN, info["user_name"])
	assert.Contains(t, info["roles"], "all_access")
	assert.Contains(t, info["roles"], "security_manager")

	nobody := env.client(t, "iamdom", "", "", "AKIAOTHER")
	code, _ := secAPI(t, nobody, http.MethodGet, "/_plugins/_security/api/roles", nil)
	assert.Equal(t, http.StatusForbidden, code)
}

func TestFGAC_BulkItemAuthorization(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	admin := seedFGACDomain(t, env)

	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/roles/logs_writer", map[string]any{
		"cluster_permissions": []string{"cluster_composite_ops"},
		"index_permissions": []map[string]any{{
			"index_patterns": []string{"logs-*"}, "allowed_actions": []string{"crud"},
		}},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/internalusers/writer",
		map[string]any{"password": "Writer-Passw0rd!"}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/logs_writer",
		map[string]any{"users": []string{"writer"}}, http.StatusCreated)

	writer := env.client(t, fgacDomain, "writer", "Writer-Passw0rd!", "")
	body := `{"index":{"_index":"logs-1","_id":"10"}}` + "\n" + `{"owner":"w"}` + "\n" +
		`{"index":{"_index":"secret-1","_id":"11"}}` + "\n" + `{"owner":"w"}` + "\n"

	resp, err := writer.Bulk(t.Context(), opensearchapi.BulkReq{Body: strings.NewReader(body)})
	require.NoError(t, err)
	require.True(t, resp.Errors)
	require.Len(t, resp.Items, 2)

	assert.Equal(t, http.StatusCreated, resp.Items[0]["index"].Status)
	assert.Equal(t, http.StatusForbidden, resp.Items[1]["index"].Status)
	assert.Equal(t, "security_exception", resp.Items[1]["index"].Error.Type)
}

func TestFGAC_SecurityAPICrud(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	env.createFGACDomain(t, fgacDomain, masterOptions())
	admin := env.client(t, fgacDomain, "admin", fgacMasterPass, "")

	const api = "/_plugins/_security/api/"

	t.Run("internal users", func(t *testing.T) {
		t.Parallel()

		out := secStatus(t, admin, http.MethodPut, api+"internalusers/u1", map[string]any{
			"password": "Passw0rd!x", "backend_roles": []string{"br1"}, "attributes": map[string]string{"a": "b"},
			"description": "first",
		}, http.StatusCreated)
		assert.Equal(t, map[string]any{"status": "CREATED", "message": "'u1' created."}, out)

		out = secStatus(t, admin, http.MethodPut, api+"internalusers/u1", map[string]any{
			"password": "Passw0rd!y", "backend_roles": []string{"br2"},
		}, http.StatusOK)
		assert.Equal(t, "'u1' updated.", out["message"])

		got := secStatus(t, admin, http.MethodGet, api+"internalusers/u1", nil, http.StatusOK)
		user, ok := got["u1"].(map[string]any)
		require.True(t, ok)
		assert.Empty(t, user["hash"], "password hashes are never returned")
		assert.Equal(t, []any{"br2"}, user["backend_roles"])

		secStatus(t, admin, http.MethodPatch, api+"internalusers/u1", []map[string]any{
			{"op": "replace", "path": "/description", "value": "patched"},
		}, http.StatusOK)

		got = secStatus(t, admin, http.MethodGet, api+"internalusers/u1", nil, http.StatusOK)
		assert.Equal(t, "patched", got["u1"].(map[string]any)["description"])

		list := secStatus(t, admin, http.MethodGet, api+"internalusers", nil, http.StatusOK)
		assert.Contains(t, list, "admin")
		assert.Contains(t, list, "u1")

		secStatus(t, admin, http.MethodDelete, api+"internalusers/u1", nil, http.StatusOK)

		missing := secStatus(t, admin, http.MethodGet, api+"internalusers/u1", nil, http.StatusNotFound)
		assert.Equal(t, "NOT_FOUND", missing["status"])
		secStatus(t, admin, http.MethodDelete, api+"internalusers/u1", nil, http.StatusNotFound)
	})

	t.Run("user needs password or hash", func(t *testing.T) {
		t.Parallel()

		secStatus(t, admin, http.MethodPut, api+"internalusers/nopw", map[string]any{}, http.StatusBadRequest)
	})

	t.Run("unknown keys are rejected", func(t *testing.T) {
		t.Parallel()

		out := secStatus(
			t,
			admin,
			http.MethodPut,
			api+"roles/badkeys",
			map[string]any{"nope": 1},
			http.StatusBadRequest,
		)
		assert.Equal(t, "Invalid configuration", out["reason"])
		assert.Equal(t, map[string]any{"keys": "nope"}, out["invalid_keys"])
	})

	t.Run("roles", func(t *testing.T) {
		t.Parallel()

		secStatus(t, admin, http.MethodPut, api+"roles/r1", map[string]any{
			"cluster_permissions": []string{"cluster_monitor"},
			"index_permissions": []map[string]any{{
				"index_patterns": []string{"a-*"}, "allowed_actions": []string{"crud"},
				"dls": `{"term":{"x":"y"}}`, "fls": []string{"~secret"}, "masked_fields": []string{"pii"},
			}},
			"tenant_permissions": []map[string]any{{
				"tenant_patterns": []string{"t1"}, "allowed_actions": []string{"kibana_all_read"},
			}},
		}, http.StatusCreated)

		got := secStatus(t, admin, http.MethodGet, api+"roles/r1", nil, http.StatusOK)
		role, ok := got["r1"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, false, role["reserved"])
		assert.Equal(t, false, role["static"])

		ip, ok := role["index_permissions"].([]any)
		require.True(t, ok)
		require.Len(t, ip, 1)
		assert.JSONEq(t, `{"term":{"x":"y"}}`, ip[0].(map[string]any)["dls"].(string))

		secStatus(t, admin, http.MethodPut, api+"roles/r_bad", map[string]any{
			"index_permissions": []map[string]any{{"allowed_actions": []string{"read"}}},
		}, http.StatusBadRequest)
		secStatus(t, admin, http.MethodPut, api+"roles/r_mixed", map[string]any{
			"index_permissions": []map[string]any{{
				"index_patterns": []string{"*"}, "allowed_actions": []string{"read"}, "fls": []string{"a", "~b"},
			}},
		}, http.StatusBadRequest)
		secStatus(t, admin, http.MethodPut, api+"roles/r_dls", map[string]any{
			"index_permissions": []map[string]any{{
				"index_patterns": []string{"*"}, "allowed_actions": []string{"read"}, "dls": "nope",
			}},
		}, http.StatusBadRequest)

		secStatus(t, admin, http.MethodDelete, api+"roles/r1", nil, http.StatusOK)
	})

	t.Run("built in roles are reserved", func(t *testing.T) {
		t.Parallel()

		list := secStatus(t, admin, http.MethodGet, api+"roles", nil, http.StatusOK)
		for _, name := range []string{"all_access", "readall", "kibana_user", "security_manager", "own_index"} {
			assert.Contains(t, list, name)
		}

		got := secStatus(t, admin, http.MethodGet, api+"roles/readall", nil, http.StatusOK)
		assert.Equal(t, true, got["readall"].(map[string]any)["reserved"])

		out := secStatus(t, admin, http.MethodPut, api+"roles/readall", map[string]any{}, http.StatusForbidden)
		assert.Equal(t, "FORBIDDEN", out["status"])
		secStatus(t, admin, http.MethodDelete, api+"roles/all_access", nil, http.StatusForbidden)
	})

	t.Run("roles mapping", func(t *testing.T) {
		t.Parallel()

		secStatus(
			t,
			admin,
			http.MethodPut,
			api+"roles/mapped",
			map[string]any{"cluster_permissions": []string{"cluster_monitor"}},
			http.StatusCreated,
		)
		secStatus(t, admin, http.MethodPut, api+"rolesmapping/mapped", map[string]any{
			"backend_roles": []string{"b1"}, "users": []string{"u"}, "hosts": []string{"h"}, "description": "d",
		}, http.StatusCreated)
		secStatus(t, admin, http.MethodPut, api+"rolesmapping/nonexistent_role", map[string]any{
			"users": []string{"u"},
		}, http.StatusNotFound)

		secStatus(t, admin, http.MethodPatch, api+"rolesmapping/mapped", []map[string]any{
			{"op": "add", "path": "/users/-", "value": "v"},
		}, http.StatusOK)

		got := secStatus(t, admin, http.MethodGet, api+"rolesmapping/mapped", nil, http.StatusOK)
		assert.Equal(t, []any{"u", "v"}, got["mapped"].(map[string]any)["users"])

		defaults := secStatus(t, admin, http.MethodGet, api+"rolesmapping/all_access", nil, http.StatusOK)
		assert.Equal(t, []any{"admin"}, defaults["all_access"].(map[string]any)["users"])
	})

	t.Run("action groups", func(t *testing.T) {
		t.Parallel()

		secStatus(t, admin, http.MethodPut, api+"actiongroups/my_group", map[string]any{
			"allowed_actions": []string{"read", "indices:admin/refresh*"}, "type": "index",
		}, http.StatusCreated)
		got := secStatus(t, admin, http.MethodGet, api+"actiongroups/my_group", nil, http.StatusOK)
		assert.Equal(t, "index", got["my_group"].(map[string]any)["type"])

		secStatus(t, admin, http.MethodPut, api+"actiongroups/no_actions", map[string]any{}, http.StatusBadRequest)
		secStatus(t, admin, http.MethodPut, api+"actiongroups/bad_type", map[string]any{
			"allowed_actions": []string{"read"}, "type": "weird",
		}, http.StatusBadRequest)
		secStatus(t, admin, http.MethodPut, api+"actiongroups/crud", map[string]any{
			"allowed_actions": []string{"read"},
		}, http.StatusForbidden)

		list := secStatus(t, admin, http.MethodGet, api+"actiongroups", nil, http.StatusOK)
		assert.Contains(t, list, "crud")
		assert.Contains(t, list, "my_group")
	})

	t.Run("tenants", func(t *testing.T) {
		t.Parallel()

		secStatus(t, admin, http.MethodPut, api+"tenants/t1", map[string]any{"description": "team"}, http.StatusCreated)
		got := secStatus(t, admin, http.MethodGet, api+"tenants/t1", nil, http.StatusOK)
		assert.Equal(t, "team", got["t1"].(map[string]any)["description"])
		secStatus(t, admin, http.MethodDelete, api+"tenants/t1", nil, http.StatusOK)
		secStatus(t, admin, http.MethodGet, api+"tenants/t1", nil, http.StatusNotFound)
	})

	t.Run("collection patch", func(t *testing.T) {
		t.Parallel()

		secStatus(t, admin, http.MethodPatch, api+"tenants", []map[string]any{
			{"op": "add", "path": "/bulk1", "value": map[string]any{"description": "x"}},
		}, http.StatusOK)
		secStatus(t, admin, http.MethodGet, api+"tenants/bulk1", nil, http.StatusOK)
		secStatus(t, admin, http.MethodPatch, api+"tenants", []map[string]any{
			{"op": "remove", "path": "/bulk1"},
		}, http.StatusOK)
		secStatus(t, admin, http.MethodGet, api+"tenants/bulk1", nil, http.StatusNotFound)
	})

	t.Run("authinfo and account", func(t *testing.T) {
		t.Parallel()

		info := secStatus(t, admin, http.MethodGet, "/_plugins/_security/authinfo", nil, http.StatusOK)
		assert.Equal(t, "admin", info["user_name"])
		assert.Contains(t, info["user"], "User [name=admin")
		assert.Contains(t, info["roles"], "all_access")

		acct := secStatus(t, admin, http.MethodGet, api+"account", nil, http.StatusOK)
		assert.Equal(t, "admin", acct["user_name"])
		assert.Equal(t, true, acct["is_internal_user"])
	})
}

func TestFGAC_AccountPasswordChange(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	env.createFGACDomain(t, fgacDomain, masterOptions())
	admin := env.client(t, fgacDomain, "admin", fgacMasterPass, "")

	const account = "/_plugins/_security/api/account"

	secStatus(t, admin, http.MethodPut, account, map[string]any{
		"current_password": "wrong", "password": "N3w-Passw0rd!",
	}, http.StatusBadRequest)
	secStatus(t, admin, http.MethodPut, account, map[string]any{
		"current_password": fgacMasterPass, "password": "N3w-Passw0rd!",
	}, http.StatusOK)

	oldPw := env.client(t, fgacDomain, "admin", fgacMasterPass, "")
	_, err := searchAll(t.Context(), oldPw, "any")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")

	newPw := env.client(t, fgacDomain, "admin", "N3w-Passw0rd!", "")
	secStatus(t, newPw, http.MethodGet, "/_plugins/_security/authinfo", nil, http.StatusOK)

	// An unrelated control-plane update must not resurrect the creation-time master password.
	_, err = env.sdk.UpdateDomainConfig(t.Context(), &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String(fgacDomain),
		AdvancedSecurityOptions: &types.AdvancedSecurityOptionsInput{
			JWTOptions: &types.JWTOptionsInput{Enabled: aws.Bool(true), JwksUrl: aws.String("https://example.com/jwks")},
		},
	})
	require.NoError(t, err)

	secStatus(t, env.client(t, fgacDomain, "admin", "N3w-Passw0rd!", ""),
		http.MethodGet, "/_plugins/_security/authinfo", nil, http.StatusOK)

	_, err = searchAll(t.Context(), env.client(t, fgacDomain, "admin", fgacMasterPass, ""), "any")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestFGAC_StatePersistsAcrossSnapshotRestore(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	admin := seedFGACDomain(t, env)

	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/roles/persisted", map[string]any{
		"index_permissions": []map[string]any{{
			"index_patterns": []string{"logs-*"}, "allowed_actions": []string{"read"},
		}},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/internalusers/dora",
		map[string]any{"password": "Dora-Passw0rd!"}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/persisted",
		map[string]any{"users": []string{"dora"}}, http.StatusCreated)

	snap := env.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	assert.NotContains(t, string(snap), "Dora-Passw0rd!", "plaintext passwords are never persisted")
	assert.NotContains(t, string(snap), fgacMasterPass)

	restored := newFGACEnv(t, nil)
	require.NoError(t, restored.handler.Restore(t.Context(), snap))

	// The restored domain keeps its users, roles and mappings, and the credentials still verify.
	dora := restored.client(t, fgacDomain, "dora", "Dora-Passw0rd!", "")
	resp, err := searchAll(t.Context(), dora, "logs-1")
	require.NoError(t, err)
	assert.Len(t, resp.Hits.Hits, 2)

	_, err = searchAll(t.Context(), dora, "secret-1")
	assert.Equal(t, http.StatusForbidden, structErr(t, err).Status)

	again := restored.client(t, fgacDomain, "admin", fgacMasterPass, "")
	secStatus(t, again, http.MethodGet, "/_plugins/_security/api/roles/persisted", nil, http.StatusOK)
}

func TestFGAC_AnonymousAuth(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	aso := masterOptions()
	aso.AnonymousAuthEnabled = aws.Bool(true)
	env.createFGACDomain(t, fgacDomain, aso)

	out, err := env.sdk.DescribeDomain(
		t.Context(),
		&opensearchsdk.DescribeDomainInput{DomainName: aws.String(fgacDomain)},
	)
	require.NoError(t, err)

	got := out.DomainStatus.AdvancedSecurityOptions
	require.NotNil(t, got)
	assert.True(t, aws.ToBool(got.AnonymousAuthEnabled))
	require.NotNil(t, got.AnonymousAuthDisableDate, "the 30 day migration window is reported")

	admin := env.client(t, fgacDomain, "admin", fgacMasterPass, "")
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/roles/anon_read", map[string]any{
		"index_permissions": []map[string]any{{"index_patterns": []string{"*"}, "allowed_actions": []string{"read"}}},
	}, http.StatusCreated)
	secStatus(t, admin, http.MethodPut, "/_plugins/_security/api/rolesmapping/anon_read", map[string]any{
		"backend_roles": []string{"opendistro_security_anonymous_backendrole"},
	}, http.StatusCreated)

	_, err = admin.Indices.Create(t.Context(), opensearchapi.IndicesCreateReq{Index: "pub"})
	require.NoError(t, err)

	anon := env.client(t, fgacDomain, "", "", "")
	_, err = searchAll(t.Context(), anon, "pub")
	require.NoError(t, err)

	_, err = anon.Indices.Create(t.Context(), opensearchapi.IndicesCreateReq{Index: "nope"})
	assert.Equal(t, http.StatusForbidden, structErr(t, err).Status)
}

func TestFGAC_DomainOptionValidation(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)

	full := func(aso *types.AdvancedSecurityOptionsInput) *opensearchsdk.CreateDomainInput {
		return &opensearchsdk.CreateDomainInput{
			DomainName:                  aws.String("valid-dom"),
			AdvancedSecurityOptions:     aso,
			EncryptionAtRestOptions:     &types.EncryptionAtRestOptions{Enabled: aws.Bool(true)},
			NodeToNodeEncryptionOptions: &types.NodeToNodeEncryptionOptions{Enabled: aws.Bool(true)},
			DomainEndpointOptions:       &types.DomainEndpointOptions{EnforceHTTPS: aws.Bool(true)},
		}
	}

	tests := []struct {
		build   func() *opensearchsdk.CreateDomainInput
		name    string
		wantErr string
	}{
		{
			name: "missing encryption at rest",
			build: func() *opensearchsdk.CreateDomainInput {
				in := full(masterOptions())
				in.EncryptionAtRestOptions = nil

				return in
			},
			wantErr: "encryption at rest",
		},
		{
			name: "missing node to node",
			build: func() *opensearchsdk.CreateDomainInput {
				in := full(masterOptions())
				in.NodeToNodeEncryptionOptions = nil

				return in
			},
			wantErr: "node-to-node",
		},
		{
			name: "missing https",
			build: func() *opensearchsdk.CreateDomainInput {
				in := full(masterOptions())
				in.DomainEndpointOptions = nil

				return in
			},
			wantErr: "HTTPS",
		},
		{
			name: "weak master password",
			build: func() *opensearchsdk.CreateDomainInput {
				aso := masterOptions()
				aso.MasterUserOptions.MasterUserPassword = aws.String("password")

				return full(aso)
			},
			wantErr: "MasterUserPassword",
		},
		{
			name: "internal db without master options",
			build: func() *opensearchsdk.CreateDomainInput {
				aso := masterOptions()
				aso.MasterUserOptions = nil

				return full(aso)
			},
			wantErr: "MasterUserName",
		},
		{
			name: "bad master arn",
			build: func() *opensearchsdk.CreateDomainInput {
				return full(&types.AdvancedSecurityOptionsInput{
					Enabled:                     aws.Bool(true),
					InternalUserDatabaseEnabled: aws.Bool(false),
					MasterUserOptions:           &types.MasterUserOptions{MasterUserARN: aws.String("not-an-arn")},
				})
			},
			wantErr: "MasterUserARN",
		},
		{
			name: "jwt requires a key",
			build: func() *opensearchsdk.CreateDomainInput {
				aso := masterOptions()
				aso.JWTOptions = &types.JWTOptionsInput{Enabled: aws.Bool(true)}

				return full(aso)
			},
			wantErr: "JWTOptions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.sdk.CreateDomain(t.Context(), tt.build())

			var ve *types.ValidationException

			require.ErrorAs(t, err, &ve)
			assert.Contains(t, ve.ErrorMessage(), tt.wantErr)
		})
	}

	t.Run("jwt and saml options round trip", func(t *testing.T) {
		t.Parallel()

		aso := masterOptions()
		aso.JWTOptions = &types.JWTOptionsInput{
			Enabled: aws.Bool(
				true,
			), PublicKey: aws.String("pk"), RolesKey: aws.String("roles"), SubjectKey: aws.String("sub"),
		}
		aso.SAMLOptions = &types.SAMLOptionsInput{
			Enabled: aws.Bool(true), SubjectKey: aws.String("NameID"),
			Idp: &types.SAMLIdp{EntityId: aws.String("https://idp"), MetadataContent: aws.String("<xml/>")},
		}

		in := full(aso)
		in.DomainName = aws.String("opts")
		_, err := env.sdk.CreateDomain(t.Context(), in)
		require.NoError(t, err)

		out, err := env.sdk.DescribeDomain(
			t.Context(),
			&opensearchsdk.DescribeDomainInput{DomainName: aws.String("opts")},
		)
		require.NoError(t, err)

		got := out.DomainStatus.AdvancedSecurityOptions
		require.NotNil(t, got.JWTOptions)
		assert.Equal(t, "pk", aws.ToString(got.JWTOptions.PublicKey))
		assert.Equal(t, "roles", aws.ToString(got.JWTOptions.RolesKey))
		require.NotNil(t, got.SAMLOptions)
		assert.Equal(t, "https://idp", aws.ToString(got.SAMLOptions.Idp.EntityId))
	})

	t.Run("cannot disable once enabled", func(t *testing.T) {
		t.Parallel()

		in := full(masterOptions())
		in.DomainName = aws.String("nodisable")
		_, err := env.sdk.CreateDomain(t.Context(), in)
		require.NoError(t, err)

		_, err = env.sdk.UpdateDomainConfig(t.Context(), &opensearchsdk.UpdateDomainConfigInput{
			DomainName:              aws.String("nodisable"),
			AdvancedSecurityOptions: &types.AdvancedSecurityOptionsInput{Enabled: aws.Bool(false)},
		})

		var ve *types.ValidationException

		require.ErrorAs(t, err, &ve)
		assert.Contains(t, ve.ErrorMessage(), "cannot be disabled")
	})

	t.Run("partial update keeps fgac enabled", func(t *testing.T) {
		t.Parallel()

		in := full(masterOptions())
		in.DomainName = aws.String("partial")
		_, err := env.sdk.CreateDomain(t.Context(), in)
		require.NoError(t, err)

		_, err = env.sdk.UpdateDomainConfig(t.Context(), &opensearchsdk.UpdateDomainConfigInput{
			DomainName: aws.String("partial"),
			AdvancedSecurityOptions: &types.AdvancedSecurityOptionsInput{
				JWTOptions: &types.JWTOptionsInput{
					Enabled: aws.Bool(true),
					JwksUrl: aws.String("https://example.com/jwks"),
				},
			},
		})
		require.NoError(t, err)

		out, err := env.sdk.DescribeDomain(
			t.Context(),
			&opensearchsdk.DescribeDomainInput{DomainName: aws.String("partial")},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(out.DomainStatus.AdvancedSecurityOptions.Enabled))
		assert.True(t, aws.ToBool(out.DomainStatus.AdvancedSecurityOptions.InternalUserDatabaseEnabled))
	})
}

func TestFGAC_PasswordNeverInResponses(t *testing.T) {
	t.Parallel()

	env := newFGACEnv(t, nil)
	env.createFGACDomain(t, fgacDomain, masterOptions())
	admin := env.client(t, fgacDomain, "admin", fgacMasterPass, "")

	for _, p := range []string{"internalusers", "internalusers/admin", "rolesmapping", "roles"} {
		code, body := secAPI(t, admin, http.MethodGet, "/_plugins/_security/api/"+p, nil)
		require.Equal(t, http.StatusOK, code)

		raw, err := json.Marshal(body)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), fgacMasterPass)
		assert.NotContains(t, string(raw), "$2a$")
	}

	out, err := env.sdk.DescribeDomain(
		t.Context(),
		&opensearchsdk.DescribeDomainInput{DomainName: aws.String(fgacDomain)},
	)
	require.NoError(t, err)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), fgacMasterPass)
	assert.NotContains(t, string(raw), "$2a$")
}
