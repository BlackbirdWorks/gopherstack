package ociobjectstorage_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestSignatureVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		signWith func(e *env, other keyPair) keyPair
		name     string
		wantCode int
		opts     envOpts
	}{
		{
			name:     "permissive accepts unregistered key",
			opts:     envOpts{},
			signWith: func(e *env, _ keyPair) keyPair { return e.keys },
			wantCode: 200,
		},
		{
			name:     "registered key valid signature",
			opts:     envOpts{register: true},
			signWith: func(e *env, _ keyPair) keyPair { return e.keys },
			wantCode: 200,
		},
		{
			name: "registered key forged signature", opts: envOpts{register: true}, wantCode: 401,
			signWith: func(e *env, other keyPair) keyPair {
				other.fprint = e.keys.fprint

				return other
			},
		},
		{
			name:     "enforce rejects unknown key",
			opts:     envOpts{enforce: true},
			signWith: func(e *env, _ keyPair) keyPair { return e.keys },
			wantCode: 401,
		},
		{
			name:     "enforce accepts registered key",
			opts:     envOpts{enforce: true, register: true},
			signWith: func(e *env, _ keyPair) keyPair { return e.keys },
			wantCode: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)
			c := e.clientFor(t, tt.signWith(e, newKeyPair(t)))

			_, err := c.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
			if tt.wantCode == 200 {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetHTTPStatusCode())
			assert.Equal(t, "NotAuthenticated", se.GetCode())
		})
	}
}

func TestBodyHashIsVerified(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	e.mkBucket(t, "signed", nil)

	body := `{"object":"x"}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		e.baseURL+"/n/"+e.ns+"/b/signed/u", strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Date", e.clock.Now().Format(http.TimeFormat))
	req.Header.Set("X-Content-Sha256", "AAAA")
	req.Header.Set(
		"Authorization",
		`Signature version="1",headers="(request-target) date host x-content-sha256",keyId="`+
			testTenancy+"/"+testUser+"/"+e.keys.fprint+`",algorithm="rsa-sha256",signature="AAAA"`,
	)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAdminKeyRegistration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	_, err := e.client.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
	assert.Equal(t, 401, svcErr(t, err).GetHTTPStatusCode())

	payload, _ := json.Marshal(map[string]string{"tenancy": testTenancy, "user": testUser, "publicKey": e.keys.pubPEM})
	code, body := rawDo(t, http.MethodPost, e.baseURL+"/_gopherstack/oci/keys", string(payload))
	require.Equal(t, http.StatusCreated, code, body)

	var out struct {
		KeyID string `json:"keyId"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.Equal(t, testTenancy+"/"+testUser+"/"+e.keys.fprint, out.KeyID)

	resp, err := e.client.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
	require.NoError(t, err)
	assert.Equal(t, e.ns, *resp.Value)

	code, _ = rawDo(t, http.MethodDelete, e.baseURL+"/_gopherstack/oci/keys?keyId="+out.KeyID, "")
	assert.Equal(t, http.StatusNoContent, code)

	_, err = e.client.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
	assert.Equal(t, 401, svcErr(t, err).GetHTTPStatusCode())

	code, _ = rawDo(
		t,
		http.MethodPost,
		e.baseURL+"/_gopherstack/oci/keys",
		`{"tenancy":"t","user":"u","publicKey":"junk"}`,
	)
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestPublicBucketAnonymousRead(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	e.mkBucket(t, "pub", func(d *objectstorage.CreateBucketDetails) {
		d.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeObjectreadwithoutlist
	})
	e.mkBucket(t, "priv", nil)
	e.put(t, "pub", "o", "open")
	e.put(t, "priv", "o", "closed")

	tests := []struct {
		name string
		path string
		want int
	}{
		{name: "public object", path: "/n/" + e.ns + "/b/pub/o/o", want: 200},
		{name: "public list denied without ObjectRead", path: "/n/" + e.ns + "/b/pub/o", want: 401},
		{name: "private object", path: "/n/" + e.ns + "/b/priv/o/o", want: 401},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := rawDo(t, http.MethodGet, e.baseURL+tt.path, "")
			assert.Equal(t, tt.want, code)
		})
	}

	_ = common.String
}
