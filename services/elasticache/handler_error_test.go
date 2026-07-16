package elasticache_test

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// errorEnvelope mirrors the AWS query-protocol error response so the wire shape
// can be asserted directly.
type errorEnvelope struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Error   struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestID string `xml:"RequestId"`
}

func newErrorTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)
	handler.Region = "us-east-1"

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func postAction(t *testing.T, srvURL string, form url.Values) (int, errorEnvelope) {
	t.Helper()

	form.Set("Version", "2015-02-02")
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		srvURL,
		strings.NewReader(form.Encode()),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var env errorEnvelope
	require.NoError(t, xml.Unmarshal(body, &env), "response body: %s", body)

	return resp.StatusCode, env
}

func TestXMLErrorEnvelopeShape(t *testing.T) {
	t.Parallel()

	srv := newErrorTestServer(t)

	tests := []struct {
		form     url.Values
		name     string
		wantCode string
		wantType string
	}{
		{
			name: "not_found_is_sender",
			form: url.Values{
				"Action":         {"DescribeCacheClusters"},
				"CacheClusterId": {"missing-cluster"},
			},
			wantCode: "CacheClusterNotFound",
			wantType: "Sender",
		},
		{
			name: "delete_missing_cluster_is_sender",
			form: url.Values{
				"Action":         {"DeleteCacheCluster"},
				"CacheClusterId": {"nope"},
			},
			wantCode: "CacheClusterNotFound",
			wantType: "Sender",
		},
		{
			name: "snapshot_not_found_is_sender",
			form: url.Values{
				"Action":       {"DeleteSnapshot"},
				"SnapshotName": {"ghost"},
			},
			wantCode: "SnapshotNotFoundFault",
			wantType: "Sender",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, env := postAction(t, srv.URL, tt.form)

			assert.GreaterOrEqual(t, status, http.StatusBadRequest)
			assert.Equal(t, tt.wantType, env.Error.Type, "fault Type element must be present and classified")
			assert.Equal(t, tt.wantCode, env.Error.Code, "error Code must match")
			assert.NotEmpty(t, env.Error.Message, "error Message must be present")
			assert.NotEmpty(t, env.Xmlns, "ErrorResponse must carry the elasticache namespace")

			// RequestId must be a fresh UUID, not a static stub.
			require.NotEmpty(t, env.RequestID)
			_, parseErr := uuid.Parse(env.RequestID)
			require.NoError(t, parseErr, "RequestId should be a UUID, got %q", env.RequestID)
			assert.NotEqual(t, "elasticache-stub", env.RequestID)
		})
	}
}

func TestXMLErrorRequestIDsAreUnique(t *testing.T) {
	t.Parallel()

	srv := newErrorTestServer(t)

	form := url.Values{"Action": {"DescribeCacheClusters"}, "CacheClusterId": {"missing"}}

	_, first := postAction(t, srv.URL, form)
	_, second := postAction(t, srv.URL, form)

	require.NotEmpty(t, first.RequestID)
	require.NotEmpty(t, second.RequestID)
	assert.NotEqual(t, first.RequestID, second.RequestID, "each error response must carry a distinct RequestId")
}

// errorFault constrains PT to be a pointer to T that implements error, so
// [requireFault] can hand back a concretely-typed *T for the caller to
// inspect further while still being driven off a single type argument.
type errorFault[T any] interface {
	*T
	error
}

// requireFault asserts err unwraps to the exact SDK-modeled fault type T via
// [errors.As] and returns it. If the emulator's wire <Code> (or HTTP status,
// checked separately) doesn't match what aws-sdk-go-v2's query-protocol
// deserializer expects for that fault, the SDK falls back to a generic
// smithy error and this assertion fails — this is what makes the check a
// real wire-shape proof rather than a bare "err != nil".
func requireFault[T any, PT errorFault[T]](t *testing.T, err error) PT {
	t.Helper()

	var target PT

	require.ErrorAsf(t, err, &target, "expected error to unwrap to %T, got %v", target, err)

	return target
}

// requireHTTPStatus asserts the HTTP status code the SDK observed on the
// response, independent of which fault type it deserialized to.
func requireHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()

	var respErr *smithyhttp.ResponseError

	require.ErrorAsf(t, err, &respErr, "expected a smithy http.ResponseError in the chain, got %v", err)
	assert.Equal(t, want, respErr.HTTPStatusCode())
}

// Test_ErrorWireShapesMatchAWS is a regression test for the parity-sweep-3
// error-code/HTTP-status audit: many NotFound/AlreadyExists faults were
// wired with the wrong <Code> string (e.g. "ReplicationGroupNotFound"
// instead of the wire-correct "ReplicationGroupNotFoundFault") and/or the
// wrong HTTP status (AWS's query-protocol model marks most *NotFoundFault
// shapes 404, not 400, per api-2.json's per-shape httpStatusCode -- see
// PARITY.md Notes). Each case below drives the real aws-sdk-go-v2 client
// against the emulator and confirms both the typed fault and the status
// code the SDK observed.
func Test_ErrorWireShapesMatchAWS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       func(t *testing.T, client *elasticachesdk.Client) error
		checkFault func(t *testing.T, err error)
		name       string
		wantStatus int
	}{
		{
			name: "CacheClusterNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
					CacheClusterId: aws.String("no-such-cluster"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheClusterNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReplicationGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
					ReplicationGroupId: aws.String("no-such-rg"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReplicationGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "CacheParameterGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheParameterGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheParameterGroupsInput{
						CacheParameterGroupName: aws.String("no-such-pg"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheParameterGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			// AWS's own model marks this one 400, not 404 -- an exception to the
			// otherwise-consistent "NotFoundFault => 404" rule for this API.
			name: "CacheSubnetGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheSubnetGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSubnetGroupsInput{CacheSubnetGroupName: aws.String("no-such-sng")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheSubnetGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "CacheSecurityGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheSecurityGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSecurityGroupsInput{CacheSecurityGroupName: aws.String("no-such-csg")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheSecurityGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
					UserId: aws.String("no-such-user"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeUserGroups(t.Context(), &elasticachesdk.DescribeUserGroupsInput{
					UserGroupId: aws.String("no-such-ug"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ServerlessCacheNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeServerlessCaches(t.Context(), &elasticachesdk.DescribeServerlessCachesInput{
					ServerlessCacheName: aws.String("no-such-sc"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ServerlessCacheNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "GlobalReplicationGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeGlobalReplicationGroups(
					t.Context(),
					&elasticachesdk.DescribeGlobalReplicationGroupsInput{
						GlobalReplicationGroupId: aws.String("no-such-grg"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.GlobalReplicationGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "SnapshotNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DeleteSnapshot(t.Context(), &elasticachesdk.DeleteSnapshotInput{
					SnapshotName: aws.String("no-such-snap"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.SnapshotNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReservedCacheNodeNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReservedCacheNodes(
					t.Context(),
					&elasticachesdk.DescribeReservedCacheNodesInput{ReservedCacheNodeId: aws.String("no-such-rcn")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodeNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReservedCacheNodesOfferingNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReservedCacheNodesOfferings(
					t.Context(),
					&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{
						ReservedCacheNodesOfferingId: aws.String("no-such-offering"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodesOfferingNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserGroupAlreadyExists",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				input := &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("dup-ug"),
					Engine:      aws.String("redis"),
				}
				_, err := client.CreateUserGroup(t.Context(), input)
				require.NoError(t, err, "first CreateUserGroup must succeed")

				_, err = client.CreateUserGroup(t.Context(), input)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserGroupAlreadyExistsFault](t, err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models this AlreadyExists fault as 404, not the 409/400 one
			// might expect -- see api-2.json ReservedCacheNodeAlreadyExistsFault.
			name: "ReservedCacheNodeAlreadyExists",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				input := &elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
					ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
					ReservedCacheNodeId:          aws.String("dup-rcn"),
				}
				_, err := client.PurchaseReservedCacheNodesOffering(t.Context(), input)
				require.NoError(t, err, "first purchase must succeed")

				_, err = client.PurchaseReservedCacheNodesOffering(t.Context(), input)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodeAlreadyExistsFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			err := tt.call(t, client)
			require.Error(t, err)

			tt.checkFault(t, err)
			requireHTTPStatus(t, err, tt.wantStatus)
		})
	}
}
