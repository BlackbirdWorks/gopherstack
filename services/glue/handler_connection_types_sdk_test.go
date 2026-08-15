package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestSDKRoundTrip_DescribeConnectionType_Capabilities drives
// DescribeConnectionType through the real aws-sdk-go-v2 client for a
// built-in read/write connector and proves Capabilities decodes as the real
// *types.Capabilities struct (gopherstack-ustu). Previously Capabilities was
// a bare []string of "READ"/"WRITE"; a real client's deserializer expects an
// object there and rejects the whole response body on the mismatch, so this
// call could not succeed against the unfixed handler at all.
func TestSDKRoundTrip_DescribeConnectionType_Capabilities(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.DescribeConnectionType(t.Context(), &gluesdk.DescribeConnectionTypeInput{
		ConnectionType: aws.String("JDBC"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Capabilities, "JDBC is a built-in read/write connector with real DataOperations state")
	assert.ElementsMatch(t,
		[]types.DataOperation{types.DataOperationRead, types.DataOperationWrite},
		out.Capabilities.SupportedDataOperations)
	assert.Empty(t, out.Capabilities.SupportedAuthenticationTypes,
		"no per-connector auth-type state exists in this backend")
	assert.Empty(t, out.Capabilities.SupportedComputeEnvironments,
		"no per-connector compute-environment state exists in this backend")
}

// TestSDKRoundTrip_DescribeConnectionType_NoCapabilitiesState proves
// Capabilities is omitted entirely (not an empty-but-present object) for a
// connector category this backend never modeled DataOperations for, rather
// than fabricating a placeholder.
func TestSDKRoundTrip_DescribeConnectionType_NoCapabilitiesState(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.DescribeConnectionType(t.Context(), &gluesdk.DescribeConnectionTypeInput{
		ConnectionType: aws.String("NETWORK"),
	})
	require.NoError(t, err)
	assert.Nil(t, out.Capabilities)
}

// TestSDKRoundTrip_ListConnectionTypes drives ListConnectionTypes through the
// real client and proves ConnectionTypeBrief.Categories decodes as the real
// []string (plural), and Capabilities decodes as the real *types.Capabilities
// struct -- the same two shape bugs as DescribeConnectionType
// (gopherstack-ustu), found by reading ConnectionTypeBrief's full real shape
// rather than only the field named in the bug report.
func TestSDKRoundTrip_ListConnectionTypes(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.ListConnectionTypes(t.Context(), &gluesdk.ListConnectionTypesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.ConnectionTypes)

	var jdbc *types.ConnectionTypeBrief

	for i := range out.ConnectionTypes {
		if out.ConnectionTypes[i].ConnectionType == types.ConnectionType("JDBC") {
			jdbc = &out.ConnectionTypes[i]

			break
		}
	}

	require.NotNil(t, jdbc, "JDBC must be present in the built-in catalog")
	assert.Equal(t, []string{"DATABASE"}, jdbc.Categories)
	require.NotNil(t, jdbc.Capabilities)
	assert.ElementsMatch(t,
		[]types.DataOperation{types.DataOperationRead, types.DataOperationWrite},
		jdbc.Capabilities.SupportedDataOperations)
}

// TestConnectionTypeWireShape asserts the raw JSON body directly (independent
// of the SDK client's own tolerance) to lock in the exact fixed shape: a real
// object under "Capabilities" rather than a bare array, "Categories" (plural
// list) rather than "Category" (singular string) on ListConnectionTypes, and
// no fabricated "Category" key at all on DescribeConnectionType. Each
// subtest fails against the pre-fix shape.
func TestConnectionTypeWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check  func(t *testing.T, body []byte)
		input  map[string]any
		name   string
		action string
	}{
		{
			name:   "describe capabilities is an object not an array",
			action: "DescribeConnectionType",
			input:  map[string]any{"ConnectionType": "JDBC"},
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var out struct {
					Capabilities struct {
						SupportedAuthenticationTypes []string `json:"SupportedAuthenticationTypes"`
						SupportedComputeEnvironments []string `json:"SupportedComputeEnvironments"`
						SupportedDataOperations      []string `json:"SupportedDataOperations"`
					} `json:"Capabilities"`
				}
				require.NoError(t, json.Unmarshal(body, &out))
				assert.ElementsMatch(t, []string{"READ", "WRITE"}, out.Capabilities.SupportedDataOperations)
			},
		},
		{
			name:   "describe has no fabricated category key",
			action: "DescribeConnectionType",
			input:  map[string]any{"ConnectionType": "JDBC"},
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var out map[string]any
				require.NoError(t, json.Unmarshal(body, &out))
				assert.NotContains(t, out, "Category",
					"DescribeConnectionTypeOutput has no Category member on the real wire")
			},
		},
		{
			name:   "list uses plural categories",
			action: "ListConnectionTypes",
			input:  map[string]any{},
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var out struct {
					ConnectionTypes []struct {
						ConnectionType string   `json:"ConnectionType"`
						Categories     []string `json:"Categories"`
					} `json:"ConnectionTypes"`
				}
				require.NoError(t, json.Unmarshal(body, &out))

				found := false

				for _, ct := range out.ConnectionTypes {
					if ct.ConnectionType == "JDBC" {
						found = true

						assert.Equal(t, []string{"DATABASE"}, ct.Categories)
					}
				}

				assert.True(t, found, "JDBC must be present")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tt.action, tt.input)
			require.Equal(t, http.StatusOK, rec.Code)

			tt.check(t, rec.Body.Bytes())
		})
	}
}
