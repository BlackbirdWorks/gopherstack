package directoryservice_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// newTestHandler constructs a Handler backed by a fresh in-memory backend,
// shared by every test file in this package.
func newTestHandler(t *testing.T) *directoryservice.Handler {
	t.Helper()

	return directoryservice.NewHandler(directoryservice.NewInMemoryBackend("000000000000", "us-east-1"))
}

// doRequest issues op against h with body marshaled as the JSON payload and
// returns the recorded response, shared by every test file in this package.
func doRequest(t *testing.T, h *directoryservice.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "DirectoryService_20150416."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

// mustCreateSimpleAD creates a Simple AD directory and returns its ID.
func mustCreateSimpleAD(t *testing.T, h *directoryservice.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateDirectory", map[string]any{
		"Name":     name,
		"Password": "Admin1234!",
		"Size":     "Small",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["DirectoryId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

// mustCreateMicrosoftAD creates a MicrosoftAD directory and returns its ID.
func mustCreateMicrosoftAD(t *testing.T, h *directoryservice.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateMicrosoftAD", map[string]any{
		"Name":     name,
		"Password": "Admin1234!",
		"Edition":  "Enterprise",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["DirectoryId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

// respBody decodes rec's JSON body into a map.
func respBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// isBase64Int returns true if s is a base64-encoded decimal integer.
func isBase64Int(s string) bool {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return false
	}

	_, err = strconv.Atoi(string(b))

	return err == nil
}

// TestUnknownOperation verifies that an unrecognised, empty, or partial
// operation name is rejected with an AWS-style 400 InvalidRequestException
// rather than a 404/501.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantInBody string
		wantCode   int
	}{
		{
			name: "completely_unknown_op", op: "NonExistentOperation",
			wantCode: http.StatusBadRequest, wantInBody: "InvalidRequestException",
		},
		{name: "malformed_op_name", op: "", wantCode: http.StatusBadRequest},
		{name: "partial_op_name", op: "CreateDirec", wantCode: http.StatusBadRequest},
		{
			name: "bogus_op", op: "NoSuchOperation",
			wantCode: http.StatusBadRequest, wantInBody: "InvalidRequestException",
		},
		{
			name: "another_bogus_op", op: "Frobnicate",
			wantCode: http.StatusBadRequest, wantInBody: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

func TestErrorCodeShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *directoryservice.Handler) (string, any)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "DeleteDirectory unknown returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DeleteDirectory", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "CreateAlias duplicate returns EntityAlreadyExistsException",
			setup: func(h *directoryservice.Handler) (string, any) {
				dirID := mustCreateSimpleAD(t, h, "corp.example.com")
				doRequest(
					t,
					h,
					"CreateAlias",
					map[string]any{"DirectoryId": dirID, "Alias": "myalias"},
				)
				dir2 := mustCreateSimpleAD(t, h, "other.example.com")

				return "CreateAlias", map[string]any{"DirectoryId": dir2, "Alias": "myalias"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityAlreadyExistsException",
		},
		{
			name: "DescribeDirectories unknown ID returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DescribeDirectories", map[string]any{
					"DirectoryIds": []string{"d-0000000000"},
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "DeleteSnapshot unknown returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "DeleteSnapshot", map[string]any{"SnapshotId": "s-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "EnableSso unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "EnableSso", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "AddTagsToResource unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "AddTagsToResource", map[string]any{
					"ResourceId": "d-0000000000",
					"Tags":       []map[string]any{{"Key": "k", "Value": "v"}},
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "GetSnapshotLimits unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "GetSnapshotLimits", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "ListIpRoutes unknown directory returns EntityDoesNotExistException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "ListIpRoutes", map[string]any{"DirectoryId": "d-0000000000"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "EntityDoesNotExistException",
		},
		{
			name: "invalid JSON body returns ClientException",
			setup: func(_ *directoryservice.Handler) (string, any) {
				return "CreateDirectory", "not-json"
			},
			wantCode: http.StatusBadRequest,
			wantType: "ClientException",
		},
		{
			name: "DirectoryLimitExceededException on 11th SimpleAD",
			setup: func(h *directoryservice.Handler) (string, any) {
				for i := range 10 {
					doRequest(t, h, "CreateDirectory", map[string]any{
						"Name": fmt.Sprintf(
							"corp%d.example.com",
							i,
						), "Password": "Admin1234!", "Size": "Small",
					})
				}

				return "CreateDirectory", map[string]any{
					"Name":     "overflow.example.com",
					"Password": "Admin1234!",
					"Size":     "Small",
				}
			},
			wantCode: http.StatusBadRequest,
			wantType: "DirectoryLimitExceededException",
		},
		{
			name: "SnapshotLimitExceededException on 6th snapshot",
			setup: func(h *directoryservice.Handler) (string, any) {
				dirID := mustCreateSimpleAD(t, h, "corp.example.com")
				for i := range 5 {
					doRequest(
						t,
						h,
						"CreateSnapshot",
						map[string]any{"DirectoryId": dirID, "Name": fmt.Sprintf("s%d", i)},
					)
				}

				return "CreateSnapshot", map[string]any{"DirectoryId": dirID, "Name": "overflow"}
			},
			wantCode: http.StatusBadRequest,
			wantType: "SnapshotLimitExceededException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			op, body := tt.setup(h)
			rec := doRequest(t, h, op, body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantType != "" {
				b := respBody(t, rec)
				assert.Equal(t, tt.wantType, b["__type"])
			}
		})
	}
}

// --- Pagination ---

// TestPaginationTokensAreOpaque verifies that nextToken values are base64-encoded
// integer offsets, not raw item IDs.
func TestPaginationTokensAreOpaque(t *testing.T) {
	t.Parallel()

	t.Run("DescribeDirectories token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		for i := range 3 {
			mustCreateSimpleAD(t, h, fmt.Sprintf("d%d.example.com", i))
		}

		rec := doRequest(t, h, "DescribeDirectories", map[string]any{"Limit": 2})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)

		// Decoded offset must equal page size.
		b, _ := base64.StdEncoding.DecodeString(tok)
		offset, _ := strconv.Atoi(string(b))
		assert.Equal(t, 2, offset)
	})

	t.Run("DescribeSnapshots token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		for range 3 {
			doRequest(t, h, "CreateSnapshot", map[string]any{
				"DirectoryId": dirID,
				"Name":        "snap",
			})
		}

		rec := doRequest(t, h, "DescribeSnapshots", map[string]any{
			"DirectoryId": dirID,
			"Limit":       2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)
	})

	t.Run("ListTagsForResource token is base64 integer", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		tags := make([]map[string]any, 3)
		for i := range 3 {
			tags[i] = map[string]any{"Key": fmt.Sprintf("k%d", i), "Value": "v"}
		}

		doRequest(t, h, "AddTagsToResource", map[string]any{
			"ResourceId": dirID,
			"Tags":       tags,
		})

		rec := doRequest(t, h, "ListTagsForResource", map[string]any{
			"ResourceId": dirID,
			"Limit":      2,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		body := respBody(t, rec)
		tok, _ := body["NextToken"].(string)
		require.NotEmpty(t, tok, "expected nextToken for page 1 of 3")
		assert.True(t, isBase64Int(tok), "nextToken must be base64-encoded integer, got %q", tok)
	})
}

// TestTimestamps_AreEpochSeconds guards against the wire bug where
// Appendix A list/describe handlers formatted timestamps as ISO8601 strings
// (time.Format("2006-01-02T15:04:05.000Z")). DirectoryService uses the AWS
// json-1.1 protocol, whose real aws-sdk-go-v2 deserializers call
// smithytime.ParseEpochSeconds on every timestamp field (confirmed against
// aws-sdk-go-v2/service/directoryservice's deserializers.go) and therefore
// reject a JSON string with "expected Timestamp to be a JSON Number, got
// string instead". Every timestamp field below must decode as a JSON number
// (float64), never a string.
func TestTimestamps_AreEpochSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, h *directoryservice.Handler, dirID string)
		call    func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any
		extract func(t *testing.T, resp map[string]any) any
		name    string
	}{
		{
			name: "ListIpRoutes AddedDateTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "AddIpRoutes", map[string]any{
					"DirectoryId": dirID,
					"IpRoutes":    []map[string]any{{"CidrIp": "10.0.0.0/24"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()

				return respBody(t, doRequest(t, h, "ListIpRoutes", map[string]any{"DirectoryId": dirID}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				list, _ := resp["IpRoutesInfo"].([]any)
				require.Len(t, list, 1)
				entry, _ := list[0].(map[string]any)

				return entry["AddedDateTime"]
			},
		},
		{
			name: "DescribeRegions LaunchTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "AddRegion", map[string]any{"DirectoryId": dirID, "RegionName": "us-west-2"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()

				return respBody(t, doRequest(t, h, "DescribeRegions", map[string]any{"DirectoryId": dirID}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				list, _ := resp["RegionsDescription"].([]any)
				require.Len(t, list, 1)
				entry, _ := list[0].(map[string]any)

				return entry["LaunchTime"]
			},
		},
		{
			name: "ListSchemaExtensions StartDateTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "StartSchemaExtension", map[string]any{
					"DirectoryId": dirID, "LdifContent": "dn: cn=schema",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()

				return respBody(t, doRequest(t, h, "ListSchemaExtensions", map[string]any{"DirectoryId": dirID}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				list, _ := resp["SchemaExtensionsInfo"].([]any)
				require.Len(t, list, 1)
				entry, _ := list[0].(map[string]any)

				return entry["StartDateTime"]
			},
		},
		{
			name: "DescribeTrusts CreatedDateTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "CreateTrust", map[string]any{
					"DirectoryId": dirID, "RemoteDomainName": "trusted.example.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()

				return respBody(t, doRequest(t, h, "DescribeTrusts", map[string]any{"DirectoryId": dirID}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				list, _ := resp["Trusts"].([]any)
				require.Len(t, list, 1)
				entry, _ := list[0].(map[string]any)

				return entry["CreatedDateTime"]
			},
		},
		{
			name: "DescribeSharedDirectories CreatedDateTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "ShareDirectory", map[string]any{
					"DirectoryId": dirID,
					"ShareTarget": map[string]any{"Id": "222222222222", "Type": "ACCOUNT"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()
				rec := doRequest(t, h, "DescribeSharedDirectories", map[string]any{"OwnerDirectoryId": dirID})

				return respBody(t, rec)
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				list, _ := resp["SharedDirectories"].([]any)
				require.Len(t, list, 1)
				entry, _ := list[0].(map[string]any)

				return entry["CreatedDateTime"]
			},
		},
		{
			name: "DescribeCertificate ExpiryDateTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "RegisterCertificate", map[string]any{
					"DirectoryId": dirID, "CertificateData": "cert-data",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()
				listRec := doRequest(t, h, "ListCertificates", map[string]any{"DirectoryId": dirID})
				listResp := respBody(t, listRec)
				certs, _ := listResp["CertificatesInfo"].([]any)
				require.Len(t, certs, 1)
				certEntry, _ := certs[0].(map[string]any)
				certID, _ := certEntry["CertificateId"].(string)
				require.NotEmpty(t, certID)

				return respBody(t, doRequest(t, h, "DescribeCertificate", map[string]any{
					"DirectoryId": dirID, "CertificateId": certID,
				}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				cert, _ := resp["Certificate"].(map[string]any)

				return cert["ExpiryDateTime"]
			},
		},
		{
			name: "DescribeADAssessment StartTime",
			setup: func(t *testing.T, h *directoryservice.Handler, dirID string) {
				t.Helper()
				rec := doRequest(t, h, "StartADAssessment", map[string]any{"DirectoryId": dirID})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			call: func(t *testing.T, h *directoryservice.Handler, dirID string) map[string]any {
				t.Helper()
				listRec := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
				listResp := respBody(t, listRec)
				assessments, _ := listResp["ADAssessments"].([]any)
				require.Len(t, assessments, 1)
				entry, _ := assessments[0].(map[string]any)
				assessmentID, _ := entry["AssessmentId"].(string)
				require.NotEmpty(t, assessmentID)

				return respBody(t, doRequest(t, h, "DescribeADAssessment", map[string]any{
					"DirectoryId": dirID, "AssessmentId": assessmentID,
				}))
			},
			extract: func(t *testing.T, resp map[string]any) any {
				t.Helper()
				assessment, _ := resp["ADAssessment"].(map[string]any)

				return assessment["StartTime"]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")
			tt.setup(t, h, dirID)
			resp := tt.call(t, h, dirID)
			value := tt.extract(t, resp)

			require.NotNil(t, value, "timestamp field must be present")
			_, isFloat := value.(float64)
			assert.True(t, isFloat,
				"timestamp must decode as a JSON number (epoch seconds), got %T: %v", value, value)
		})
	}
}

// TestHandler_PersistenceDelegation guards against the disguised no-op where
// InMemoryBackend.BackendSnapshot/Restore are fully implemented but Handler
// never exposed them under the names (Snapshot(ctx) []byte /
// Restore(ctx, []byte) error) that cli.go's setupPersistence type-asserts
// against. Without that delegation, setupPersistence's `svc.(persistable)`
// check silently fails and the persistence manager never registers
// directoryservice at all -- so no snapshot is ever taken and no restore ever
// runs, regardless of how correct the underlying backend's own
// BackendSnapshot/Restore are.
func TestHandler_PersistenceDelegation(t *testing.T) {
	t.Parallel()

	// The exact shape cli.go's setupPersistence type-asserts services
	// against (see persistence.Persistable / the local `persistable`
	// interface in cli.go's setupPersistence).
	type persistable interface {
		Snapshot(ctx context.Context) []byte
		Restore(ctx context.Context, data []byte) error
	}

	h := newTestHandler(t)

	p, ok := any(h).(persistable)
	require.True(t, ok,
		"Handler must implement Snapshot(ctx)/Restore(ctx,[]byte) or setupPersistence never registers it")

	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	ctx := t.Context()
	snap := p.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restoredBackend := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	restoredHandler := directoryservice.NewHandler(restoredBackend)
	restoredP, ok := any(restoredHandler).(persistable)
	require.True(t, ok)
	require.NoError(t, restoredP.Restore(ctx, snap))

	dirs, _, err := restoredBackend.DescribeDirectories(ctx, []string{dirID}, 0, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1,
		"directory created before Snapshot must survive the Handler-level Snapshot/Restore round trip")
	assert.Equal(t, dirID, dirs[0].DirectoryID)
}
