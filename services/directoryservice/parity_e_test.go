package directoryservice_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestAppendixATimestamps_AreEpochSeconds guards against the wire bug where
// Appendix A list/describe handlers formatted timestamps as ISO8601 strings
// (time.Format("2006-01-02T15:04:05.000Z")). DirectoryService uses the AWS
// json-1.1 protocol, whose real aws-sdk-go-v2 deserializers call
// smithytime.ParseEpochSeconds on every timestamp field (confirmed against
// aws-sdk-go-v2/service/directoryservice's deserializers.go) and therefore
// reject a JSON string with "expected Timestamp to be a JSON Number, got
// string instead". Every timestamp field below must decode as a JSON number
// (float64), never a string.
func TestAppendixATimestamps_AreEpochSeconds(t *testing.T) {
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

// TestShareDirectory_StatusLifecycle guards two ShareStatus bugs in the
// shared-directory family:
//
//  1. ShareDirectory with the (default) HANDSHAKE method must start
//     PendingAcceptance, since AWS requires the consumer account to call
//     AcceptSharedDirectory before the share becomes Shared -- an
//     ORGANIZATIONS-method share needs no handshake and starts Shared
//     immediately.
//  2. RejectSharedDirectory must transition the share to the terminal
//     "Rejected" state (the successful outcome), not "RejectFailed" (which
//     is the AWS enum value for a FAILED reject attempt) -- the previous
//     code inverted this, marking every successful reject as if it had
//     failed.
func TestShareDirectory_StatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		shareMethod    string
		wantAfterShare string
		name           string
	}{
		{name: "HANDSHAKE starts PendingAcceptance", shareMethod: "HANDSHAKE", wantAfterShare: "PendingAcceptance"},
		{name: "ORGANIZATIONS starts Shared", shareMethod: "ORGANIZATIONS", wantAfterShare: "Shared"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			shareRec := doRequest(t, h, "ShareDirectory", map[string]any{
				"DirectoryId": dirID,
				"ShareMethod": tt.shareMethod,
				"ShareTarget": map[string]any{"Id": "222222222222", "Type": "ACCOUNT"},
			})
			require.Equal(t, http.StatusOK, shareRec.Code)

			descRec := doRequest(t, h, "DescribeSharedDirectories", map[string]any{"OwnerDirectoryId": dirID})
			resp := respBody(t, descRec)
			list, _ := resp["SharedDirectories"].([]any)
			require.Len(t, list, 1)
			entry, _ := list[0].(map[string]any)
			assert.Equal(t, tt.wantAfterShare, entry["ShareStatus"])
		})
	}

	t.Run("RejectSharedDirectory transitions to Rejected", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		shareRec := doRequest(t, h, "ShareDirectory", map[string]any{
			"DirectoryId": dirID,
			"ShareMethod": "HANDSHAKE",
			"ShareTarget": map[string]any{"Id": "222222222222", "Type": "ACCOUNT"},
		})
		require.Equal(t, http.StatusOK, shareRec.Code)
		var shareResp map[string]any
		require.NoError(t, json.Unmarshal(shareRec.Body.Bytes(), &shareResp))
		sharedID, _ := shareResp["SharedDirectoryId"].(string)
		require.NotEmpty(t, sharedID)

		rejectRec := doRequest(t, h, "RejectSharedDirectory", map[string]any{"SharedDirectoryId": sharedID})
		require.Equal(t, http.StatusOK, rejectRec.Code)

		descRec := doRequest(t, h, "DescribeSharedDirectories", map[string]any{"OwnerDirectoryId": dirID})
		resp := respBody(t, descRec)
		list, _ := resp["SharedDirectories"].([]any)
		require.Len(t, list, 1)
		entry, _ := list[0].(map[string]any)
		assert.Equal(t, "Rejected", entry["ShareStatus"])
	})
}
