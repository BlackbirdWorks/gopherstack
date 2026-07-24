package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DistinctErrorCodes verifies that Athena operations surface the
// AWS-accurate __type for their failure mode, rather than every error collapsing
// to InvalidRequestException. Sessions, calculations, and prepared statements
// report ResourceNotFoundException; database/table metadata lookups report
// MetadataException; workgroup/named-query/query-execution failures keep
// InvalidRequestException (which is what AWS returns for those).
func TestHandler_DistinctErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     string
		wantType string
	}{
		{
			name:     "session_not_found_is_resource_not_found",
			action:   "GetSession",
			body:     `{"SessionId":"nope"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "session_status_not_found_is_resource_not_found",
			action:   "GetSessionStatus",
			body:     `{"SessionId":"nope"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "calculation_not_found_is_resource_not_found",
			action:   "GetCalculationExecution",
			body:     `{"CalculationExecutionId":"nope"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "prepared_statement_not_found_is_resource_not_found",
			action:   "GetPreparedStatement",
			body:     `{"StatementName":"nope","WorkGroup":"primary"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "database_not_found_is_metadata_exception",
			action:   "GetDatabase",
			body:     `{"CatalogName":"AwsDataCatalog","DatabaseName":"ghost"}`,
			wantType: "MetadataException",
		},
		{
			name:     "table_not_found_is_metadata_exception",
			action:   "GetTableMetadata",
			body:     `{"CatalogName":"AwsDataCatalog","DatabaseName":"default","TableName":"ghost"}`,
			wantType: "MetadataException",
		},
		{
			name:     "workgroup_not_found_is_invalid_request",
			action:   "GetWorkGroup",
			body:     `{"WorkGroup":"ghost-wg"}`,
			wantType: "InvalidRequestException",
		},
		{
			name:     "query_execution_not_found_is_invalid_request",
			action:   "GetQueryExecution",
			body:     `{"QueryExecutionId":"ghost"}`,
			wantType: "InvalidRequestException",
		},
		{
			name:     "start_query_unknown_workgroup_is_invalid_request",
			action:   "StartQueryExecution",
			body:     `{"QueryString":"SELECT 1","WorkGroup":"ghost-wg"}`,
			wantType: "InvalidRequestException",
		},
		{
			name:   "tag_resource_unknown_arn_is_invalid_request",
			action: "TagResource",
			body: `{"ResourceARN":"arn:aws:athena:us-east-1:000000000000:workgroup/ghost-wg",` +
				`"Tags":[{"Key":"k","Value":"v"}]}`,
			wantType: "InvalidRequestException",
		},
		{
			name:     "untag_resource_unknown_arn_is_invalid_request",
			action:   "UntagResource",
			body:     `{"ResourceARN":"arn:aws:athena:us-east-1:000000000000:workgroup/ghost-wg","TagKeys":["k"]}`,
			wantType: "InvalidRequestException",
		},
		{
			name:     "list_tags_for_resource_unknown_arn_is_invalid_request",
			action:   "ListTagsForResource",
			body:     `{"ResourceARN":"arn:aws:athena:us-east-1:000000000000:workgroup/ghost-wg"}`,
			wantType: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code,
				"Athena client errors are HTTP 400")

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))

			assert.Equal(t, tt.wantType, errResp["__type"],
				"error __type must be AWS-accurate for %s", tt.action)
			assert.NotEmpty(t, errResp["message"], "error message must be populated")
		})
	}
}
