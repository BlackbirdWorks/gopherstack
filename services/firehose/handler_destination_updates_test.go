package firehose_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestFirehoseHandler_UpdateDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *firehose.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
					"DeliveryStreamName": "upd-stream",
				})
			},
			body: map[string]any{
				"DeliveryStreamName":             "upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"S3DestinationUpdate": map[string]any{
					"BucketARN": "arn:aws:s3:::new-bucket",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"DeliveryStreamName": "nonexistent",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "UpdateDestination", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUpdateDestination_Success_VersionIncrements verifies that a successful
// UpdateDestination increments VersionId and persists the new destination config.
func TestUpdateDestination_Success_VersionIncrements(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "upd-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "upd-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	var descOut struct {
		DeliveryStreamDescription struct {
			VersionID    string `json:"VersionId"`
			Destinations []struct {
				DestinationID string `json:"DestinationId"`
			} `json:"Destinations"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &descOut))
	versionBefore := descOut.DeliveryStreamDescription.VersionID

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "upd-stream",
		"CurrentDeliveryStreamVersionId": versionBefore,
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::updated-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc2 := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "upd-stream"})
	require.Equal(t, http.StatusOK, desc2.Code)

	var descOut2 struct {
		DeliveryStreamDescription struct {
			VersionID    string `json:"VersionId"`
			Destinations []struct {
				ExtendedS3DestinationDescription struct {
					BucketARN string `json:"BucketARN"`
				} `json:"ExtendedS3DestinationDescription"`
			} `json:"Destinations"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(desc2.Body.Bytes(), &descOut2))
	assert.NotEqual(t, versionBefore, descOut2.DeliveryStreamDescription.VersionID)
	require.Len(t, descOut2.DeliveryStreamDescription.Destinations, 1)
	assert.Equal(t, "arn:aws:s3:::updated-bucket",
		descOut2.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription.BucketARN)
}

// TestUpdateDestination_VersionMismatch verifies that a stale
// CurrentDeliveryStreamVersionId is rejected.
func TestUpdateDestination_VersionMismatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ver-stream")

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-stream",
		"CurrentDeliveryStreamVersionId": "999",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandlerUpdateDestination_VersionMismatch verifies a version-mismatched update is
// rejected with 400, exercising a DestinationId-qualified request body.
func TestHandlerUpdateDestination_VersionMismatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "ver-mismatch"})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-mismatch",
		"CurrentDeliveryStreamVersionId": "99",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestUpdateDestination_ExtendedS3Update verifies UpdateDestination applies an
// ExtendedS3DestinationUpdate.
func TestUpdateDestination_ExtendedS3Update(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ext-upd-stream", map[string]any{
		"ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::orig-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ext-upd-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"ExtendedS3DestinationUpdate": map[string]any{
			"BucketARN":         "arn:aws:s3:::new-ext-bucket",
			"RoleARN":           "arn:aws:iam::000000000000:role/r",
			"ErrorOutputPrefix": "errors/",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "ext-upd-stream"})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), "new-ext-bucket")
	assert.Contains(t, desc.Body.String(), "errors/")
}

// TestUpdateDestination_ChangesS3Bucket verifies that after UpdateDestination with a new
// S3 bucket, subsequent flush delivers to the new bucket.
func TestUpdateDestination_ChangesS3Bucket(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "upd-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original-bucket",
		},
	})

	// Update to new bucket, version must match current ("1").
	upd := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "upd-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::updated-bucket",
		},
	})
	require.Equal(t, http.StatusOK, upd.Code)

	// Verify VersionId incremented.
	desc := auditDescribe(t, h, "upd-stream")
	assert.Equal(t, "2", desc["VersionId"])

	// Flush goes to new bucket.
	auditPut(t, h, "upd-stream", "payload")
	h.Backend.(*firehose.InMemoryBackend).FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	assert.Equal(t, "updated-bucket", s3mock.calls[0].bucket)
}

// TestUpdateDestination_VersionMismatchRejected verifies that a stale VersionId is
// rejected with 400 InvalidArgumentException.
func TestUpdateDestination_VersionMismatchRejected(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	auditCreateStream(t, h, "ver-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::ver-bucket",
		},
	})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-stream",
		"CurrentDeliveryStreamVersionId": "99",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidArgumentException", body["__type"])
}

// TestUpdateDestination_HTTPEndpoint verifies that UpdateDestination accepts updates for
// every destination type and persists them so DescribeDeliveryStream returns the
// updated fields. Real AWS supports updating all destination types.
func TestUpdateDestination_HTTPEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createBody  map[string]any
		updateBody  map[string]any
		wantContain string
	}{
		{
			name: "http_endpoint_update_persisted",
			createBody: map[string]any{
				"DeliveryStreamName": "http-upd-stream",
				"HTTPEndpointDestinationConfiguration": map[string]any{
					"EndpointConfiguration": map[string]any{
						"Url":  "https://original.example.com",
						"Name": "orig",
					},
				},
			},
			updateBody: map[string]any{
				"DeliveryStreamName":             "http-upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"HttpEndpointDestinationUpdate": map[string]any{
					"EndpointConfiguration": map[string]any{
						"Url":  "https://updated.example.com",
						"Name": "updated",
					},
					"BufferingHints": map[string]any{
						"SizeInMBs":         10,
						"IntervalInSeconds": 60,
					},
				},
			},
			wantContain: "updated.example.com",
		},
		{
			name: "redshift_update_persisted",
			createBody: map[string]any{
				"DeliveryStreamName": "rs-upd-stream",
				"RedshiftDestinationConfiguration": map[string]any{
					"ClusterJDBCURL": "jdbc:redshift://cluster.original.redshift.amazonaws.com:5439/db",
					"RoleARN":        "arn:aws:iam::000000000000:role/firehose-role",
					"CopyCommand":    map[string]any{"DataTableName": "events"},
					"Username":       "user",
				},
			},
			updateBody: map[string]any{
				"DeliveryStreamName":             "rs-upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"RedshiftDestinationUpdate": map[string]any{
					"ClusterJDBCURL": "jdbc:redshift://cluster.updated.redshift.amazonaws.com:5439/newdb",
					"RoleARN":        "arn:aws:iam::000000000000:role/firehose-role",
					"CopyCommand":    map[string]any{"DataTableName": "events_v2"},
					"Username":       "user",
				},
			},
			wantContain: "cluster.updated.redshift.amazonaws.com",
		},
		{
			name: "opensearch_update_persisted",
			createBody: map[string]any{
				"DeliveryStreamName": "aos-upd-stream",
				"AmazonOpenSearchServiceDestinationConfiguration": map[string]any{
					"DomainARN": "arn:aws:es:us-east-1:000000000000:domain/original",
					"IndexName": "logs",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose-role",
				},
			},
			updateBody: map[string]any{
				"DeliveryStreamName":             "aos-upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"AmazonOpenSearchServiceDestinationUpdate": map[string]any{
					"DomainARN": "arn:aws:es:us-east-1:000000000000:domain/updated",
					"IndexName": "logs-v2",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose-role",
				},
			},
			wantContain: "domain/updated",
		},
		{
			name: "splunk_update_persisted",
			createBody: map[string]any{
				"DeliveryStreamName": "splunk-upd-stream",
				"SplunkDestinationConfiguration": map[string]any{
					"HECEndpoint":     "https://original.splunk.example.com:8088",
					"HECEndpointType": "Raw",
					"HECToken":        "original-token",
				},
			},
			updateBody: map[string]any{
				"DeliveryStreamName":             "splunk-upd-stream",
				"CurrentDeliveryStreamVersionId": "1",
				"DestinationId":                  "destinationId-000000000001",
				"SplunkDestinationUpdate": map[string]any{
					"HECEndpoint":     "https://updated.splunk.example.com:8088",
					"HECEndpointType": "Event",
					"HECToken":        "updated-token",
				},
			},
			wantContain: "updated.splunk.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)

			create := doFirehoseRequest(t, h, "CreateDeliveryStream", tt.createBody)
			require.Equal(t, http.StatusOK, create.Code, "create failed: %s", create.Body)

			upd := doFirehoseRequest(t, h, "UpdateDestination", tt.updateBody)
			require.Equal(t, http.StatusOK, upd.Code, "update failed: %s", upd.Body)

			desc := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.createBody["DeliveryStreamName"],
			})
			require.Equal(t, http.StatusOK, desc.Code)
			assert.Contains(t, desc.Body.String(), tt.wantContain)
		})
	}
}

// TestUpdateDestination_VersionIncrements_NonS3 verifies that updating a non-S3
// destination increments VersionId just like S3 updates do.
func TestUpdateDestination_VersionIncrements_NonS3(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "http-ver-stream",
		"HTTPEndpointDestinationConfiguration": map[string]any{
			"EndpointConfiguration": map[string]any{
				"Url":  "https://ep.example.com",
				"Name": "ep",
			},
		},
	})

	upd := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "http-ver-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"DestinationId":                  "destinationId-000000000001",
		"HttpEndpointDestinationUpdate": map[string]any{
			"EndpointConfiguration": map[string]any{
				"Url":  "https://ep2.example.com",
				"Name": "ep2",
			},
		},
	})
	require.Equal(t, http.StatusOK, upd.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "http-ver-stream",
	})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), `"VersionId":"2"`)
}
