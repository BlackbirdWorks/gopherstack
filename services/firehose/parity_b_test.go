package firehose_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_UpdateDestination_HTTPEndpoint verifies that UpdateDestination
// accepts an HTTP endpoint update and persists it so DescribeDeliveryStream
// returns the updated URL. Real AWS supports updating all destination types.
func TestParity_UpdateDestination_HTTPEndpoint(t *testing.T) {
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
					"DataTableName":  "events",
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
					"DataTableName":  "events_v2",
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

// TestParity_DescribeDeliveryStream_OpenSearchAndSplunk verifies that
// DescribeDeliveryStream includes OpenSearch and Splunk destination descriptions
// in the response. Real AWS includes all configured destinations.
func TestParity_DescribeDeliveryStream_OpenSearchAndSplunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createBody  map[string]any
		wantContain string
		wantKey     string
	}{
		{
			name: "opensearch_in_describe",
			createBody: map[string]any{
				"DeliveryStreamName": "aos-desc-stream",
				"AmazonOpenSearchServiceDestinationConfiguration": map[string]any{
					"DomainARN": "arn:aws:es:us-east-1:000000000000:domain/my-domain",
					"IndexName": "access-logs",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
				},
			},
			wantKey:     "AmazonOpenSearchServiceDestinationDescriptions",
			wantContain: "my-domain",
		},
		{
			name: "splunk_in_describe",
			createBody: map[string]any{
				"DeliveryStreamName": "splunk-desc-stream",
				"SplunkDestinationConfiguration": map[string]any{
					"HECEndpoint":     "https://my-splunk.example.com:8088",
					"HECEndpointType": "Raw",
					"HECToken":        "my-token",
				},
			},
			wantKey:     "SplunkDestinationDescriptions",
			wantContain: "my-splunk.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)

			create := doFirehoseRequest(t, h, "CreateDeliveryStream", tt.createBody)
			require.Equal(t, http.StatusOK, create.Code)

			desc := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
				"DeliveryStreamName": tt.createBody["DeliveryStreamName"],
			})
			require.Equal(t, http.StatusOK, desc.Code)

			body := desc.Body.String()
			assert.Contains(t, body, tt.wantKey)
			assert.Contains(t, body, tt.wantContain)
		})
	}
}

// TestParity_UpdateDestination_VersionIncrements_NonS3 verifies that updating
// a non-S3 destination increments VersionId just like S3 updates do.
func TestParity_UpdateDestination_VersionIncrements_NonS3(t *testing.T) {
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
