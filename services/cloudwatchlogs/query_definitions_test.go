package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_QueryDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(b *cloudwatchlogs.InMemoryBackend)
		name        string
		opName      string
		queryString string
		id          string
		prefix      string
		op          string
		wantLen     int
	}{
		{
			name:        "put_and_describe_all",
			opName:      "my-query",
			queryString: "fields @message | limit 20",
			op:          "put_then_describe",
			wantLen:     1,
		},
		{
			name: "describe_with_prefix",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutQueryDefinition(
					"prod-errors",
					"fields @message | filter @message like /ERROR/",
					"",
					nil,
					nil,
				)
				_, _ = b.PutQueryDefinition("dev-logs", "fields @message | limit 10", "", nil, nil)
			},
			op:      "describe_prefix",
			prefix:  "prod",
			wantLen: 1,
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutQueryDefinition("q1", "fields @message", "", nil, nil)
			},
			op: "delete_first",
		},
		{
			name:    "delete_missing",
			op:      "delete_direct",
			id:      "nonexistent-id",
			wantErr: cloudwatchlogs.ErrQueryDefinitionNotFound,
		},
		{
			name:    "put_missing_name",
			opName:  "",
			op:      "put_direct",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "put_then_describe":
				var id string
				id, err = b.PutQueryDefinition(tt.opName, tt.queryString, "", nil, nil)
				require.NoError(t, err)
				assert.NotEmpty(t, id)
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions("", 50, "")
				require.NoError(t, err)
				assert.Len(t, defs, tt.wantLen)

				return
			case "describe_prefix":
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions(tt.prefix, 50, "")
				require.NoError(t, err)
				assert.Len(t, defs, tt.wantLen)

				return
			case "delete_first":
				var defs []cloudwatchlogs.QueryDefinition
				defs, _, err = b.DescribeQueryDefinitions("", 50, "")
				require.NoError(t, err)
				require.Len(t, defs, 1)
				err = b.DeleteQueryDefinition(defs[0].QueryDefinitionID)
			case "delete_direct":
				err = b.DeleteQueryDefinition(tt.id)
			case "put_direct":
				_, err = b.PutQueryDefinition(tt.opName, tt.queryString, "", nil, nil)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_PutQueryDefinition_UpdateVerifiesID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		queryDefinitionID string
		createFirst       bool
	}{
		{
			name:              "create_new_no_id",
			queryDefinitionID: "",
		},
		{
			name:              "update_existing_id",
			queryDefinitionID: "placeholder",
			createFirst:       true,
		},
		{
			name:              "update_nonexistent_id",
			queryDefinitionID: "00000000-0000-0000-0000-000000000000",
			createFirst:       false,
			wantErr:           cloudwatchlogs.ErrQueryDefinitionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()

			queryID := tt.queryDefinitionID
			if tt.createFirst {
				var err error
				queryID, err = b.PutQueryDefinition("initial", "fields @message", "", nil, nil)
				require.NoError(t, err)
			}

			_, err := b.PutQueryDefinition("updated", "fields @timestamp", queryID, nil, nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
