package timestreamwrite_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	twsdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteRecordsFutureTimestamp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		offset  time.Duration
		wantErr bool
	}{
		{offset: 0, name: "now"},
		{offset: 10 * time.Minute, name: "ten_minutes_ahead"},
		{offset: time.Hour, name: "hour_ahead", wantErr: true},
		{offset: 24 * 365 * time.Hour, name: "year_ahead", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &twsdk.CreateDatabaseInput{DatabaseName: aws.String("fdb")})
			require.NoError(t, err)
			_, err = client.CreateTable(ctx, &twsdk.CreateTableInput{
				DatabaseName: aws.String("fdb"), TableName: aws.String("ftbl"),
			})
			require.NoError(t, err)

			_, err = client.WriteRecords(ctx, &twsdk.WriteRecordsInput{
				DatabaseName: aws.String("fdb"), TableName: aws.String("ftbl"),
				Records: []twtypes.Record{{
					Dimensions:       []twtypes.Dimension{{Name: aws.String("h"), Value: aws.String("a")}},
					MeasureName:      aws.String("cpu"),
					MeasureValue:     aws.String("1"),
					MeasureValueType: twtypes.MeasureValueTypeDouble,
					Time:             aws.String(strconv.FormatInt(time.Now().Add(tc.offset).UnixMilli(), 10)),
					TimeUnit:         twtypes.TimeUnitMilliseconds,
				}},
			})

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "RejectedRecordsException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListPagingRejectsBadToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(c *twsdk.Client) error
		name string
	}{
		{func(c *twsdk.Client) error {
			_, err := c.ListDatabases(t.Context(), &twsdk.ListDatabasesInput{NextToken: aws.String("%%bad")})

			return err
		}, "databases"},
		{func(c *twsdk.Client) error {
			_, err := c.ListTables(t.Context(), &twsdk.ListTablesInput{NextToken: aws.String("%%bad")})

			return err
		}, "tables"},
		{func(c *twsdk.Client) error {
			_, err := c.ListBatchLoadTasks(t.Context(), &twsdk.ListBatchLoadTasksInput{NextToken: aws.String("%%bad")})

			return err
		}, "batch_load_tasks"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run(newTestHandlerAndClient(t))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}
