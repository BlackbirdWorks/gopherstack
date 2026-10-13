package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareQuery_SelectColumnMetadata(t *testing.T) {
	t.Parallel()

	type want struct {
		name, db, table string
		aliased         bool
	}

	tests := []struct {
		name  string
		query string
		want  []want
	}{
		{
			name:  "plain columns",
			query: `SELECT device_id, region FROM "mydb"."mytbl" WHERE region = 'x'`,
			want: []want{
				{name: "device_id", db: "mydb", table: "mytbl"},
				{name: "region", db: "mydb", table: "mytbl"},
			},
		},
		{
			name:  "alias and aggregate",
			query: "SELECT device_id AS d, COUNT(*) AS cnt FROM db1.t1 GROUP BY device_id",
			want: []want{
				{name: "d", db: "db1", table: "t1", aliased: true},
				{name: "cnt", aliased: true},
			},
		},
		{
			name:  "join leaves origin empty",
			query: "SELECT a FROM db1.t1 JOIN db1.t2 ON t1.a = t2.a",
			want:  []want{{name: "a"}},
		},
		{
			name:  "unqualified table",
			query: "SELECT a FROM tbl",
			want:  []want{{name: "a"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			out, err := client.PrepareQuery(t.Context(), &tqsdk.PrepareQueryInput{QueryString: aws.String(tt.query)})
			require.NoError(t, err)
			require.Len(t, out.Columns, len(tt.want))

			for i, w := range tt.want {
				c := out.Columns[i]
				assert.Equal(t, w.name, aws.ToString(c.Name))
				assert.Equal(t, w.db, aws.ToString(c.DatabaseName))
				assert.Equal(t, w.table, aws.ToString(c.TableName))
				require.NotNil(t, c.Aliased)
				assert.Equal(t, w.aliased, *c.Aliased)
			}
		})
	}
}
