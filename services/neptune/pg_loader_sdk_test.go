package neptune_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata"
	"github.com/aws/aws-sdk-go-v2/service/neptunedata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNeptunedataSDKPropertyGraphLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   any
		files  map[string]string
		edge   *bool
		name   string
		format types.Format
		query  string
	}{
		{
			name: "gremlin csv", format: types.FormatCsv,
			files: map[string]string{
				"d/v.csv": "~id,~label,name:String,age:Int\nv1,person,marko,29\nv2,software,lop,\n",
				"d/e.csv": "~id,~from,~to,~label,weight:Double\ne1,v1,v2,created,0.4\n",
			},
			query: "MATCH (a:person)-[r:created]->(b) RETURN a.name AS a, a.age AS age, r.weight AS w, b.name AS b",
			want:  map[string]any{"a": "marko", "age": float64(29), "w": 0.4, "b": "lop"},
		},
		{
			name: "opencypher csv", format: types.FormatOpencypher, edge: aws.Bool(true),
			files: map[string]string{
				"d/n.csv": ":ID,name:String,:LABEL\nn1,ann,person\nn2,bob,person\n",
				"d/r.csv": ":ID,:START_ID,:END_ID,:TYPE,since:Int\nr1,n1,n2,KNOWS,2020\n",
			},
			query: "MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS a, r.since AS since, b.name AS b",
			want:  map[string]any{"a": "ann", "since": float64(2020), "b": "bob"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLoaderCluster(t, tt.files)
			cl := neptunedata.New(neptunedata.Options{
				Region: "us-east-1", BaseEndpoint: aws.String("http://" + clusterAddr(c)),
				Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
			})

			start, err := cl.StartLoaderJob(t.Context(), &neptunedata.StartLoaderJobInput{
				Source: aws.String("s3://lake/d/"), Format: tt.format,
				IamRoleArn:          aws.String("arn:aws:iam::000000000000:role/r"),
				S3BucketRegion:      types.S3BucketRegionUsEast1,
				UserProvidedEdgeIds: tt.edge,
			})
			require.NoError(t, err)

			id := start.Payload["loadId"]
			require.NotEmpty(t, id)

			require.Eventually(t, func() bool {
				st, gerr := cl.GetLoaderJobStatus(
					t.Context(),
					&neptunedata.GetLoaderJobStatusInput{LoadId: aws.String(id)},
				)
				if gerr != nil {
					return false
				}

				var doc map[string]any
				if st.Payload == nil || st.Payload.UnmarshalSmithyDocument(&doc) != nil {
					return false
				}

				overall, _ := doc["overallStatus"].(map[string]any)

				return overall["status"] == "LOAD_COMPLETED"
			}, 10*time.Second, 10*time.Millisecond)

			q, err := cl.ExecuteOpenCypherQuery(t.Context(), &neptunedata.ExecuteOpenCypherQueryInput{
				OpenCypherQuery: aws.String(tt.query),
			})
			require.NoError(t, err)

			raw, err := q.Results.MarshalSmithyDocument()
			require.NoError(t, err)
			assert.JSONEq(t, mustJSON(t, []any{tt.want}), string(raw))

			list, err := cl.ListLoaderJobs(t.Context(), &neptunedata.ListLoaderJobsInput{})
			require.NoError(t, err)
			assert.Contains(t, list.Payload.LoadIds, id)
		})
	}
}
