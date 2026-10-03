package s3_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectObjectContent_ScanRange(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	csvBucket := selectBugfixesPutObject(t, client, "d.csv", []byte("name,age\nAlice,30\nBob,25\nCarol,41\n"))
	jsonBucket := selectBugfixesPutObject(t, client, "d.json", []byte("{\"a\":\"p\"}\n{\"a\":\"q\"}\n{\"a\":\"r\"}\n"))

	csvIn := &types.InputSerialization{CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse}}
	jsonIn := &types.InputSerialization{JSON: &types.JSONInput{Type: types.JSONTypeLines}}

	tests := []struct {
		in     *types.InputSerialization
		out    *types.OutputSerialization
		scan   *types.ScanRange
		name   string
		bucket string
		key    string
		expr   string
		want   string
	}{
		{name: "csv no range", bucket: csvBucket, key: "d.csv", in: csvIn, scan: nil,
			expr: "SELECT s.name FROM s3object s", want: "Alice\nBob\nCarol\n"},
		{
			name:   "csv start only",
			bucket: csvBucket,
			key:    "d.csv",
			in:     csvIn,
			scan:   &types.ScanRange{Start: aws.Int64(18)},
			expr:   "SELECT s.name FROM s3object s",
			want:   "Bob\nCarol\n",
		},
		{name: "csv start and end", bucket: csvBucket, key: "d.csv", in: csvIn,
			scan: &types.ScanRange{Start: aws.Int64(9), End: aws.Int64(18)},
			expr: "SELECT s.name FROM s3object s", want: "Alice\nBob\n"},
		{
			name:   "csv end only",
			bucket: csvBucket,
			key:    "d.csv",
			in:     csvIn,
			scan:   &types.ScanRange{End: aws.Int64(9)},
			expr:   "SELECT s.name FROM s3object s",
			want:   "Carol\n",
		},
		{name: "json lines range", bucket: jsonBucket, key: "d.json", in: jsonIn,
			scan: &types.ScanRange{Start: aws.Int64(10), End: aws.Int64(10)},
			out:  &types.OutputSerialization{JSON: &types.JSONOutput{}},
			expr: "SELECT s.a FROM s3object s", want: "{\"a\":\"q\"}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outSer := tt.out
			if outSer == nil {
				outSer = &types.OutputSerialization{CSV: &types.CSVOutput{}}
			}

			out, err := client.SelectObjectContent(t.Context(), &sdk_s3.SelectObjectContentInput{
				Bucket:              aws.String(tt.bucket),
				Key:                 aws.String(tt.key),
				Expression:          aws.String(tt.expr),
				ExpressionType:      types.ExpressionTypeSql,
				InputSerialization:  tt.in,
				OutputSerialization: outSer,
				ScanRange:           tt.scan,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(selectBugfixesDrain(t, out)))
		})
	}
}
