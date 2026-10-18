package billingexport_test

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
)

func sample() *billingexport.Table {
	return &billingexport.Table{
		Columns: []billingexport.Column{
			{Name: "a", Type: billingexport.TypeString},
			{Name: "b", Type: billingexport.TypeDouble},
		},
		Rows: [][]any{{"x", 1.5}, {"y", nil}},
	}
}

func TestEncode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		format string
		comp   string
		ext    string
	}{
		{"plain", billingexport.FormatCSV, billingexport.CompressNone, ".csv"},
		{"gzip", billingexport.FormatCSV, billingexport.CompressGzip, ".csv.gz"},
		{"zip", billingexport.FormatCSV, billingexport.CompressZip, ".csv.zip"},
		{"parquet", billingexport.FormatParquet, billingexport.CompressNone, ".snappy.parquet"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body, ext, err := billingexport.Encode(sample(), tc.format, tc.comp, "r")
			require.NoError(t, err)
			assert.Equal(t, tc.ext, ext)

			var text string

			switch tc.name {
			case "gzip":
				zr, zerr := gzip.NewReader(bytes.NewReader(body))
				require.NoError(t, zerr)

				b, _ := io.ReadAll(zr)
				text = string(b)
			case "zip":
				zr, zerr := zip.NewReader(bytes.NewReader(body), int64(len(body)))
				require.NoError(t, zerr)

				f, _ := zr.File[0].Open()
				b, _ := io.ReadAll(f)
				text = string(b)
			case "parquet":
				assert.True(t, bytes.HasPrefix(body, []byte("PAR1")))

				return
			default:
				text = string(body)
			}

			assert.Equal(t, "a,b\nx,1.5\ny,\n", text)
		})
	}
}

func TestNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "line_item_usage_account_id", billingexport.SnakeCase("lineItem/UsageAccountId"))
	assert.Equal(t, "product_region", billingexport.SnakeCase("product/region"))
	assert.Equal(t, "AmazonS3", billingexport.ProductCode("Amazon Simple Storage Service"))
	assert.Equal(t, "AmazonFooBar", billingexport.ProductCode("Amazon Foo-Bar"))
}
