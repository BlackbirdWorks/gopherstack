package rds_test

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDBLogFiles_FileSizeFilterIsStrictlyGreaterThan verifies AWS's
// DescribeDBLogFilesInput.FileSize doc comment: "Filters the available log
// files for files larger than the specified size" -- strictly greater than,
// not "at least". A log file whose size exactly equals the FileSize value
// must be excluded.
func TestDescribeDBLogFiles_FileSizeFilterIsStrictlyGreaterThan(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBLogFilesResponse"`
		Result  struct {
			DescribeDBLogFiles struct {
				Members []struct {
					LogFileName string `xml:"LogFileName"`
					Size        int64  `xml:"Size"`
				} `xml:"DescribeDBLogFilesDetails"`
			} `xml:"DescribeDBLogFiles"`
		} `xml:"DescribeDBLogFilesResult"`
	}

	h := newRDSHandler()
	postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=log-size-db&Engine=postgres")

	rec := postRDSForm(t, h,
		"Action=DescribeDBLogFiles&Version=2014-10-31&DBInstanceIdentifier=log-size-db")
	require.Equal(t, http.StatusOK, rec.Code)

	var unfiltered describeResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &unfiltered))
	require.NotEmpty(t, unfiltered.Result.DescribeDBLogFiles.Members)

	boundarySize := unfiltered.Result.DescribeDBLogFiles.Members[0].Size
	require.Positive(t, boundarySize)

	rec = postRDSForm(t, h,
		"Action=DescribeDBLogFiles&Version=2014-10-31&DBInstanceIdentifier=log-size-db"+
			"&FileSize="+strconv.FormatInt(boundarySize, 10))
	require.Equal(t, http.StatusOK, rec.Code)

	var filtered describeResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &filtered))

	for _, m := range filtered.Result.DescribeDBLogFiles.Members {
		assert.NotEqualf(t, boundarySize, m.Size,
			"file %s has size %d, exactly the FileSize filter value -- FileSize is documented "+
				"as strictly greater-than, so an exact match must be excluded", m.LogFileName, m.Size)
		assert.Greater(t, m.Size, boundarySize)
	}
}
