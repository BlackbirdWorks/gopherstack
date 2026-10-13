package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	rdsVersion = "2014-10-31"
	rdsXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
	formTrue   = "true"

	rdsDescribeDefaultPageSize = 100

	// AWS bounds for AllocatedStorage (GiB) on general-purpose RDS engines.
	minAllocatedStorage = 20
	maxAllocatedStorage = 65536

	// AWS bounds for BackupRetentionPeriod on DB clusters (1–35 days; 0 disables backups for instances).
	minClusterBackupRetention = 1
	maxClusterBackupRetention = 35

	monitoringInterval5  = 5
	monitoringInterval10 = 10
	monitoringInterval15 = 15
	monitoringInterval30 = 30
	monitoringInterval60 = 60
)

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

func parseMultiValueParam(vals url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}

func parseDescribePagination(vals url.Values) (string, int, error) {
	marker := vals.Get("Marker")
	if err := page.ValidateToken(marker); err != nil {
		return "", 0, fmt.Errorf("%w: invalid Marker %q", ErrInvalidParameter, marker)
	}

	maxRecords := 0
	rawMaxRecords := vals.Get("MaxRecords")
	if rawMaxRecords == "" {
		return marker, maxRecords, nil
	}

	maxRecords, err := strconv.Atoi(rawMaxRecords)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid MaxRecords %q", ErrInvalidParameter, rawMaxRecords)
	}

	lo, hi := maxRecordsBounds(vals.Get("Action"))
	if maxRecords < lo || (hi > 0 && maxRecords > hi) {
		return "", 0, fmt.Errorf("%w: MaxRecords must be between %d and %d", ErrInvalidParameter, lo, hi)
	}

	return marker, maxRecords, nil
}

// maxRecordsBounds returns the documented MaxRecords range for a Describe action
// (rds@v1.124.1 api_op_Describe*.go "Constraints"); hi 0 means no documented maximum.
func maxRecordsBounds(action string) (int, int) {
	switch action {
	case actionDescribeOrderableDBInstanceOptions:
		return minDescribeMaxRecords, maxOrderableMaxRecords
	case actionDescribeServerlessV2PlatformVersions:
		return 1, maxServerlessMaxRecords
	case actionDescribeAccountAttributes, actionDescribeDBClusterAutomatedBackups,
		actionDescribeDBClusterSnapshotAttributes, actionDescribeDBInstanceAutomatedBackups,
		actionDescribeDBLogFiles, actionDescribeDBMajorEngineVersions, actionDescribeDBRecommendations,
		actionDescribeDBSnapshotAttributes, actionDescribeDBSnapshotTenantDatabases,
		actionDescribeEventCategories, actionDescribeTenantDatabases,
		actionDescribeValidDBInstanceModifications:
		return 1, 0
	default:
		return minDescribeMaxRecords, maxDescribeMaxRecords
	}
}

const (
	minDescribeMaxRecords   = 20
	maxDescribeMaxRecords   = 100
	maxOrderableMaxRecords  = 1000
	maxServerlessMaxRecords = 200

	actionDescribeOrderableDBInstanceOptions   = "DescribeOrderableDBInstanceOptions"
	actionDescribeServerlessV2PlatformVersions = "DescribeServerlessV2PlatformVersions"
	actionDescribeAccountAttributes            = "DescribeAccountAttributes"
	actionDescribeDBClusterAutomatedBackups    = "DescribeDBClusterAutomatedBackups"
	actionDescribeDBClusterSnapshotAttributes  = "DescribeDBClusterSnapshotAttributes"
	actionDescribeDBInstanceAutomatedBackups   = "DescribeDBInstanceAutomatedBackups"
	actionDescribeDBLogFiles                   = "DescribeDBLogFiles"
	actionDescribeDBMajorEngineVersions        = "DescribeDBMajorEngineVersions"
	actionDescribeDBRecommendations            = "DescribeDBRecommendations"
	actionDescribeDBSnapshotAttributes         = "DescribeDBSnapshotAttributes"
	actionDescribeDBSnapshotTenantDatabases    = "DescribeDBSnapshotTenantDatabases"
	actionDescribeEventCategories              = "DescribeEventCategories"
	actionDescribeTenantDatabases              = "DescribeTenantDatabases"
	actionDescribeValidDBInstanceModifications = "DescribeValidDBInstanceModifications"
)

func paginateDescribe[TData any, TXMLOutput any](
	vals url.Values,
	data []TData,
	less func(a, b TData) bool,
	convert func(TData) TXMLOutput,
) ([]TXMLOutput, string, error) {
	marker, maxRecords, err := parseDescribePagination(vals)
	if err != nil {
		return nil, "", err
	}

	sort.Slice(data, func(i, j int) bool {
		return less(data[i], data[j])
	})

	p := page.New(data, marker, maxRecords, rdsDescribeDefaultPageSize)
	members := make([]TXMLOutput, 0, len(p.Data))
	for _, item := range p.Data {
		members = append(members, convert(item))
	}

	return members, p.Next, nil
}

func extractMemberList(vals url.Values, prefix string) []string {
	result := make([]string, 0)
	for i := 1; ; i++ {
		v := vals.Get(prefix + strconv.Itoa(i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}

// extractIndexedList reads a numbered list of values from form values like
// "prefix1", "prefix2", etc.
func extractIndexedList(vals url.Values, prefix string) []string {
	result := make([]string, 0)
	for i := 1; ; i++ {
		v := vals.Get(prefix + strconv.Itoa(i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}
