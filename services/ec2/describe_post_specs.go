package ec2

import "net/url"

const (
	pageMaxHosts          = 500    // api_op_DescribeHosts.go: "between 5 and 500"
	pageMaxLaunchTemplate = 200    // api_op_DescribeLaunchTemplates.go: "between 1 and 200"
	pageMinTags           = 5      // api_op_DescribeTags.go: "between 5 and 1000"
	pageMaxMetricData     = 100000 // api_op_GetCapacityManagerMetricData.go: "1 to 100,000"
	pageDefaultIpamHist   = 100    // api_op_GetIpamAddressHistory.go: "Defaults to 100"
)

func specHosts() pageSpec {
	return pageSpec{min: ec2PageMinSecurityGroups, max: pageMaxHosts, idParam: "HostId"}
}

func spec5to500() pageSpec { return pageSpec{min: ec2PageMinSecurityGroups, max: pageMaxHosts} }

func specLaunchTemplates() pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: pageMaxLaunchTemplate}
}

func specTags() pageSpec { return pageSpec{min: pageMinTags, max: ec2PageMaxDefault} }

// specClamp5 is the 5..1000 range where larger values return 1000 results.
func specClamp5() pageSpec {
	return pageSpec{min: ec2PageMinSecurityGroups, max: ec2PageMaxDefault, clamp: true}
}

// specClamp is the shared range where values above 1000 return 1000 results.
func specClamp() pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: ec2PageMaxDefault, clamp: true}
}

func specClampParallel() pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: ec2PageMaxDefault, clamp: true, parallel: true}
}

func specWithIDs(idParam string) pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: ec2PageMaxDefault, idParam: idParam}
}

func specMetricData() pageSpec { return pageSpec{min: ec2PageMinDefault, max: pageMaxMetricData} }

func specIpamHistory() pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: ec2PageMaxDefault, defaultTo: pageDefaultIpamHist}
}

func specScheduledInstances() pageSpec {
	return pageSpec{
		min: ec2PageMinScheduledInstances, max: ec2PageMaxScheduledInstances,
		defaultTo: ec2PageDefaultScheduledInstances,
	}
}

func specElasticGpus() pageSpec { return pageSpec{min: ec2PageMinElasticGpus, max: ec2PageMaxDefault} }

// specTokenOnly is for ops whose input has NextToken but no MaxResults.
func specTokenOnly() pageSpec { return pageSpec{noMaxArg: true} }

func boundsSpotPriceHistory(vals url.Values) []timeBound {
	return timeBoundsOf(timeBoundSpec{vals.Get("EndTime"), "timestamp", false})
}

func boundsScheduledInstances(vals url.Values) []timeBound {
	return timeBoundsOf(
		timeBoundSpec{vals.Get("SlotStartTimeRange.EarliestTime"), "nextSlotStartTime", true},
		timeBoundSpec{vals.Get("SlotStartTimeRange.LatestTime"), "nextSlotStartTime", false},
	)
}

func boundsCapacityBlockOfferings(vals url.Values) []timeBound {
	return timeBoundsOf(
		timeBoundSpec{vals.Get("StartDateRange"), "startDate", true},
		timeBoundSpec{vals.Get("EndDateRange"), "endDate", false},
	)
}
