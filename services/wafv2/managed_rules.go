package wafv2

const awsVendorName = "AWS"

const (
	timestampMidYr2024 int64 = 1717200000
	timestampNov2023   int64 = 1700000000
)

// mobileSdkReleaseInfo holds catalog metadata for a WAF mobile SDK release.
type mobileSdkReleaseInfo struct {
	Platform       string
	ReleaseVersion string
	ReleaseNotes   string
	Timestamp      int64
}

// getMobileSdkRelease returns the mobile SDK release entry for the given platform and version.
// Returns nil if the platform or version is not in the catalog.
func getMobileSdkRelease(platform, version string) *mobileSdkReleaseInfo {
	for _, r := range buildMobileSdkCatalog() {
		if r.Platform == platform && r.ReleaseVersion == version {
			cp := r
			return &cp
		}
	}

	return nil
}

// getMobileSdkReleases returns all catalog entries for the given platform.
// Returns nil if the platform is unknown.
func getMobileSdkReleases(platform string) []mobileSdkReleaseInfo {
	var result []mobileSdkReleaseInfo

	for _, r := range buildMobileSdkCatalog() {
		if r.Platform == platform {
			result = append(result, r)
		}
	}

	return result
}

func buildMobileSdkCatalog() []mobileSdkReleaseInfo {
	return []mobileSdkReleaseInfo{
		{
			Platform:       "Android",
			ReleaseVersion: "3.1.0",
			ReleaseNotes:   "WAF Mobile SDK 3.1.0 for Android — threat intelligence updates and bug fixes",
			Timestamp:      timestampMidYr2024,
		},
		{
			Platform:       "Android",
			ReleaseVersion: "3.0.0",
			ReleaseNotes:   "WAF Mobile SDK 3.0.0 for Android — initial v3 release",
			Timestamp:      timestampNov2023,
		},
		{
			Platform:       "iOS",
			ReleaseVersion: "3.1.0",
			ReleaseNotes:   "WAF Mobile SDK 3.1.0 for iOS — threat intelligence updates and bug fixes",
			Timestamp:      timestampMidYr2024,
		},
		{
			Platform:       "iOS",
			ReleaseVersion: "3.0.0",
			ReleaseNotes:   "WAF Mobile SDK 3.0.0 for iOS — initial v3 release",
			Timestamp:      timestampNov2023,
		},
	}
}

// managedRuleGroupInfo holds catalog metadata for an AWS Managed Rule Group.
type managedRuleGroupInfo struct {
	VendorName          string
	Name                string
	Description         string
	Capacity            int64
	VersioningSupported bool
}

// getManagedRuleGroups returns the static catalog of common AWS Managed Rule Groups.
func getManagedRuleGroups() []managedRuleGroupInfo {
	return []managedRuleGroupInfo{
		{
			VendorName:          awsVendorName,
			Name:                "AWSManagedRulesCommonRuleSet",
			Capacity:            700, //nolint:mnd // AWS-defined capacity value
			Description:         "Contains rules that are generally applicable to web applications.",
			VersioningSupported: true,
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesKnownBadInputsRuleSet",
			Capacity:    200, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules to block request patterns known to be invalid.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesAmazonIpReputationList",
			Capacity:    25, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules based on Amazon threat intelligence.",
		},
		{
			VendorName:          awsVendorName,
			Name:                "AWSManagedRulesBotControlRuleSet",
			Capacity:            50, //nolint:mnd // AWS-defined capacity value
			Description:         "Provides protection against automated bots.",
			VersioningSupported: true,
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesATPRuleSet",
			Capacity:    50, //nolint:mnd // AWS-defined capacity value
			Description: "Account Takeover Prevention.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesACFPRuleSet",
			Capacity:    50, //nolint:mnd // AWS-defined capacity value
			Description: "Account Creation Fraud Prevention.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesAnonymousIpList",
			Capacity:    50, //nolint:mnd // AWS-defined capacity value
			Description: "Blocks requests from IPs known for hosting anonymous proxies.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesSQLiRuleSet",
			Capacity:    200, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules to block SQL injection attacks.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesLinuxRuleSet",
			Capacity:    200, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules for Linux-specific vulnerabilities.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesUnixRuleSet",
			Capacity:    100, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules for POSIX and POSIX-like OS vulnerabilities.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesWindowsRuleSet",
			Capacity:    200, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules for Windows-specific vulnerabilities.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesPHPRuleSet",
			Capacity:    100, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules for PHP-specific vulnerabilities.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesWordPressRuleSet",
			Capacity:    100, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules for WordPress vulnerabilities.",
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesAdminProtectionRuleSet",
			Capacity:    100, //nolint:mnd // AWS-defined capacity value
			Description: "Provides protection for admin pages.",
		},
	}
}
