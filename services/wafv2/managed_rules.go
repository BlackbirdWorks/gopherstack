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

// managedRuleInfo holds a single rule definition within a managed rule group.
type managedRuleInfo struct {
	// Name is the rule name as returned by DescribeManagedRuleGroup.
	Name string
	// DefaultAction is the rule's default action: "block", "count", or "allow".
	DefaultAction string
	// Labels are the label names the rule attaches to matching requests.
	Labels []string
}

// managedRuleGroupInfo holds catalog metadata for an AWS Managed Rule Group.
type managedRuleGroupInfo struct {
	VendorName          string
	Name                string
	Description         string
	Rules               []managedRuleInfo
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
			Rules:               crsRules(),
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesKnownBadInputsRuleSet",
			Capacity:    200, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules to block request patterns known to be invalid.",
			Rules:       knownBadInputsRules(),
		},
		{
			VendorName:  awsVendorName,
			Name:        "AWSManagedRulesAmazonIpReputationList",
			Capacity:    25, //nolint:mnd // AWS-defined capacity value
			Description: "Contains rules based on Amazon threat intelligence.",
			Rules:       ipReputationRules(),
		},
		{
			VendorName:          awsVendorName,
			Name:                "AWSManagedRulesBotControlRuleSet",
			Capacity:            50, //nolint:mnd // AWS-defined capacity value
			Description:         "Provides protection against automated bots.",
			VersioningSupported: true,
			Rules:               botControlRules(),
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
			Rules:       sqliRules(),
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

const (
	actionBlock = "block"
	actionCount = "count"
)

// crsRules returns the publicly documented rules for AWSManagedRulesCommonRuleSet.
func crsRules() []managedRuleInfo {
	const p = "awswaf:managed:aws:core-rule-set:"

	return []managedRuleInfo{
		{Name: "NoUserAgent_HEADER", DefaultAction: actionBlock, Labels: []string{p + "NoUserAgent"}},
		{Name: "UserAgent_BadBots_HEADER", DefaultAction: actionBlock, Labels: []string{p + "UserAgent_BadBots"}},
		{Name: "SizeRestrictions_QUERYSTRING", DefaultAction: actionBlock,
			Labels: []string{p + "SizeRestrictions_QUERYSTRING"}},
		{Name: "SizeRestrictions_Cookie_HEADER", DefaultAction: actionBlock,
			Labels: []string{p + "SizeRestrictions_Cookie_HEADER"}},
		{Name: "SizeRestrictions_BODY", DefaultAction: actionBlock, Labels: []string{p + "SizeRestrictions_BODY"}},
		{Name: "SizeRestrictions_URIPATH", DefaultAction: actionBlock,
			Labels: []string{p + "SizeRestrictions_URIPATH"}},
		{Name: "EC2MetaDataSSRF_BODY", DefaultAction: actionBlock, Labels: []string{p + "EC2MetaDataSSRF_BODY"}},
		{Name: "EC2MetaDataSSRF_COOKIE", DefaultAction: actionBlock, Labels: []string{p + "EC2MetaDataSSRF_COOKIE"}},
		{Name: "EC2MetaDataSSRF_URIPATH", DefaultAction: actionBlock, Labels: []string{p + "EC2MetaDataSSRF_URIPATH"}},
		{Name: "EC2MetaDataSSRF_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "EC2MetaDataSSRF_QUERYARGUMENTS"}},
		{Name: "GenericLFI_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "GenericLFI_QUERYARGUMENTS"}},
		{Name: "GenericLFI_URIPATH", DefaultAction: actionBlock, Labels: []string{p + "GenericLFI_URIPATH"}},
		{Name: "GenericLFI_BODY", DefaultAction: actionBlock, Labels: []string{p + "GenericLFI_BODY"}},
		{Name: "GenericRFI_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "GenericRFI_QUERYARGUMENTS"}},
		{Name: "GenericRFI_BODY", DefaultAction: actionBlock, Labels: []string{p + "GenericRFI_BODY"}},
		{Name: "GenericRFI_URIPATH", DefaultAction: actionBlock, Labels: []string{p + "GenericRFI_URIPATH"}},
		{Name: "RestrictedExtensions_URIPATH", DefaultAction: actionBlock,
			Labels: []string{p + "RestrictedExtensions_URIPATH"}},
		{Name: "RestrictedExtensions_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "RestrictedExtensions_QUERYARGUMENTS"}},
		{Name: "GenericSSRF_BODY", DefaultAction: actionBlock, Labels: []string{p + "GenericSSRF_BODY"}},
		{Name: "GenericSSRF_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "GenericSSRF_QUERYARGUMENTS"}},
		{Name: "GenericSSRF_URIPATH", DefaultAction: actionBlock, Labels: []string{p + "GenericSSRF_URIPATH"}},
		{Name: "CrossSiteScripting_COOKIE", DefaultAction: actionBlock,
			Labels: []string{p + "CrossSiteScripting_COOKIE"}},
		{Name: "CrossSiteScripting_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "CrossSiteScripting_QUERYARGUMENTS"}},
		{Name: "CrossSiteScripting_BODY", DefaultAction: actionBlock, Labels: []string{p + "CrossSiteScripting_BODY"}},
		{Name: "CrossSiteScripting_URIPATH", DefaultAction: actionBlock,
			Labels: []string{p + "CrossSiteScripting_URIPATH"}},
	}
}

// sqliRules returns the publicly documented rules for AWSManagedRulesSQLiRuleSet.
func sqliRules() []managedRuleInfo {
	const p = "awswaf:managed:aws:sql-database:"

	return []managedRuleInfo{
		{Name: "SQLiExtendedPatterns_QUERYARGUMENTS", DefaultAction: actionBlock,
			Labels: []string{p + "SQLiExtendedPatterns_QUERYARGUMENTS"}},
		{Name: "SQLiExtendedPatterns_BODY", DefaultAction: actionBlock,
			Labels: []string{p + "SQLiExtendedPatterns_BODY"}},
		{Name: "SQLiExtendedPatterns_COOKIE", DefaultAction: actionBlock,
			Labels: []string{p + "SQLiExtendedPatterns_COOKIE"}},
		{Name: "SQLiExtendedPatterns_HEADER", DefaultAction: actionBlock,
			Labels: []string{p + "SQLiExtendedPatterns_HEADER"}},
		{Name: "SQLi_QUERYARGUMENTS", DefaultAction: actionBlock, Labels: []string{p + "SQLi_QUERYARGUMENTS"}},
		{Name: "SQLi_BODY", DefaultAction: actionBlock, Labels: []string{p + "SQLi_BODY"}},
		{Name: "SQLi_COOKIE", DefaultAction: actionBlock, Labels: []string{p + "SQLi_COOKIE"}},
		{Name: "SQLi_HEADER", DefaultAction: actionBlock, Labels: []string{p + "SQLi_HEADER"}},
	}
}

// knownBadInputsRules returns the publicly documented rules for AWSManagedRulesKnownBadInputsRuleSet.
func knownBadInputsRules() []managedRuleInfo {
	const p = "awswaf:managed:aws:known-bad-inputs:"

	return []managedRuleInfo{
		{Name: "Host_localhost_HEADER", DefaultAction: actionBlock, Labels: []string{p + "Host_localhost_HEADER"}},
		{Name: "PROPFIND_METHOD", DefaultAction: actionBlock, Labels: []string{p + "PROPFIND_METHOD"}},
		{Name: "ExploitablePaths_URIPATH", DefaultAction: actionBlock,
			Labels: []string{p + "ExploitablePaths_URIPATH"}},
		{Name: "Log4JRCE_QUERYARGUMENTS", DefaultAction: actionBlock, Labels: []string{p + "Log4JRCE_QUERYARGUMENTS"}},
		{Name: "Log4JRCE_BODY", DefaultAction: actionBlock, Labels: []string{p + "Log4JRCE_BODY"}},
		{Name: "Log4JRCE_URIPATH", DefaultAction: actionBlock, Labels: []string{p + "Log4JRCE_URIPATH"}},
		{Name: "Log4JRCE_HEADER", DefaultAction: actionBlock, Labels: []string{p + "Log4JRCE_HEADER"}},
		{Name: "JavaDeserializationRCE_HEADER", DefaultAction: actionBlock,
			Labels: []string{p + "JavaDeserializationRCE_HEADER"}},
		{Name: "JavaDeserializationRCE_BODY", DefaultAction: actionBlock,
			Labels: []string{p + "JavaDeserializationRCE_BODY"}},
	}
}

// ipReputationRules returns the publicly documented rules for AWSManagedRulesAmazonIpReputationList.
func ipReputationRules() []managedRuleInfo {
	const p = "awswaf:managed:aws:amazon-ip-list:"

	return []managedRuleInfo{
		{Name: "AWSManagedIPReputationList", DefaultAction: actionBlock,
			Labels: []string{p + "AWSManagedIPReputationList"}},
		{Name: "AWSManagedReconnaissanceList", DefaultAction: actionBlock,
			Labels: []string{p + "AWSManagedReconnaissanceList"}},
		{Name: "AWSManagedIPDDoSList", DefaultAction: actionBlock, Labels: []string{p + "AWSManagedIPDDoSList"}},
	}
}

// botControlRules returns the publicly documented rules for AWSManagedRulesBotControlRuleSet.
func botControlRules() []managedRuleInfo {
	const p = "awswaf:managed:aws:bot-control:"
	const verified = p + "bot:verified"

	return []managedRuleInfo{
		{Name: "CategoryAdvertising", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:advertising", verified}},
		{Name: "CategoryArchiver", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:archiver", verified}},
		{Name: "CategoryContentFetcher", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:content_fetcher", verified}},
		{Name: "CategoryHttpLibrary", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:http_library"}},
		{Name: "CategoryLinkChecker", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:link_checker", verified}},
		{Name: "CategoryMonitoring", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:monitoring", verified}},
		{Name: "CategoryScrapingFramework", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:scraping_framework"}},
		{Name: "CategorySearchEngine", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:search_engine", verified}},
		{Name: "CategorySeo", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:seo", verified}},
		{Name: "CategorySocialMedia", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:social_media", verified}},
		{Name: "CategoryTestingTool", DefaultAction: actionCount,
			Labels: []string{p + "bot:category:testing_tool"}},
		{Name: "SignalAutomatedBrowser", DefaultAction: actionBlock,
			Labels: []string{p + "signal:automated_browser"}},
		{Name: "SignalKnownBotDataCenter", DefaultAction: actionBlock,
			Labels: []string{p + "signal:known_bot_data_center"}},
		{Name: "SignalNonBrowserUserAgent", DefaultAction: actionBlock,
			Labels: []string{p + "signal:non_browser_user_agent"}},
	}
}
