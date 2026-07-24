package elasticbeanstalk

// configOptionCatalogEntry describes one static configuration option entry in
// configurationOptionsCatalog below. It mirrors the fields AWS documents on
// types.ConfigurationOptionDescription (aws-sdk-go-v2/service/elasticbeanstalk/types)
// that this backend can express statically (Namespace, Name, DefaultValue,
// ChangeSeverity, ValueType, ValueOptions, MaxLength, MinValue, MaxValue).
// Regex is intentionally not modeled -- none of the catalog entries below
// document one.
type configOptionCatalogEntry struct {
	MaxLength      *int32
	MinValue       *int32
	MaxValue       *int32
	Namespace      string
	Name           string
	DefaultValue   string
	ChangeSeverity string
	ValueType      string
	ValueOptions   []string
}

// changeSeverity* mirror the ChangeSeverity values AWS documents on
// ConfigurationOptionDescription (see types.go in the SDK module).
//
// optDefaultFalse/optDefaultTrue and optNameSecurityGroups are pulled out as
// constants purely to dedupe the repeated literals below (goconst); they
// carry no independent meaning beyond the strings they hold.
const (
	changeSeverityNone                     = "NoInterruption"
	changeSeverityRestartEnvironment       = "RestartEnvironment"
	changeSeverityRestartApplicationServer = "RestartApplicationServer"

	optDefaultFalse       = "false"
	optDefaultTrue        = "true"
	optNameSecurityGroups = "SecurityGroups"

	// rootVolumeMinSizeGB is the documented minimum for
	// aws:autoscaling:launchconfiguration's RootVolumeSize option.
	rootVolumeMinSizeGB = 8
	// rdsMinAllocatedStorageGB is the documented minimum for
	// aws:rds:dbinstance's DBAllocatedStorage option.
	rdsMinAllocatedStorageGB = 5
)

// configurationOptionsCatalog is a curated, representative catalog of Elastic
// Beanstalk configuration options across the namespaces this backend
// otherwise already recognizes (see knownNamespaces in
// handler_configuration_templates.go). Real AWS's DescribeConfigurationOptions
// returns hundreds of options that vary per solution stack/platform version;
// this backend applies the same fixed catalog regardless of the requested
// platform, which is a known, documented limitation (see PARITY.md) -- but
// every entry here reflects a real, currently-documented Elastic Beanstalk
// option, namespace, default value and value type, which is a substantial
// improvement over returning an unfiltered, request-blind 3-option stub.
//
//nolint:gochecknoglobals // static reference data, mirrors the knownNamespaces convention in this package
var configurationOptionsCatalog = []configOptionCatalogEntry{
	// aws:autoscaling:asg
	{
		Namespace: nsAutoScalingASG, Name: "MinSize", DefaultValue: "1",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar, MinValue: new(int32(0)),
	},
	{
		Namespace: nsAutoScalingASG, Name: "MaxSize", DefaultValue: "4",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar, MinValue: new(int32(0)),
	},
	{
		Namespace: nsAutoScalingASG, Name: "Availability Zones", DefaultValue: "Any",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	// aws:autoscaling:launchconfiguration
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "IamInstanceProfile",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "InstanceType", DefaultValue: "t3.micro",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "EC2KeyName",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "ImageId",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "RootVolumeType", DefaultValue: "gp3",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"gp2", "gp3", "io1", "standard"},
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "RootVolumeSize",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		MinValue: new(int32(rootVolumeMinSizeGB)),
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: optNameSecurityGroups,
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingLaunchConfig, Name: "DisableIMDSv1", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeBoolean,
	},
	// aws:autoscaling:trigger
	{
		Namespace: nsAutoScalingTrigger, Name: "MeasureName", DefaultValue: "CPUUtilization",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingTrigger, Name: "Statistic", DefaultValue: "Average",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"Minimum", "Maximum", "Sum", "Average"},
	},
	{
		Namespace: nsAutoScalingTrigger, Name: "Unit", DefaultValue: "Percent",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingTrigger, Name: "LowerThreshold", DefaultValue: "20",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsAutoScalingTrigger, Name: "UpperThreshold", DefaultValue: "80",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:ec2:vpc
	{
		Namespace: nsEC2VPC, Name: "VPCId",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsEC2VPC, Name: "Subnets",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeList,
	},
	{
		Namespace: nsEC2VPC, Name: "ELBSubnets",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeList,
	},
	{
		Namespace: nsEC2VPC, Name: "AssociatePublicIpAddress", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsEC2VPC, Name: "ELBScheme", DefaultValue: "public",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"public", "internal"},
	},
	// aws:elasticbeanstalk:application
	{
		Namespace: nsEBApplication, Name: "Application Healthcheck URL", DefaultValue: "/",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:elasticbeanstalk:cloudwatch:logs
	{
		Namespace: nsEBCloudWatchLogs, Name: "StreamLogs", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsEBCloudWatchLogs, Name: "DeleteOnTerminate", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsEBCloudWatchLogs, Name: "RetentionInDays", DefaultValue: "7",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:elasticbeanstalk:environment
	{
		Namespace: nsEBEnvironment, Name: "EnvironmentType", DefaultValue: "LoadBalanced",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"LoadBalanced", "SingleInstance"},
	},
	{
		Namespace: nsEBEnvironment, Name: "ServiceRole",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsEBEnvironment, Name: "LoadBalancerType", DefaultValue: "application",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"classic", "application", "network"},
	},
	// aws:elasticbeanstalk:environment:proxy
	{
		Namespace: nsEBEnvironmentProxy, Name: "ProxyServer", DefaultValue: "nginx",
		ChangeSeverity: changeSeverityRestartApplicationServer, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"nginx", "none"},
	},
	// aws:elasticbeanstalk:healthreporting:system
	{
		Namespace: nsEBHealthReportingSystem, Name: "SystemType", DefaultValue: "enhanced",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"basic", "enhanced"},
	},
	// aws:elasticbeanstalk:managedactions
	{
		Namespace: nsEBManagedActions, Name: "ManagedActionsEnabled", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsEBManagedActions, Name: "PreferredStartTime",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:elasticbeanstalk:sns:topics
	{
		Namespace: nsEBSNSTopics, Name: "Notification Endpoint",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsEBSNSTopics, Name: "Notification Protocol", DefaultValue: "email",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsEBSNSTopics, Name: "Notification Topic ARN",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:elasticbeanstalk:xray
	{
		Namespace: nsEBXRay, Name: "XRayEnabled", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	// aws:elb:loadbalancer
	{
		Namespace: nsELBLoadBalancer, Name: "CrossZone", DefaultValue: optDefaultTrue,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsELBLoadBalancer, Name: "LoadBalancerHTTPPort", DefaultValue: "80",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsELBLoadBalancer, Name: "LoadBalancerHTTPSPort", DefaultValue: "OFF",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsELBLoadBalancer, Name: "SSLCertificateId",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsELBLoadBalancer, Name: optNameSecurityGroups,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	// aws:elbv2:loadbalancer
	{
		Namespace: nsELBv2LoadBalancer, Name: "IdleTimeout", DefaultValue: "60",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsELBv2LoadBalancer, Name: optNameSecurityGroups,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsELBv2LoadBalancer, Name: "AccessLogsS3Enabled", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeBoolean,
	},
	// aws:rds:dbinstance
	{
		Namespace: nsRDSDBInstance, Name: "DBEngine",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsRDSDBInstance, Name: "DBInstanceClass",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
	},
	{
		Namespace: nsRDSDBInstance, Name: "DBAllocatedStorage", DefaultValue: "5",
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeScalar,
		MinValue: new(int32(rdsMinAllocatedStorageGB)),
	},
	{
		Namespace: nsRDSDBInstance, Name: "MultiAZDatabase", DefaultValue: optDefaultFalse,
		ChangeSeverity: changeSeverityRestartEnvironment, ValueType: optionValueTypeBoolean,
	},
	{
		Namespace: nsRDSDBInstance, Name: "DBDeletionPolicy", DefaultValue: "Snapshot",
		ChangeSeverity: changeSeverityNone, ValueType: optionValueTypeScalar,
		ValueOptions: []string{"Delete", "Snapshot", "Retain"},
	},
}

// optionSpecKey builds the (Namespace, OptionName) composite key shared by
// filterConfigurationOptions and its callers.
func optionSpecKey(namespace, optionName string) string {
	return namespace + "\x00" + optionName
}

// filterConfigurationOptions returns the catalog entries matching filters
// (parsed from the request's Options.member.N.Namespace/OptionName
// parameters -- see DescribeConfigurationOptionsInput.Options: "If
// specified, restricts the descriptions to only the specified options.").
// An empty filter list returns the full catalog.
func filterConfigurationOptions(filters []OptionSetting) []configOptionCatalogEntry {
	if len(filters) == 0 {
		return configurationOptionsCatalog
	}

	wanted := make(map[string]bool, len(filters))
	for _, f := range filters {
		wanted[optionSpecKey(f.Namespace, f.OptionName)] = true
	}

	out := make([]configOptionCatalogEntry, 0, len(filters))

	for _, entry := range configurationOptionsCatalog {
		if wanted[optionSpecKey(entry.Namespace, entry.Name)] {
			out = append(out, entry)
		}
	}

	return out
}
