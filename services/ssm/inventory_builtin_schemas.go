package ssm

import "strings"

// builtinInventorySchemas lists the AWS-provided inventory types with the attributes
// documented in the Systems Manager user guide, "Metadata collected by Inventory".
func builtinInventorySchemas() []InventorySchemaItem {
	return []InventorySchemaItem{
		{TypeName: "AWS:Application", Version: inventorySchemaV11, Attributes: inventoryAttrs(
			"Name", "ApplicationType", "Publisher", "Version", "Release", "Epoch", "InstalledTime",
			"Architecture", "URL", "Summary", "PackageId",
		)},
		{TypeName: "AWS:AWSComponent", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "ApplicationType", "Publisher", "Version", "InstalledTime", "Architecture",
			"URL",
		)},
		{TypeName: "AWS:BillingInfo", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"BillingProductId",
		)},
		{TypeName: "AWS:ComplianceItem", Version: inventorySchemaV11, Attributes: inventoryAttrs(
			"ComplianceType", "ExecutionId", "ExecutionType", "ExecutionTime", "Id",
			"Title", "Status", "Severity", "DocumentName", "DocumentVersion", "Classification",
			"PatchBaselineId", "PatchSeverity", "PatchState", "PatchGroup", "InstalledTime",
			"InstallOverrideList", "DetailedText", "DetailedLink", "CVEIds",
		)},
		{TypeName: "AWS:ComplianceSummary", Version: inventorySchemaV11, Attributes: inventoryAttrs(
			"ComplianceType", "PatchGroup", "PatchBaselineId", "Status", "OverallSeverity",
			"ExecutionId", "ExecutionType", "ExecutionTime", "#CompliantCriticalCount",
			"#CompliantHighCount", "#CompliantMediumCount", "#CompliantLowCount", "#CompliantInformationalCount",
			"#CompliantUnspecifiedCount", "#NonCompliantCriticalCount", "#NonCompliantHighCount",
			"#NonCompliantMediumCount", "#NonCompliantLowCount", "#NonCompliantInformationalCount",
			"#NonCompliantUnspecifiedCount",
		)},
		{TypeName: "AWS:File", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "Size", "Description", "FileVersion", "InstalledDate", "ModificationTime",
			"LastAccessTime", "ProductName", "InstalledDir", "ProductLanguage", "CompanyName",
			"ProductVersion",
		)},
		{TypeName: "AWS:InstanceDetailedInformation", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"CPUModel", "#CPUCores", "#CPUs", "#CPUSpeedMHz", "#CPUSockets", "CPUHyperThreadEnabled",
			"OSServicePack",
		)},
		{TypeName: "AWS:InstanceInformation", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"AgentType", "AgentVersion", "ComputerName", "InstanceId", "IpAddress", "PlatformName",
			"PlatformType", "PlatformVersion", "ResourceType", "AgentStatus", "InstanceStatus",
		)},
		{TypeName: "AWS:Network", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "SubnetMask", "Gateway", "DHCPServer", "DNSServer", "MacAddress",
			"IPV4", "IPV6",
		)},
		{TypeName: "AWS:PatchCompliance", Version: inventorySchemaV11, Attributes: inventoryAttrs(
			"Title", "KBId", "Classification", "Severity", "State", "InstalledTime",
		)},
		{TypeName: "AWS:PatchSummary", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"PatchGroup", "BaselineId", "SnapshotId", "OwnerInformation", "#InstalledCount",
			"#InstalledPendingRebootCount", "#InstalledOtherCount", "#InstalledRejectedCount",
			"#NotApplicableCount", "#UnreportedNotApplicableCount", "#MissingCount",
			"#FailedCount", "OperationType", "OperationStartTime", "OperationEndTime",
			"InstallOverrideList", "RebootOption", "LastNoRebootInstallOperationTime",
			"ExecutionId", "NonCompliantSeverity", "#SecurityNonCompliantCount", "#CriticalNonCompliantCount",
			"#OtherNonCompliantCount",
		)},
		{TypeName: "AWS:ResourceGroup", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "Arn",
		)},
		{TypeName: "AWS:Service", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "DisplayName", "ServiceType", "Status", "DependentServices", "ServicesDependedOn",
			"StartType",
		)},
		{TypeName: "AWS:Tag", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Key", "Value",
		)},
		{TypeName: "AWS:WindowsRegistry", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"KeyPath", "ValueName", "ValueType", "Value",
		)},
		{TypeName: "AWS:WindowsRole", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"Name", "DisplayName", "Path", "FeatureType", "DependsOn", "Description",
			"Installed", "InstalledState", "SubFeatures", "ServerComponentDescriptor",
			"Parent",
		)},
		{TypeName: "AWS:WindowsUpdate", Version: inventorySchemaV10, Attributes: inventoryAttrs(
			"HotFixId", "Description", "InstalledTime", "InstalledBy",
		)},
	}
}

// inventoryAttrs builds the attribute list; a leading "#" marks a number attribute.
func inventoryAttrs(fields ...string) []InventoryItemAttribute {
	attrs := make([]InventoryItemAttribute, 0, len(fields))

	for _, f := range fields {
		if name, isNumber := strings.CutPrefix(f, "#"); isNumber {
			attrs = append(attrs, InventoryItemAttribute{Name: name, DataType: attrDataTypeNumber})

			continue
		}

		attrs = append(attrs, InventoryItemAttribute{Name: f, DataType: attrDataTypeString})
	}

	return attrs
}
