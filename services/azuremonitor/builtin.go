package azuremonitor

import "github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"

const pairWidth = 2

func cols(pairs ...string) []Column {
	out := make([]Column, 0, len(pairs)/pairWidth)
	for i := 0; i+1 < len(pairs); i += pairWidth {
		out = append(out, Column{Name: pairs[i], Type: pairs[i+1]})
	}

	return out
}

const (
	tString = string(kql.TString)
	tDT     = string(kql.TDatetime)
	tReal   = string(kql.TReal)
	tLong   = string(kql.TLong)
	tDyn    = string(kql.TDynamic)
	tInt    = string(kql.TInt)
	tBool   = string(kql.TBool)
)

// builtinSchemas lists the Microsoft-owned tables every workspace starts with.
func builtinSchemas() map[string][]Column {
	return map[string][]Column{
		"Heartbeat": cols("TimeGenerated", tDT, "Computer", tString, "Category", tString, "OSType", tString,
			"OSName", tString, "Version", tString, "ComputerIP", tString, "ResourceGroup", tString,
			"SubscriptionId", tString, "SourceComputerId", tString, "SourceSystem", tString),
		"Syslog": cols("TimeGenerated", tDT, "Computer", tString, "Facility", tString, "HostName", tString,
			"HostIP", tString, "SeverityLevel", tString, "SyslogMessage", tString, "ProcessID", tInt,
			"ProcessName", tString, "SourceSystem", tString),
		"Event": cols("TimeGenerated", tDT, "Computer", tString, "EventLog", tString, "EventLevelName", tString,
			"EventID", tInt, "Source", tString, "RenderedDescription", tString, "UserName", tString,
			"SourceSystem", tString),
		"Perf": cols("TimeGenerated", tDT, "Computer", tString, "ObjectName", tString, "CounterName", tString,
			"InstanceName", tString, "CounterValue", tReal, "SourceSystem", tString),
		"AzureActivity": cols("TimeGenerated", tDT, "OperationName", tString, "OperationNameValue", tString,
			"Level", tString, "ActivityStatus", tString, "Caller", tString, "CategoryValue", tString,
			"CorrelationId", tString, "ResourceGroup", tString, "SubscriptionId", tString, "Properties", tDyn),
		"AzureDiagnostics": cols("TimeGenerated", tDT, "ResourceId", tString, "Category", tString,
			"OperationName", tString, "ResultType", tString, "ResultDescription", tString,
			"ResourceProvider", tString, "ResourceType", tString, "SubscriptionId", tString,
			"ResourceGroup", tString, "DurationMs", tLong, "Message", tString),
		"Usage": cols("TimeGenerated", tDT, "DataType", tString, "Solution", tString, "Quantity", tReal,
			"QuantityUnit", tString, "IsBillable", tBool),
	}
}
