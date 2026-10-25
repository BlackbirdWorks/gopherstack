package azuremonitor

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

const (
	nsOperationalInsights = "Microsoft.OperationalInsights"
	nsInsights            = "Microsoft.Insights"
	typeWorkspaces        = "workspaces"
	typeTables            = "tables"
	typeDCE               = "dataCollectionEndpoints"
	typeDCR               = "dataCollectionRules"
	defaultLocation       = "local"
	fieldProperties       = "properties"
	fieldProvisioning     = "provisioningState"
	provisioningSucceeded = "Succeeded"
	fieldName             = "name"
	fieldType             = "type"
	colType               = "Type"
	colResourceID         = "_ResourceId"
	armBoolean            = "boolean"
	fieldEndpoint         = "endpoint"
	planAnalytics         = "Analytics"
	colTenantID           = "TenantId"
	keyBytes              = 64
	immutableIDLen        = 32
	hoursPerDay           = 24
)

// Column is one table or stream column; Type is a KQL type name.
type Column struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
}

// Table is a workspace table with its rows. Cells use engine types (see kql.Rel).
type Table struct {
	Created     time.Time `json:"created"`
	Name        string    `json:"name"`
	Plan        string    `json:"plan"`
	Description string    `json:"description,omitempty"`
	Columns     []Column  `json:"columns"`
	Rows        [][]any   `json:"-"`
	Retention   int       `json:"retention"`
	TotalRet    int       `json:"totalRetention"`
	Custom      bool      `json:"custom"`
}

// Workspace is a Log Analytics workspace.
type Workspace struct {
	Created    time.Time         `json:"created"`
	Modified   time.Time         `json:"modified"`
	SKUUpdated time.Time         `json:"skuUpdated"`
	Tags       map[string]string `json:"tags"`
	Props      map[string]any    `json:"props"`
	Tables     map[string]*Table `json:"-"`
	Sub        string            `json:"sub"`
	RG         string            `json:"rg"`
	Location   string            `json:"location"`
	CustomerID string            `json:"customerId"`
	SKU        string            `json:"sku"`
	Primary    string            `json:"primaryKey"`
	Secondary  string            `json:"secondaryKey"`
	Name       string            `json:"name"`
	Retention  int               `json:"retention"`
}

// DCE is a data collection endpoint.
type DCE struct {
	Created     time.Time         `json:"created"`
	Tags        map[string]string `json:"tags"`
	Props       map[string]any    `json:"props"`
	Name        string            `json:"name"`
	Sub         string            `json:"sub"`
	RG          string            `json:"rg"`
	Location    string            `json:"location"`
	Kind        string            `json:"kind"`
	ImmutableID string            `json:"immutableId"`
}

// DCR is a data collection rule.
type DCR struct {
	Created     time.Time         `json:"created"`
	Tags        map[string]string `json:"tags"`
	Props       map[string]any    `json:"props"`
	Identity    map[string]any    `json:"identity,omitempty"`
	Name        string            `json:"name"`
	Sub         string            `json:"sub"`
	RG          string            `json:"rg"`
	Location    string            `json:"location"`
	Kind        string            `json:"kind"`
	ImmutableID string            `json:"immutableId"`
}

func (w *Workspace) armID() string {
	return armID(w.Sub, w.RG, nsOperationalInsights, typeWorkspaces, w.Name)
}
func (d *DCE) armID() string { return armID(d.Sub, d.RG, nsInsights, typeDCE, d.Name) }
func (d *DCR) armID() string { return armID(d.Sub, d.RG, nsInsights, typeDCR, d.Name) }

func armID(sub, rg, ns string, typ, name string) string {
	return "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/" + ns + "/" + typ + "/" + name
}

func key(parts ...string) string { return strings.ToLower(strings.Join(parts, "/")) }

type typeInfo struct {
	name string
	kql  kql.Type
}

//nolint:gochecknoglobals // static lookup
var armToKQL = map[string]typeInfo{
	string(kql.TString): {string(kql.TString), kql.TString},
	"int":               {"int", kql.TInt},
	"long":              {"long", kql.TLong},
	string(kql.TReal):   {string(kql.TReal), kql.TReal},
	"double":            {string(kql.TReal), kql.TReal},
	armBoolean:          {armBoolean, kql.TBool},
	string(kql.TBool): {
		armBoolean,
		kql.TBool,
	},
	"datetime":        {"dateTime", kql.TDatetime},
	string(kql.TGUID): {string(kql.TGUID), kql.TGUID},
	"dynamic":         {"dynamic", kql.TDynamic},
}

// kqlTypeOf maps an ARM/DCR column type name to a KQL type.
func kqlTypeOf(armType string) (kql.Type, bool) {
	t, ok := armToKQL[strings.ToLower(armType)]

	return t.kql, ok
}

func armTypeOf(t kql.Type) string {
	switch t { //nolint:exhaustive // other types render as their KQL name
	case kql.TBool:
		return armBoolean
	case kql.TDatetime:
		return "dateTime"
	}

	return string(t)
}
