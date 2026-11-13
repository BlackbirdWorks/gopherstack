package azurearm

import (
	"fmt"
	"strings"
)

const webTierShared = "shared"

// webConnStrPrefixes maps a connection string type to the environment variable prefix App Service gives it.
//
//nolint:gochecknoglobals // read-only table
var webConnStrPrefixes = map[string]string{
	"sqlserver":       "SQLCONNSTR_",
	"mysql":           "MYSQLCONNSTR_",
	"sqlazure":        "SQLAZURECONNSTR_",
	"custom":          "CUSTOMCONNSTR_",
	"postgresql":      "POSTGRESQLCONNSTR_",
	"notificationhub": "NOTIFICATIONHUBCONNSTR_",
	"servicebus":      "SERVICEBUSCONNSTR_",
	"eventhub":        "EVENTHUBCONNSTR_",
	"docdb":           "DOCDBCONNSTR_",
	"rediscache":      "REDISCACHECONNSTR_",
}

// connStringEnv turns a site's connection strings into the prefixed environment variables its host receives.
func connStringEnv(conn map[string]any) map[string]string {
	env := make(map[string]string, len(conn))

	for name, raw := range conn {
		entry := mapOf(raw)
		value, _ := entry["value"].(string)
		typ, _ := entry["type"].(string)

		prefix, known := webConnStrPrefixes[strings.ToLower(typ)]
		if !known || name == "" || strings.ContainsAny(name, "=\x00") {
			continue
		}

		env[prefix+name] = value
	}

	return env
}

// syncConnStrings pushes the site's connection strings to its host environment.
func (p *WebProvider) syncConnStrings(s *storedWebSite) error {
	return p.dataPlane.SetConnectionStrings(s.dpName(), connStringEnv(s.Configs[webConfigConn]))
}

// alwaysOnAllowed reports whether a plan SKU supports the Always On setting: it exists on Basic, Standard, Premium
// and Isolated App Service plans only, never on Free, Shared, Consumption, Elastic Premium or Flex Consumption.
func alwaysOnAllowed(sku map[string]any) bool {
	tier, _ := sku[cosmosFieldTier].(string)
	name, _ := sku["name"].(string)
	tier, name = strings.ToLower(tier), strings.ToLower(name)

	switch tier {
	case acSkuFree, webTierShared, "dynamic", "elasticpremium", "flexconsumption":
		return false
	case "":
		return name != "" && !strings.HasPrefix(name, "ep") && name != "y1" && name != "fc1" &&
			tierFromSKUName(name) != acSkuFree && tierFromSKUName(name) != webTierShared
	}

	return true
}

// mergedWebConfig overlays siteCfg on the site's stored web configuration, refusing settings its plan cannot honour.
func (p *WebProvider) mergedWebConfig(rec, planSite *storedWebSite, siteCfg map[string]any) (map[string]any, error) {
	cfg := mergeWebConfig(rec.Configs[webConfigWeb], siteCfg)

	return cfg, p.checkAlwaysOn(planSite, cfg)
}

// checkAlwaysOn rejects alwaysOn=true on a site whose plan does not support it.
func (p *WebProvider) checkAlwaysOn(s *storedWebSite, cfg map[string]any) error {
	if on, _ := cfg["alwaysOn"].(bool); !on {
		return nil
	}

	if sku, ok := p.siteSKU(s); ok && !alwaysOnAllowed(sku) {
		return badWebRequest(
			fmt.Errorf("%w: alwaysOn is not available on this plan; use a Basic or higher App Service plan",
				ErrWebInvalidSiteConfig),
		)
	}

	return nil
}
