package gcpcompute

import (
	"fmt"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	crs422 = "-v422"
	crs33  = "-v33"
)

type wafFamily struct {
	name     string
	variants []string
}

// wafFamilies are the preconfigured WAF rule sets of docs.cloud.google.com/armor/docs/waf-rules;
// variants are the CRS suffixes ("" is CRS 3.0).
var wafFamilies = []wafFamily{ //nolint:gochecknoglobals // read-only catalogue
	{"sqli", []string{crs422, crs33, ""}},
	{"xss", []string{crs422, crs33, ""}},
	{"lfi", []string{crs422, crs33, ""}},
	{"rfi", []string{crs422, crs33, ""}},
	{"rce", []string{crs422, crs33, ""}},
	{"methodenforcement", []string{crs422, crs33, ""}},
	{"scannerdetection", []string{crs422, crs33, ""}},
	{"protocolattack", []string{crs422, crs33, ""}},
	{"php", []string{crs422, crs33, ""}},
	{"sessionfixation", []string{crs422, crs33, ""}},
	{"java", []string{crs422, crs33}},
	{"nodejs", []string{crs33}},
	{"generic", []string{crs422}},
}

func preconfiguredExpressionSets() *computeapi.SecurityPoliciesListPreconfiguredExpressionSetsResponse {
	var sets []*computeapi.WafExpressionSet

	for _, f := range wafFamilies {
		for _, v := range f.variants {
			for _, ch := range []string{"stable", "canary"} {
				sets = append(sets, &computeapi.WafExpressionSet{Id: fmt.Sprintf("%s%s-%s", f.name, v, ch)})
			}
		}
	}

	sets = append(sets,
		&computeapi.WafExpressionSet{Id: "cve-canary"},
		&computeapi.WafExpressionSet{Id: "json-sqli-canary"},
	)

	return &computeapi.SecurityPoliciesListPreconfiguredExpressionSetsResponse{
		PreconfiguredExpressionSets: &computeapi.SecurityPoliciesWafConfig{
			WafRules: &computeapi.PreconfiguredWafSet{ExpressionSets: sets},
		},
	}
}
