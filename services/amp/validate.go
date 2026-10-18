package amp

import (
	"encoding/json"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	maxAliasLen     = 100
	maxNamespaceLen = 64
	maxTags         = 50
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxDataBytes    = 1 << 20
	maxPolicyBytes  = 10 * 1024
	maxExporters    = 1
	maxDetectorQL   = 2048
	maxIntervalSecs = 3600
	maxSubnets      = 16
	maxSecGroups    = 5
	maxRetention    = 36500
	listDefault     = 100
	listMax         = 1000
)

var (
	namespaceRe  = regexp.MustCompile(`^[0-9A-Za-z][-.0-9A-Z_a-z]*$`)
	kmsKeyRe     = regexp.MustCompile(`^arn:aws[a-z-]*:kms:[a-z0-9-]+:\d{12}:key/.+$`)
	logGroupRe   = regexp.MustCompile(`^arn:aws[a-z-]*:logs:[a-z0-9-]+:\d{12}:log-group:.+$`)
	roleRe       = regexp.MustCompile(`^arn:aws[a-z-]*:iam::\d{12}:role/.+$`)
	workspaceRe  = regexp.MustCompile(`^arn:aws[a-z-]*:aps:([a-z0-9-]+):(\d{12}):workspace/(ws-[-0-9a-f]+)$`)
	datasetRe    = regexp.MustCompile(`^arn:aws[a-z-]*:cloudwatch:[a-z0-9-]+:\d{12}:dataset/.+$`)
	eksClusterRe = regexp.MustCompile(`^arn:aws[a-z-]*:eks:([a-z0-9-]+):(\d{12}):cluster/([0-9A-Za-z][-_0-9A-Za-z]*)$`)
	domainRe     = regexp.MustCompile(`^arn:aws[a-z-]*:(es|aoss):[a-z0-9-]+:\d{12}:(domain|collection)/.+$`)
	detectorQRe  = regexp.MustCompile(
		`^(avg|count|group|max|min|quantile|stddev|stdvar|sum)\s*((by|without)\s*\([^)]*\))?\s*\(`)
)

func validateAlias(alias string) error {
	if len(alias) > maxAliasLen {
		return invalid("alias must be at most %d characters", maxAliasLen)
	}

	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return restroute.QuotaExceeded("a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		switch {
		case k == "" || len(k) > maxTagKeyLen:
			return invalid("tag keys must be between 1 and %d characters", maxTagKeyLen)
		case len(v) > maxTagValueLen:
			return invalid("tag values must be at most %d characters", maxTagValueLen)
		case strings.HasPrefix(strings.ToLower(k), "aws:"):
			return invalid("tag keys must not begin with aws:")
		}
	}

	return nil
}

func validateData(what string, data []byte) error {
	if len(data) == 0 {
		return invalid("%s data must not be empty", what)
	}

	if len(data) > maxDataBytes {
		return invalid("%s data must be at most %d bytes", what, maxDataBytes)
	}

	return nil
}

func parseYAMLMap(what string, data []byte) (map[string]any, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, cannotParse("%s is not valid YAML: %v", what, err)
	}

	if doc == nil {
		return nil, cannotParse("%s must be a YAML mapping", what)
	}

	return doc, nil
}

// balanced reports whether brackets and quotes pair up in a PromQL expression.
func balanced(expr string) bool {
	stack := make([]rune, 0, len(expr))

	var quote rune

	for _, r := range expr {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case strings.ContainsRune("\"'`", r):
			quote = r
		case strings.ContainsRune("([{", r):
			stack = append(stack, r)
		case strings.ContainsRune(")]}", r):
			if len(stack) == 0 || !closes(stack[len(stack)-1], r) {
				return false
			}

			stack = stack[:len(stack)-1]
		}
	}

	return quote == 0 && len(stack) == 0
}

func closes(open, closing rune) bool {
	return (open == '(' && closing == ')') || (open == '[' && closing == ']') || (open == '{' && closing == '}')
}

func validateRules(data []byte) error {
	doc, err := parseYAMLMap("rule groups namespace", data)
	if err != nil {
		return err
	}

	groups, ok := doc["groups"].([]any)
	if !ok || len(groups) == 0 {
		return cannotParse("rule groups namespace must define a non-empty groups list")
	}

	seen := map[string]bool{}

	for i, g := range groups {
		gm, iok := g.(map[string]any)
		if !iok {
			return cannotParse("groups[%d] must be a mapping", i)
		}

		name, _ := gm["name"].(string)
		if name == "" || seen[name] {
			return cannotParse("groups[%d] needs a unique non-empty name", i)
		}

		seen[name] = true

		if ierr := validateRuleGroup(i, gm); ierr != nil {
			return ierr
		}
	}

	return nil
}

func validateRuleGroup(i int, gm map[string]any) error {
	rules, ok := gm["rules"].([]any)
	if !ok || len(rules) == 0 {
		return cannotParse("groups[%d] must define a non-empty rules list", i)
	}

	for j, r := range rules {
		rm, iok := r.(map[string]any)
		if !iok {
			return cannotParse("groups[%d].rules[%d] must be a mapping", i, j)
		}

		record, _ := rm["record"].(string)
		alert, _ := rm["alert"].(string)

		if (record == "") == (alert == "") {
			return cannotParse("groups[%d].rules[%d] must set exactly one of record or alert", i, j)
		}

		expr, _ := rm["expr"].(string)
		if strings.TrimSpace(expr) == "" || !balanced(expr) {
			return cannotParse("groups[%d].rules[%d] has an invalid expr", i, j)
		}

		if f, isStr := rm["for"].(string); isStr && f != "" {
			if _, err := promql.ParseDuration(f); err != nil {
				return cannotParse("groups[%d].rules[%d] has an invalid for duration", i, j)
			}
		}
	}

	return nil
}

func validateAlertManager(data []byte) error {
	doc, err := parseYAMLMap("alert manager definition", data)
	if err != nil {
		return err
	}

	raw, ok := doc["alertmanager_config"].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return cannotParse("alert manager definition must define alertmanager_config as a YAML string")
	}

	cfg, err := parseYAMLMap("alertmanager_config", []byte(raw))
	if err != nil {
		return err
	}

	receivers, ok := cfg["receivers"].([]any)
	if !ok || len(receivers) == 0 {
		return cannotParse("alertmanager_config must define at least one receiver")
	}

	names := map[string]bool{}

	for _, r := range receivers {
		if rm, iok := r.(map[string]any); iok {
			if n, _ := rm["name"].(string); n != "" {
				names[n] = true
			}
		}
	}

	route, ok := cfg["route"].(map[string]any)
	if !ok {
		return cannotParse("alertmanager_config must define a route")
	}

	if recv, _ := route["receiver"].(string); !names[recv] {
		return cannotParse("route receiver %q is not defined in receivers", recv)
	}

	return nil
}

func validateScrapeConfig(data []byte) error {
	if err := validateData("scrape configuration", data); err != nil {
		return err
	}

	doc, err := parseYAMLMap("scrape configuration", data)
	if err != nil {
		return err
	}

	jobs, ok := doc["scrape_configs"].([]any)
	if !ok || len(jobs) == 0 {
		return cannotParse("scrape configuration must define a non-empty scrape_configs list")
	}

	seen := map[string]bool{}

	for i, j := range jobs {
		jm, iok := j.(map[string]any)
		if !iok {
			return cannotParse("scrape_configs[%d] must be a mapping", i)
		}

		name, _ := jm["job_name"].(string)
		if name == "" || seen[name] {
			return cannotParse("scrape_configs[%d] needs a unique non-empty job_name", i)
		}

		seen[name] = true
	}

	return nil
}

func validateLogGroup(arn string) error {
	if !logGroupRe.MatchString(arn) {
		return invalid("logGroupArn %q is not a CloudWatch Logs log group ARN", arn)
	}

	return nil
}

func validatePolicy(doc string) error {
	if len(doc) == 0 || len(doc) > maxPolicyBytes {
		return invalid("policyDocument must be between 1 and %d characters", maxPolicyBytes)
	}

	var p struct {
		Statement any `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(doc), &p); err != nil || p.Statement == nil {
		return cannotParse("policyDocument must be a JSON policy with a Statement")
	}

	return nil
}

func validateDetectorQuery(q string) error {
	q = strings.TrimSpace(q)
	if q == "" || len(q) > maxDetectorQL || !balanced(q) {
		return invalid("randomCutForest.query is not a valid PromQL expression")
	}

	if !detectorQRe.MatchString(q) {
		return invalid("randomCutForest.query must be wrapped by a supported aggregation operator")
	}

	return nil
}

func validateSubnetsAndGroups(subnets, groups []string) error {
	if len(subnets) == 0 || len(subnets) > maxSubnets {
		return invalid("subnetIds must contain between 1 and %d entries", maxSubnets)
	}

	if len(groups) > maxSecGroups {
		return invalid("securityGroupIds must contain at most %d entries", maxSecGroups)
	}

	return nil
}

func validComponentType(t string) bool {
	switch t {
	case "SERVICE_DISCOVERY", "COLLECTOR", "EXPORTER":
		return true
	}

	return false
}
