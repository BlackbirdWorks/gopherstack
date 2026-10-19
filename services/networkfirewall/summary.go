package networkfirewall

import (
	"context"
	"slices"
	"strings"
)

const (
	summarySID      = "SID"
	summaryMsg      = "MSG"
	summaryMetadata = "METADATA"
)

// RuleSummary is one rule's selected options.
type RuleSummary struct {
	Metadata string `json:"Metadata,omitempty"`
	Msg      string `json:"Msg,omitempty"`
	SID      string `json:"SID,omitempty"`
}

// Summary is the DescribeRuleGroupSummary body.
type Summary struct {
	RuleSummaries []RuleSummary `json:"RuleSummaries"`
}

// DescribeRuleGroupSummaryInput is the DescribeRuleGroupSummary request.
type DescribeRuleGroupSummaryInput = DescribeRuleGroupMetadataInput

// DescribeRuleGroupSummaryOutput is the DescribeRuleGroupSummary response.
type DescribeRuleGroupSummaryOutput struct {
	RuleGroupName string  `json:"RuleGroupName"`
	Description   string  `json:"Description,omitempty"`
	Summary       Summary `json:"Summary"`
}

// ruleOptions splits the "(key:value; key; ...)" tail of a Suricata rule, honouring quotes and escapes.
func ruleOptions(line string) map[string]string {
	open, end := strings.Index(line, "("), strings.LastIndex(line, ")")
	if open < 0 || end <= open {
		return nil
	}

	opts := map[string]string{}

	var cur strings.Builder

	quoted, escaped := false, false

	flush := func() {
		key, val, _ := strings.Cut(strings.TrimSpace(cur.String()), ":")
		if key = strings.TrimSpace(key); key != "" {
			if _, dup := opts[key]; !dup {
				opts[key] = strings.TrimSpace(val)
			}
		}

		cur.Reset()
	}

	for _, c := range line[open+1 : end] {
		switch {
		case escaped:
			escaped = false
		case c == '\\':
			escaped = true
		case c == '"':
			quoted = !quoted
		case c == ';' && !quoted:
			flush()

			continue
		}

		cur.WriteRune(c)
	}

	flush()

	return opts
}

func summarize(opts map[string]string, want []string) RuleSummary {
	var s RuleSummary

	if slices.Contains(want, summarySID) {
		s.SID = opts["sid"]
	}

	if slices.Contains(want, summaryMsg) {
		s.Msg = strings.Trim(opts["msg"], `"`)
	}

	if slices.Contains(want, summaryMetadata) {
		s.Metadata = opts["metadata"]
	}

	return s
}

func structuredOptions(r StatefulRule) map[string]string {
	opts := map[string]string{}

	for _, o := range r.RuleOptions {
		opts[o.Keyword] = strings.Join(o.Settings, ", ")
	}

	return opts
}

func ruleSummaries(g *RuleGroup, want []string) []RuleSummary {
	out := []RuleSummary{}

	for line := range strings.SplitSeq(g.RulesSource.RulesString, "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, summarize(ruleOptions(line), want))
		}
	}

	for _, r := range g.RulesSource.StatefulRules {
		out = append(out, summarize(structuredOptions(r), want))
	}

	return out
}

// DescribeRuleGroupSummary returns the selected options of each rule in a stateful rule group.
func (b *InMemoryBackend) DescribeRuleGroupSummary(
	_ context.Context, in *DescribeRuleGroupSummaryInput,
) (*DescribeRuleGroupSummaryOutput, error) {
	b.mu.RLock("DescribeRuleGroupSummary")
	defer b.mu.RUnlock()

	r, err := b.findRuleGroupLocked(in.RuleGroupArn, in.RuleGroupName, in.Type)
	if err != nil {
		return nil, err
	}

	if r.Type != ruleGroupStateful {
		return nil, invalid("Only STATEFUL rule groups have a summary")
	}

	var want []string
	if r.Summary != nil {
		want = r.Summary.RuleOptions
	}

	return &DescribeRuleGroupSummaryOutput{
		RuleGroupName: r.Name,
		Description:   r.Description,
		Summary:       Summary{RuleSummaries: ruleSummaries(&r.RuleGroup, want)},
	}, nil
}
