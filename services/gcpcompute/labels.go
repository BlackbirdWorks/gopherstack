package gcpcompute

import (
	"encoding/json"
	"regexp"
	"sort"
)

const (
	maxLabels         = 64
	emptyLabelsFP     = "42WmSpB8rSM="
	labelsStaleDetail = "Labels fingerprint either invalid or resource labels have changed"
)

var (
	labelKeyRE   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValueRE = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	nameRE       = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

type labelsRequest struct {
	Labels           map[string]string `json:"labels"`
	LabelFingerprint string            `json:"labelFingerprint"`
}

func validName(field, name string) *APIError {
	if name == "" {
		return errRequired(field)
	}

	if !nameRE.MatchString(name) {
		return errField(field, name,
			"Must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'")
	}

	return nil
}

func validLabels(labels map[string]string) *APIError {
	if len(labels) > maxLabels {
		return errField("labels", "", "Too many labels, at most 64 are allowed.")
	}

	for k, v := range labels {
		if !labelKeyRE.MatchString(k) {
			return errField("labels.key", k, "Label keys must start with a lowercase letter and contain only "+
				"lowercase letters, digits, underscores and dashes (max 63 characters).")
		}

		if !labelValueRE.MatchString(v) {
			return errField("labels.value", v, "Label values may contain only lowercase letters, digits, "+
				"underscores and dashes (max 63 characters).")
		}
	}

	return nil
}

func labelFP(labels map[string]string) string {
	if len(labels) == 0 {
		return emptyLabelsFP
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	pairs := make([][2]string, len(keys))
	for i, k := range keys {
		pairs[i] = [2]string{k, labels[k]}
	}

	data, _ := json.Marshal(pairs)

	return fingerprint(string(data))
}

// applyLabels validates a setLabels request against the current labels and fingerprint.
func applyLabels(cur map[string]string, curFP string, req labelsRequest) (map[string]string, string, *APIError) {
	if curFP == "" {
		curFP = labelFP(cur)
	}

	if req.LabelFingerprint != curFP {
		return nil, "", errPrecondition("%s", labelsStaleDetail)
	}

	if err := validLabels(req.Labels); err != nil {
		return nil, "", err
	}

	next := map[string]string{}
	for k, v := range req.Labels {
		next[k] = v
	}

	return next, labelFP(next), nil
}
