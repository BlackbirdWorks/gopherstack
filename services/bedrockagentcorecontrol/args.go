package bedrockagentcorecontrol

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	keyDescription       = "description"
	keyAgentRuntimeID    = "agentRuntimeId"
	keyAPIKey            = "apiKey"
	keyARN               = "arn"
	keyParentVersionIDs  = "parentVersionIds"
	keyCreatedBy         = "createdBy"
	keyBranchName        = "branchName"
	keyCommitMessage     = "commitMessage"
	keyCredProviderARN   = "credentialProviderArn" //nolint:gosec // member name, not a credential
	keyDatasetID         = "datasetId"
	keyEndpointName      = "endpointName"
	keyGatewayIdentifier = "gatewayIdentifier"
	keyHarnessID         = "harnessId"
	keyPaymentManagerID  = "paymentManagerId"
	keyPolicy            = "policy"
	keyPolicyEngineID    = "policyEngineId"
	keyRateLimitID       = "rateLimitId"
	keyRegistryID        = "registryId"
	keyStatus            = "status"
	keyMemory            = "memory"
	draftUnmodified      = "UNMODIFIED"
	idModeName           = "name"
	idModeNameKeyed      = "name-keyed"
	idModeToken          = "token"
	idModeToken12        = "token12"
)

const (
	keyClientToken = "clientToken"
	keyMaxResults  = "maxResults"
	keyNextToken   = "nextToken"
	keyTags        = "tags"
	keyName        = "name"

	defaultPageLimit = 50
	maxPageLimit     = 100
	maxTags          = 50
)

// args is the merged input of one call: JSON body members, query parameters and path labels by wire name.
type args map[string]any

func (a args) str(k string) string {
	s, _ := a[k].(string)

	return s
}

func (a args) has(k string) bool {
	v, ok := a[k]

	return ok && v != nil
}

func (a args) list(k string) []any {
	l, _ := a[k].([]any)

	return l
}

func (a args) obj(k string) map[string]any {
	m, _ := a[k].(map[string]any)

	return m
}

func (a args) strMap(k string) map[string]string {
	out := map[string]string{}

	for key, v := range a.obj(k) {
		s, _ := v.(string)
		out[key] = s
	}

	return out
}

func (a args) integer(k string) (int, bool) {
	switch v := a[k].(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}

	return 0, false
}

func buildArgs(req *restroute.Request, sp *opSpec) (args, error) {
	in := args{}

	if len(req.Body) > 0 {
		var body map[string]any
		if err := req.Decode(&body); err != nil {
			return nil, err
		}

		maps.Copy(in, body)
	}

	for _, q := range sp.Query {
		vals, ok := req.Query[q]
		if !ok || len(vals) == 0 {
			continue
		}

		if q == keyMaxResults {
			n, err := strconv.Atoi(vals[0])
			if err != nil {
				return nil, invalid("maxResults must be an integer")
			}

			in[q] = float64(n)

			continue
		}

		in[q] = vals[0]
	}

	for k, v := range req.Params {
		in[k] = v
	}

	return in, nil
}

func checkRequired(sp *opSpec, in args) error {
	var missing []string

	for _, k := range sp.Required {
		if v, ok := in[k]; !ok || v == nil || v == "" {
			missing = append(missing, k)
		}
	}

	if len(missing) > 0 {
		return invalid("1 validation error detected: %s must not be null or empty", strings.Join(missing, ", "))
	}

	return nil
}

func checkEnums(sp *opSpec, in args) error {
	for k, vals := range sp.Enums {
		s, ok := in[k].(string)
		if !ok {
			continue
		}

		if !containsStr(vals, s) {
			return invalid("Value '%s' at '%s' failed to satisfy constraint: Member must satisfy enum value set: [%s]",
				s, k, strings.Join(vals, ", "))
		}
	}

	return nil
}

func containsStr(vals []string, s string) bool {
	return slices.Contains(vals, s)
}

// idOf reduces an ARN to its final path segment so callers may pass either an identifier or an ARN.
func idOf(v string) string {
	if strings.HasPrefix(v, "arn:") {
		if _, after, ok := strings.CutLast(v, "/"); ok {
			return after
		}
	}

	return v
}
