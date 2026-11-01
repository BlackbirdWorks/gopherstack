package gcpapigateway

import (
	"regexp"
	"slices"
	"strings"
)

const (
	kindApis       = "apis"
	kindConfigs    = "configs"
	kindGateways   = "gateways"
	kindOperations = "operations"

	globalLocation   = "global"
	maxLabels        = 64
	maskDisplay      = "display_name"
	maskDisplayCamel = "displayName"
	maskLabels       = "labels"
	maskAPIConfig    = "api_config"
	maskAll          = "*"
)

var (
	idRe       = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	labelKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValRe = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	saEmailRe  = regexp.MustCompile(`^[^@/\s]+@[^@/\s]+\.iam\.gserviceaccount\.com$`)
)

// gatewayRegions are the regions a gateway can be created in.
//
//nolint:gochecknoglobals // immutable lookup table
var gatewayRegions = []string{
	"asia-northeast1", "australia-southeast1", "europe-west1", "europe-west2", "us-central1", "us-east1",
	"us-east4", "us-west2", "us-west3", "us-west4",
}

func parseRes(name string, kinds ...string) ([]string, bool) {
	p := strings.Split(name, "/")
	if len(p) != 2*len(kinds) {
		return nil, false
	}

	ids := make([]string, len(kinds))

	for i, k := range kinds {
		if p[2*i] != k || p[2*i+1] == "" {
			return nil, false
		}

		ids[i] = p[2*i+1]
	}

	return ids, true
}

func parseParent(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations")
	if !ok {
		return "", "", errInvalid("Invalid parent %q: want projects/{project}/locations/{location}.", name)
	}

	return ids[0], ids[1], nil
}

// parseGatewayName returns the project and location of "projects/{p}/locations/{l}/gateways/{id}".
func parseGatewayName(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations", kindGateways)
	if !ok {
		return "", "", errInvalid(
			"Invalid name %q: want projects/{project}/locations/{location}/gateways/{id}.", name)
	}

	return ids[0], ids[1], nil
}

// parseConfigName returns the project of an API config name.
func parseConfigName(name string) (string, error) {
	ids, ok := parseRes(name, "projects", "locations", kindApis, kindConfigs)
	if !ok || ids[1] != globalLocation {
		return "", errInvalid(
			"Invalid name %q: want projects/{project}/locations/global/apis/{api}/configs/{config}.", name)
	}

	return ids[0], nil
}

func parseAPIName(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations", kindApis)
	if !ok || ids[1] != globalLocation {
		return "", "", errInvalid("Invalid name %q: want projects/{project}/locations/global/apis/{api}.", name)
	}

	return ids[0], ids[2], nil
}

func locationName(project, location string) string {
	return "projects/" + project + "/locations/" + location
}

func childName(project, location, kind, id string) string {
	return locationName(project, location) + "/" + kind + "/" + id
}

func apiName(project, api string) string { return childName(project, globalLocation, kindApis, api) }

func configName(project, api, config string) string {
	return apiName(project, api) + "/" + kindConfigs + "/" + config
}

func checkID(field, id string) error {
	if id == "" {
		return errInvalid("%s is required.", field)
	}

	if !idRe.MatchString(id) {
		return errInvalid("Invalid %s %q: must match %s.", field, id, idRe.String())
	}

	return nil
}

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("At most %d labels are allowed.", maxLabels)
	}

	for k, v := range labels {
		if !labelKeyRe.MatchString(k) {
			return errInvalid("Invalid label key %q.", k)
		}

		if !labelValRe.MatchString(v) {
			return errInvalid("Invalid value %q for label %q.", v, k)
		}
	}

	return nil
}

func knownRegion(r string) bool { return slices.Contains(gatewayRegions, r) }

func validServiceAccount(sa string) bool {
	return saEmailRe.MatchString(sa) || strings.HasPrefix(sa, "projects/") && strings.Count(sa, "/") >= 3
}
