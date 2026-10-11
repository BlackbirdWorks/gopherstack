package ssm

import (
	_ "embed" // public_parameters.json
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

//go:embed public_parameters.json
var publicParametersJSON []byte

const (
	publicParameterPrefix = "/aws/service/"
	globalInfraPrefix     = "/aws/service/global-infrastructure/"
	publicParameterType   = "String"
	publicParameterData   = "text"
	publicParameterTier   = "Standard"
	// publicParameterModified is 2026-01-01T00:00:00Z.
	publicParameterModified = float64(1767225600)
)

type publicParametersFile struct {
	Parameters []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"parameters"`
	Regions []struct {
		Name     string `json:"name"`
		LongName string `json:"longName"`
	} `json:"regions"`
	Services []string `json:"services"`
}

type publicCatalog struct {
	byName map[string]string
	names  []string
}

//nolint:gochecknoglobals // read-only catalog built once from embedded data
var loadPublicCatalog = sync.OnceValue(func() *publicCatalog {
	var f publicParametersFile
	if err := json.Unmarshal(publicParametersJSON, &f); err != nil {
		panic("ssm: corrupt embedded public_parameters.json: " + err.Error())
	}

	c := &publicCatalog{byName: make(map[string]string, len(f.Parameters))}

	for _, p := range f.Parameters {
		c.byName[p.Name] = p.Value
	}

	for _, r := range f.Regions {
		c.byName[globalInfraPrefix+"regions/"+r.Name] = r.Name
		c.byName[globalInfraPrefix+"regions/"+r.Name+"/longName"] = r.LongName
		c.byName[globalInfraPrefix+"regions/"+r.Name+"/partition"] = arn.PartitionForRegion(r.Name)

		for _, svc := range f.Services {
			c.byName[globalInfraPrefix+"regions/"+r.Name+"/services/"+svc] = svc
			c.byName[globalInfraPrefix+"services/"+svc+"/regions/"+r.Name] = r.Name
		}
	}

	for _, svc := range f.Services {
		c.byName[globalInfraPrefix+"services/"+svc] = svc
	}

	c.names = make([]string, 0, len(c.byName))
	for name := range c.byName {
		c.names = append(c.names, name)
	}

	sort.Strings(c.names)

	return c
})

func isPublicParameterName(name string) bool {
	return strings.HasPrefix(name, publicParameterPrefix)
}

func publicParameter(name, region string) (Parameter, bool) {
	value, ok := loadPublicCatalog().byName[name]
	if !ok {
		return Parameter{}, false
	}

	return Parameter{
		Name:             name,
		Type:             publicParameterType,
		Value:            value,
		DataType:         publicParameterData,
		Tier:             publicParameterTier,
		ARN:              arn.Build("ssm", region, "", "parameter"+name),
		LastModifiedDate: publicParameterModified,
		Version:          1,
		Public:           true,
	}, true
}

// resolvePublicParameter honours a numeric version selector; public
// parameters only ever expose version 1 and carry no labels.
func resolvePublicParameter(name, selector, region string) (Parameter, error) {
	p, ok := publicParameter(name, region)
	if !ok {
		return Parameter{}, ErrParameterNotFound
	}

	if selector == "" {
		return p, nil
	}

	version, err := strconv.ParseInt(selector, 10, 64)
	if err != nil {
		return Parameter{}, ErrParameterNotFound
	}

	if version != p.Version {
		return Parameter{}, ErrParameterVersionNotFound
	}

	p.Selector = ":" + selector

	return p, nil
}

func publicParametersByPath(path, region string, recursive bool, filters []ParameterFilter) []*Parameter {
	if !strings.HasPrefix(path, publicParameterPrefix) {
		return nil
	}

	var out []*Parameter

	for _, name := range loadPublicCatalog().names {
		if !strings.HasPrefix(name, path) {
			continue
		}

		if !recursive && strings.Contains(name[len(path):], "/") {
			continue
		}

		p, _ := publicParameter(name, region)
		if len(filters) > 0 && !paramByPathMatchesFilters(&p, filters) {
			continue
		}

		out = append(out, &p)
	}

	return out
}

func filtersTargetPublicParameters(filters []ParameterFilter) bool {
	for _, f := range filters {
		if f.Key != "Name" && f.Key != "Path" {
			continue
		}

		if slices.ContainsFunc(f.Values, func(v string) bool {
			return strings.HasPrefix(v, "/aws/service")
		}) {
			return true
		}
	}

	return false
}

func publicParameterMetadata(region string) []ParameterMetadata {
	names := loadPublicCatalog().names
	out := make([]ParameterMetadata, 0, len(names))

	for _, name := range names {
		p, _ := publicParameter(name, region)
		out = append(out, ParameterMetadata{
			Name:             p.Name,
			Type:             p.Type,
			Tier:             p.Tier,
			DataType:         p.DataType,
			ARN:              p.ARN,
			LastModifiedDate: p.LastModifiedDate,
			Version:          p.Version,
		})
	}

	return out
}
