package servicequotas

import (
	"bytes"
	"compress/gzip"
	_ "embed" // quota catalog
	"encoding/json"
	"io"
	"slices"
	"strings"
	"sync"
)

//go:embed catalog.json
var catalogJSON []byte

//go:embed docs_quotas.json.gz
var docsQuotasGZ []byte

func docsQuotasJSON() ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(docsQuotasGZ))
	if err != nil {
		return nil, err
	}

	return io.ReadAll(zr)
}

// UsageMetric describes the CloudWatch metric that tracks usage of a quota.
type UsageMetric struct {
	Dimensions map[string]string `json:"dimensions,omitempty"`
	Namespace  string            `json:"namespace"`
	Name       string            `json:"name"`
	Statistic  string            `json:"statistic"`
}

// QuotaDef is one catalogued quota with its AWS default.
type QuotaDef struct {
	Usage       *UsageMetric `json:"usage,omitempty"`
	Code        string       `json:"code"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Unit        string       `json:"unit"`
	Default     float64      `json:"default"`
	Max         float64      `json:"max,omitempty"`
	Adjustable  bool         `json:"adjustable"`
	Global      bool         `json:"global,omitempty"`
}

// ServiceDef is one catalogued service and its quotas.
type ServiceDef struct {
	Code   string     `json:"code"`
	Name   string     `json:"name"`
	Source string     `json:"source,omitempty"`
	Quotas []QuotaDef `json:"quotas"`
}

// Catalog is the set of services and quotas the emulator knows.
type Catalog struct {
	services []ServiceDef
}

var defaultCatalog = sync.OnceValue(func() *Catalog { //nolint:gochecknoglobals // immutable embedded data
	var svcs []ServiceDef
	if err := json.Unmarshal(catalogJSON, &svcs); err != nil {
		panic("servicequotas: embedded catalog is invalid: " + err.Error())
	}

	raw, err := docsQuotasJSON()
	if err != nil {
		panic("servicequotas: embedded docs catalog is invalid: " + err.Error())
	}

	var docs []ServiceDef
	if err = json.Unmarshal(raw, &docs); err != nil {
		panic("servicequotas: embedded docs catalog is invalid: " + err.Error())
	}

	svcs = mergeServices(svcs, docs)

	slices.SortFunc(svcs, func(a, b ServiceDef) int { return strings.Compare(a.Code, b.Code) })

	return NewCatalog(svcs)
})

// NewCatalog builds a catalog from service definitions.
func NewCatalog(svcs []ServiceDef) *Catalog { return &Catalog{services: svcs} }

// Services returns every service in code order.
func (c *Catalog) Services() []ServiceDef { return c.services }

// Service looks a service up by its code.
func (c *Catalog) Service(code string) (*ServiceDef, bool) {
	for i := range c.services {
		if c.services[i].Code == code {
			return &c.services[i], true
		}
	}

	return nil, false
}

// Quota looks a quota up by service and quota code.
func (c *Catalog) Quota(serviceCode, quotaCode string) (*ServiceDef, *QuotaDef, bool) {
	svc, ok := c.Service(serviceCode)
	if !ok {
		return nil, nil, false
	}

	for i := range svc.Quotas {
		if svc.Quotas[i].Code == quotaCode {
			return svc, &svc.Quotas[i], true
		}
	}

	return svc, nil, false
}

// mergeServices adds the documented services to the curated ones; a curated quota or service name wins.
func mergeServices(curated, docs []ServiceDef) []ServiceDef {
	idx := make(map[string]int, len(curated))
	for i := range curated {
		idx[curated[i].Code] = i
	}

	for _, d := range docs {
		i, ok := idx[d.Code]
		if !ok {
			idx[d.Code] = len(curated)
			curated = append(curated, d)

			continue
		}

		have := make(map[string]bool, len(curated[i].Quotas))
		for _, q := range curated[i].Quotas {
			have[q.Code] = true
		}

		for _, q := range d.Quotas {
			if !have[q.Code] {
				curated[i].Quotas = append(curated[i].Quotas, q)
			}
		}

		if curated[i].Source == "" {
			curated[i].Source = d.Source
		}
	}

	return curated
}
