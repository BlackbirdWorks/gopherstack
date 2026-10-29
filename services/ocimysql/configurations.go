package ocimysql

import (
	"maps"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	caseSensitive   = "CASE_SENSITIVE"
	caseInsensitive = "CASE_INSENSITIVE_LOWERCASE"
)

// CreateConfigurationIn is the CreateConfiguration body.
type CreateConfigurationIn struct {
	Tags
	Variables     map[string]any `json:"variables"`
	InitVariables *struct {
		LowerCaseTableNames string `json:"lowerCaseTableNames"`
	} `json:"initVariables"`
	CompartmentID string `json:"compartmentId"`
	ShapeName     string `json:"shapeName"`
	DisplayName   string `json:"displayName"`
	Description   string `json:"description"`
	ParentID      string `json:"parentConfigurationId"`
}

// validateVariables checks names and value kinds against the SDK ConfigurationVariables model.
func validateVariables(vars map[string]any) (map[string]any, *APIError) {
	out := make(map[string]any, len(vars))

	for k, v := range vars {
		kind, ok := configVariables[k]
		if !ok {
			return nil, errParam("variables." + k + " is not a MySQL configuration variable")
		}

		if v == nil {
			continue
		}

		nv, ok := coerceVariable(kind, v)
		if !ok {
			return nil, errParam("variables." + k + " must be of type " + kind)
		}

		out[k] = nv
	}

	return out, nil
}

func coerceVariable(kind string, v any) (any, bool) {
	switch kind {
	case varInt:
		f, ok := v.(float64)
		if ok && f == math.Trunc(f) {
			return int64(f), true
		}

		i, isInt := v.(int64)

		return i, isInt
	case varBool:
		b, ok := v.(bool)

		return b, ok
	default:
		s, ok := v.(string)

		return s, ok
	}
}

func (b *InMemoryBackend) configDoc(c *Configuration, full bool) Doc {
	d := Doc{
		"id": c.ID, keyCompartment: c.CompartmentID, "shapeName": c.Shape, "type": c.Type,
		keyLifecycle: c.State, keyDisplayName: c.Name, keyTimeCreated: c.TimeCreated, keyTimeUpdated: c.TimeUpdated,
	}
	if c.Description != "" {
		d["description"] = c.Description
	}

	putTags(d, c.Tags)

	if !full {
		return d
	}

	d["options"], d["variables"] = []Doc{}, c.Variables
	if c.ParentID != "" {
		d["parentConfigurationId"] = c.ParentID
	}

	if c.LowerCase != "" {
		d["initVariables"] = Doc{"lowerCaseTableNames": c.LowerCase}
	}

	return d
}

// CreateConfiguration creates a CUSTOM configuration.
func (b *InMemoryBackend) CreateConfiguration(in CreateConfigurationIn) (Doc, string, error) {
	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	if aerr := firstErr(b.checkCompartment(in.CompartmentID)); aerr != nil {
		return nil, "", aerr
	}

	parent, shape, aerr := b.configParent(in)
	if aerr != nil {
		return nil, "", aerr
	}

	vars, aerr := validateVariables(in.Variables)
	if aerr != nil {
		return nil, "", aerr
	}

	merged := maps.Clone(parent.Variables)
	if merged == nil {
		merged = map[string]any{}
	}

	maps.Copy(merged, vars)

	name := orStr(in.DisplayName, "MySQLConfiguration"+b.nowUTC().Format("20060102150405"))
	if aerr = validateName(name); aerr != nil {
		return nil, "", aerr
	}

	lower := ""
	if in.InitVariables != nil {
		lower = in.InitVariables.LowerCaseTableNames
		if aerr = validEnum("initVariables.lowerCaseTableNames", lower, caseSensitive, caseInsensitive); aerr != nil {
			return nil, "", aerr
		}
	}

	now := b.nowUTC()
	c := &Configuration{
		ID:            newOCID("mysqlconfiguration"),
		CompartmentID: in.CompartmentID,
		Shape:         shape,
		Type:          configCustom,
		State:         stateActive,
		Name:          name,
		Description:   in.Description,
		ParentID:      parent.ID,
		LowerCase:     strings.ToUpper(lower),
		Variables:     merged,
		Tags:          in.Tags,
		Etag:          newEtag(),
		TimeCreated:   now,
		TimeUpdated:   now,
		Seq:           b.nextSeq(),
	}
	b.configs[c.ID] = c

	return b.configDoc(c, true), c.Etag, nil
}

func (b *InMemoryBackend) configParent(in CreateConfigurationIn) (*Configuration, string, *APIError) {
	if in.ShapeName == "" {
		return nil, "", errMissing("shapeName is required")
	}

	sh, ok := shapeByName(in.ShapeName)
	if !ok || sh.support != supportDBSystem {
		return nil, "", errParam("shapeName " + in.ShapeName + " is not a MySQL DB system shape; see ListShapes")
	}

	pid := orStr(in.ParentID, defaultConfigID(in.ShapeName, false))

	parent, ok := b.lookupConfig(pid)
	if !ok {
		return nil, "", errParam("parentConfigurationId " + pid + " does not reference an existing configuration")
	}

	if parent.Shape != in.ShapeName {
		return nil, "", errParam("parentConfigurationId is for shape " + parent.Shape + ", not " + in.ShapeName)
	}

	return parent, in.ShapeName, nil
}

// GetConfiguration returns a default or custom configuration.
func (b *InMemoryBackend) GetConfiguration(id string) (Doc, string, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.lookupConfig(id)
	if !ok {
		return nil, "", errNotFound(id)
	}

	return b.configDoc(c, true), c.Etag, nil
}

// ConfigListIn are the ListConfigurations filters.
type ConfigListIn struct {
	Compartment string
	ID          string
	State       string
	Name        string
	Shape       string
	Types       []string
	ListOpts
}

func (b *InMemoryBackend) validConfigList(in ConfigListIn) *APIError {
	aerr := firstErr(b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, "shapeName", keyTimeCreated, keyTimeUpdated),
		validEnum(keyLifecycle, in.State, stateActive, stateDeleted))

	for _, t := range in.Types {
		aerr = firstErr(aerr, validEnum("type", t, configDefault, configCustom))
	}

	return aerr
}

func (in ConfigListIn) matches(c *Configuration) bool {
	return (in.ID == "" || c.ID == in.ID) && (in.Name == "" || c.Name == in.Name) &&
		(in.Shape == "" || c.Shape == in.Shape) && (in.State == "" || strings.EqualFold(c.State, in.State)) &&
		(len(in.Types) == 0 || containsFold(in.Types, c.Type))
}

// ListConfigurations lists the default configurations and the custom ones of a compartment.
func (b *InMemoryBackend) ListConfigurations(in ConfigListIn) (page.Page[Doc], error) {
	if aerr := b.validConfigList(in); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	all := slices.Clone(b.defaultList)

	for _, c := range b.configs {
		if c.CompartmentID == in.Compartment && c.State == stateActive {
			all = append(all, c)
		}
	}

	all = slices.DeleteFunc(all, func(c *Configuration) bool { return !in.matches(c) })

	pg := paginate(all, in.ListOpts, func(c *Configuration) sortKeys {
		return sortKeys{created: c.TimeCreated, updated: c.TimeUpdated, name: c.Name, shape: c.Shape, seq: c.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, b.configDoc(c, false))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

// UpdateConfigurationIn is the UpdateConfiguration body.
type UpdateConfigurationIn struct {
	DisplayName *string `json:"displayName"`
	Description *string `json:"description"`
	Tags
}

func (b *InMemoryBackend) customConfig(id, ifMatch string) (*Configuration, *APIError) {
	if _, isDefault := b.defaults[id]; isDefault {
		return nil, errParam("default configurations cannot be modified or deleted")
	}

	c, ok := b.configs[id]
	if !ok || c.State != stateActive {
		return nil, errNotFound(id)
	}

	return c, checkEtag(c.Etag, ifMatch)
}

// UpdateConfiguration updates a custom configuration.
func (b *InMemoryBackend) UpdateConfiguration(id, ifMatch string, in UpdateConfigurationIn) (Doc, string, error) {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	c, aerr := b.customConfig(id, ifMatch)
	if aerr != nil {
		return nil, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateName(*in.DisplayName); aerr != nil {
			return nil, "", aerr
		}

		c.Name = *in.DisplayName
	}

	if in.Description != nil {
		c.Description = *in.Description
	}

	c.Tags.merge(in.Tags)
	c.TimeUpdated, c.Etag = b.nowUTC(), newEtag()

	return b.configDoc(c, true), c.Etag, nil
}

// DeleteConfiguration deletes a custom configuration that no DB system uses.
func (b *InMemoryBackend) DeleteConfiguration(id, ifMatch string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if _, aerr := b.customConfig(id, ifMatch); aerr != nil {
		return aerr
	}

	for _, s := range b.systems {
		if s.ConfigID == id && s.State != stateDeleted {
			return errState("configuration " + id + " is used by DB system " + s.ID)
		}
	}

	delete(b.configs, id)

	return nil
}

// ListShapes lists the documented MySQL shapes.
func (b *InMemoryBackend) ListShapes(compartment string, supportedFor []string, name string) ([]Doc, error) {
	if aerr := b.checkCompartmentLocked(compartment); aerr != nil {
		return nil, aerr
	}

	for _, v := range supportedFor {
		if aerr := validEnum("isSupportedFor", v, supportDBSystem, supportHeatWave); aerr != nil {
			return nil, aerr
		}
	}

	out := []Doc{}

	for _, s := range shapeDefs {
		if (name != "" && s.name != name) || (len(supportedFor) > 0 && !containsFold(supportedFor, s.support)) {
			continue
		}

		out = append(out, Doc{
			"name": s.name, "cpuCoreCount": s.cpus, "memorySizeInGBs": s.memGB, "isSupportedFor": []string{s.support},
		})
	}

	return out, nil
}

// ListVersions lists the available MySQL versions by family.
func (b *InMemoryBackend) ListVersions(compartment string) ([]Doc, error) {
	if aerr := b.checkCompartmentLocked(compartment); aerr != nil {
		return nil, aerr
	}

	fams := append([]versionFamily(nil), versionFamilies...)
	sort.SliceStable(fams, func(i, j int) bool { return fams[i].family < fams[j].family })

	out := make([]Doc, 0, len(fams))

	for _, f := range fams {
		vs := make([]Doc, 0, len(f.versions))
		for _, v := range f.versions {
			vs = append(vs, Doc{"version": v, "description": "MySQL " + v})
		}

		out = append(out, Doc{"versionFamily": f.family, "versions": vs})
	}

	return out, nil
}

func (h *Handler) createConfiguration(q *req) error {
	var in CreateConfigurationIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, err := h.Backend.CreateConfiguration(in)

	return reply(q, d, etag, err)
}

func (h *Handler) getConfiguration(q *req) error {
	d, etag, err := h.Backend.GetConfiguration(q.param("configurationId"))

	return reply(q, d, etag, err)
}

func (h *Handler) listConfigurations(q *req) error {
	pg, err := h.Backend.ListConfigurations(ConfigListIn{
		Compartment: q.query(keyCompartment), ID: q.query("configurationId"), State: q.query(keyLifecycle),
		Name: q.query(keyDisplayName), Shape: q.query("shapeName"), Types: q.queryAll("type"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) updateConfiguration(q *req) error {
	var in UpdateConfigurationIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, err := h.Backend.UpdateConfiguration(q.param("configurationId"), q.ifMatch(), in)

	return reply(q, d, etag, err)
}

func (h *Handler) deleteConfiguration(q *req) error {
	if err := h.Backend.DeleteConfiguration(q.param("configurationId"), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) listShapes(q *req) error {
	out, err := h.Backend.ListShapes(q.query(keyCompartment), q.queryAll("isSupportedFor"), q.query("name"))

	return reply(q, out, "", err)
}

func (h *Handler) listVersions(q *req) error {
	out, err := h.Backend.ListVersions(q.query(keyCompartment))

	return reply(q, out, "", err)
}
