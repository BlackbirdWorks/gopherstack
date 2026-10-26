package azuremonitor

import (
	"maps"
	"net"
	"strconv"
	"strings"
	"time"
)

// Ref addresses an ARM resource served by this backend.
type Ref struct {
	// Host is the request host (no port) used to build advertised endpoints.
	Host      string
	Sub       string
	RG        string
	Namespace string
	Types     []string
	Names     []string
}

type kind int

const (
	kindUnknown kind = iota
	kindWorkspace
	kindTable
	kindDCE
	kindDCR
)

func (r Ref) kind() kind {
	path := strings.ToLower(strings.Join(r.Types, "/"))

	switch strings.ToLower(r.Namespace) + "/" + path {
	case "microsoft.operationalinsights/workspaces":
		return kindWorkspace
	case "microsoft.operationalinsights/workspaces/tables":
		return kindTable
	case "microsoft.insights/datacollectionendpoints":
		return kindDCE
	case "microsoft.insights/datacollectionrules":
		return kindDCR
	}

	return kindUnknown
}

func (r Ref) leaf() string {
	if len(r.Names) == 0 {
		return ""
	}

	return r.Names[len(r.Names)-1]
}

// Put creates or replaces a resource and returns its ARM body.
func (b *Backend) Put(ref Ref, body map[string]any) (map[string]any, error) {
	switch ref.kind() {
	case kindWorkspace:
		return b.putWorkspace(ref, body)
	case kindTable:
		return b.putTable(ref, body)
	case kindDCE:
		return b.putDCE(ref, body)
	case kindDCR:
		return b.putDCR(ref, body)
	case kindUnknown:
	}

	return nil, ErrUnsupported
}

// Get returns a resource's ARM body.
func (b *Backend) Get(ref Ref) (map[string]any, error) {
	b.mu.RLock("Get")
	defer b.mu.RUnlock()

	switch ref.kind() {
	case kindWorkspace:
		w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.leaf())
		if !ok {
			return nil, notFound("workspace %q", ref.leaf())
		}

		return w.body(), nil
	case kindTable:
		w, t, err := b.tableOf(ref)
		if err != nil {
			return nil, err
		}

		return t.body(w), nil
	case kindDCE:
		d, ok := b.dces[key(ref.Sub, ref.RG, ref.leaf())]
		if !ok {
			return nil, notFound("data collection endpoint %q", ref.leaf())
		}

		return d.body(b.endpoint(ref.Host)), nil
	case kindDCR:
		d, ok := b.dcrs[key(ref.Sub, ref.RG, ref.leaf())]
		if !ok {
			return nil, notFound("data collection rule %q", ref.leaf())
		}

		return b.dcrBody(d, b.endpoint(ref.Host)), nil
	case kindUnknown:
	}

	return nil, ErrUnsupported
}

func (b *Backend) tableOf(ref Ref) (*Workspace, *Table, error) {
	w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.Names[0])
	if !ok {
		return nil, nil, notFound("workspace %q", ref.Names[0])
	}

	t, ok := w.table(ref.Names[1])
	if !ok {
		return nil, nil, notFound("table %q", ref.Names[1])
	}

	return w, t, nil
}

// Delete removes a resource.
func (b *Backend) Delete(ref Ref) error {
	b.mu.Lock("Delete")
	defer b.mu.Unlock()

	switch ref.kind() {
	case kindWorkspace:
		k := key(ref.Sub, ref.RG, ref.leaf())
		if _, ok := b.workspaces[k]; !ok {
			return notFound("workspace %q", ref.leaf())
		}

		delete(b.workspaces, k)
	case kindTable:
		w, t, err := b.tableOf(ref)
		if err != nil {
			return err
		}

		if !t.Custom {
			return invalid("the table %q is a Microsoft table and cannot be deleted", t.Name)
		}

		delete(w.Tables, t.Name)
	case kindDCE:
		k := key(ref.Sub, ref.RG, ref.leaf())
		if _, ok := b.dces[k]; !ok {
			return notFound("data collection endpoint %q", ref.leaf())
		}

		delete(b.dces, k)
	case kindDCR:
		k := key(ref.Sub, ref.RG, ref.leaf())
		if _, ok := b.dcrs[k]; !ok {
			return notFound("data collection rule %q", ref.leaf())
		}

		delete(b.dcrs, k)
	case kindUnknown:
		return ErrUnsupported
	}

	return nil
}

// listScoped renders the in-scope members of m in key order.
func listScoped[T any](m map[string]T, inScope func(T) bool, render func(T) map[string]any) []map[string]any {
	out := []map[string]any{}

	for _, k := range sortedKeys(m) {
		if v := m[k]; inScope(v) {
			out = append(out, render(v))
		}
	}

	return out
}

// List returns every resource of the ref's type under the subscription, resource group or parent.
func (b *Backend) List(ref Ref) ([]map[string]any, error) {
	b.mu.RLock("List")
	defer b.mu.RUnlock()

	scoped := func(sub, rg string) bool {
		return strings.EqualFold(sub, ref.Sub) && (ref.RG == "" || strings.EqualFold(rg, ref.RG))
	}
	ep := b.endpoint(ref.Host)

	switch ref.kind() {
	case kindWorkspace:
		return listScoped(b.workspaces, func(w *Workspace) bool { return scoped(w.Sub, w.RG) },
			(*Workspace).body), nil
	case kindTable:
		w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.Names[0])
		if !ok {
			return nil, notFound("workspace %q", ref.Names[0])
		}

		return listScoped(w.Tables, func(*Table) bool { return true },
			func(t *Table) map[string]any { return t.body(w) }), nil
	case kindDCE:
		return listScoped(b.dces, func(d *DCE) bool { return scoped(d.Sub, d.RG) },
			func(d *DCE) map[string]any { return d.body(ep) }), nil
	case kindDCR:
		return listScoped(b.dcrs, func(d *DCR) bool { return scoped(d.Sub, d.RG) },
			func(d *DCR) map[string]any { return b.dcrBody(d, ep) }), nil
	case kindUnknown:
	}

	return nil, ErrUnsupported
}

// Patch updates tags (and, for workspaces and tables, selected properties).
func (b *Backend) Patch(ref Ref, body map[string]any) (map[string]any, error) {
	b.mu.Lock("Patch")
	defer b.mu.Unlock()

	now := b.now()

	switch ref.kind() {
	case kindWorkspace:
		w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.leaf())
		if !ok {
			return nil, notFound("workspace %q", ref.leaf())
		}

		if err := w.patch(body, now); err != nil {
			return nil, err
		}

		return w.body(), nil
	case kindTable:
		w, t, err := b.tableOf(ref)
		if err != nil {
			return nil, err
		}

		if err = t.applyProperties(w, mapOf(body[fieldProperties]), false); err != nil {
			return nil, err
		}

		return t.body(w), nil
	case kindDCE:
		d, ok := b.dces[key(ref.Sub, ref.RG, ref.leaf())]
		if !ok {
			return nil, notFound("data collection endpoint %q", ref.leaf())
		}

		if _, has := body["tags"]; has {
			d.Tags = stringMap(body["tags"])
		}

		return d.body(b.endpoint(ref.Host)), nil
	case kindDCR:
		d, ok := b.dcrs[key(ref.Sub, ref.RG, ref.leaf())]
		if !ok {
			return nil, notFound("data collection rule %q", ref.leaf())
		}

		if _, has := body["tags"]; has {
			d.Tags = stringMap(body["tags"])
		}

		return b.dcrBody(d, b.endpoint(ref.Host)), nil
	case kindUnknown:
	}

	return nil, ErrUnsupported
}

// DeleteInGroup removes every resource of the resource group.
func (b *Backend) DeleteInGroup(rg string) {
	b.mu.Lock("DeleteInGroup")
	defer b.mu.Unlock()

	for k, w := range b.workspaces {
		if strings.EqualFold(w.RG, rg) {
			delete(b.workspaces, k)
		}
	}

	for k, d := range b.dces {
		if strings.EqualFold(d.RG, rg) {
			delete(b.dces, k)
		}
	}

	for k, d := range b.dcrs {
		if strings.EqualFold(d.RG, rg) {
			delete(b.dcrs, k)
		}
	}
}

// Action runs a POST action on a workspace (sharedKeys, listKeys, regenerateSharedKey).
func (b *Backend) Action(ref Ref, action string, _ map[string]any) (map[string]any, error) {
	if ref.kind() != kindWorkspace {
		return nil, ErrUnsupported
	}

	b.mu.Lock("Action")
	defer b.mu.Unlock()

	w, ok := b.workspaceByRef(ref.Sub, ref.RG, ref.leaf())
	if !ok {
		return nil, notFound("workspace %q", ref.leaf())
	}

	switch strings.ToLower(action) {
	case "sharedkeys", "listkeys":
	case "regeneratesharedkey":
		w.Primary, w.Secondary = newKey(), newKey()
	default:
		return nil, ErrUnsupported
	}

	return map[string]any{"primarySharedKey": w.Primary, "secondarySharedKey": w.Secondary}, nil
}

func (b *Backend) endpoint(host string) string {
	if a := b.settings.Advertise; a != "" {
		return strings.TrimSuffix(a, "/")
	}

	if host == "" {
		host = "localhost"
	}

	port := b.settings.Port
	if port == 0 {
		port = DefaultPort
	}

	return "https://" + net.JoinHostPort(host, strconv.Itoa(port))
}

func envelope(id, name, typ string) map[string]any {
	return map[string]any{"id": id, fieldName: name, fieldType: typ}
}

func isoTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (w *Workspace) body() map[string]any {
	out := envelope(w.armID(), w.Name, nsOperationalInsights+"/"+typeWorkspaces)
	out["location"] = w.Location
	out["tags"] = w.Tags

	props := cloneJSON(w.Props)
	if props == nil {
		props = map[string]any{}
	}

	setDefault(props, "features", map[string]any{"enableLogAccessUsingOnlyResourcePermissions": true})
	setDefault(props, "workspaceCapping", map[string]any{
		"dailyQuotaGb": float64(-1), "dataIngestionStatus": "RespectQuota",
		"quotaNextResetTime": isoTime(w.Modified.Truncate(hoursPerDay * time.Hour).Add(hoursPerDay * time.Hour)),
	})
	setDefault(props, "publicNetworkAccessForIngestion", "Enabled")
	setDefault(props, "publicNetworkAccessForQuery", "Enabled")
	setDefault(props, "forceCmkForQuery", false)

	props["customerId"] = w.CustomerID
	props[fieldProvisioning] = provisioningSucceeded
	props["sku"] = map[string]any{"name": w.SKU, "lastSkuUpdate": isoTime(w.SKUUpdated)}
	props["retentionInDays"] = w.Retention
	props["createdDate"] = isoTime(w.Created)
	props["modifiedDate"] = isoTime(w.Modified)
	props["privateLinkScopedResources"] = []any{}
	out[fieldProperties] = props
	out["etag"] = `"` + w.CustomerID[:8] + `-` + strconv.FormatInt(w.Modified.UnixNano(), 10) + `"`

	return out
}

func setDefault(m map[string]any, k string, v any) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

var computedWorkspaceProps = []string{ //nolint:gochecknoglobals // static lookup
	"customerId", fieldProvisioning, "sku", "retentionInDays", "createdDate", "modifiedDate",
	"privateLinkScopedResources",
}

func validRetention(n int) bool { return n >= minRetention && n <= maxRetention }

func validWorkspaceSKU(name string) (string, bool) {
	for _, s := range workspaceSKUs {
		if strings.EqualFold(s, name) {
			return s, true
		}
	}

	return "", false
}

func (b *Backend) putWorkspace(ref Ref, body map[string]any) (map[string]any, error) {
	name := ref.leaf()
	if !workspaceNameRe.MatchString(name) {
		return nil, invalid("workspace name %q must be 4-63 characters of letters, digits and hyphens, "+
			"starting and ending with a letter or digit", name)
	}

	props := mapOf(body[fieldProperties])

	b.mu.Lock("putWorkspace")
	defer b.mu.Unlock()

	now := b.now()
	w, exists := b.workspaceByRef(ref.Sub, ref.RG, name)

	if !exists {
		w = &Workspace{
			Name: name, Sub: ref.Sub, RG: ref.RG, Created: now, CustomerID: uuid4(), SKU: defaultSKU,
			Retention: defaultRetention, SKUUpdated: now, Primary: newKey(), Secondary: newKey(),
			Tables: builtinTables(now),
		}
	}

	if loc := strOf(body["location"]); loc != "" {
		w.Location = loc
	} else if w.Location == "" {
		w.Location = defaultLocation
	}

	w.Tags = stringMap(body["tags"])

	if err := w.applyCore(props, now); err != nil {
		return nil, err
	}

	w.Props = omit(props, computedWorkspaceProps...)
	w.Modified = now
	b.workspaces[key(ref.Sub, ref.RG, name)] = w

	return w.body(), nil
}

func (w *Workspace) applyCore(props map[string]any, now time.Time) error {
	if sku := mapOf(props["sku"]); sku != nil {
		name, ok := validWorkspaceSKU(strOf(sku["name"]))
		if !ok {
			return invalid("the workspace SKU %q is not supported", strOf(sku["name"]))
		}

		if name != w.SKU {
			w.SKU, w.SKUUpdated = name, now
		}
	}

	if v, ok := props["retentionInDays"]; ok && v != nil {
		n, isNum := v.(float64)
		if !isNum || !validRetention(int(n)) {
			return invalid("retentionInDays must be between %d and %d", minRetention, maxRetention)
		}

		w.Retention = int(n)
	}

	return nil
}

func (w *Workspace) patch(body map[string]any, now time.Time) error {
	if _, has := body["tags"]; has {
		w.Tags = stringMap(body["tags"])
	}

	props := mapOf(body[fieldProperties])
	if err := w.applyCore(props, now); err != nil {
		return err
	}

	if w.Props == nil {
		w.Props = map[string]any{}
	}

	maps.Copy(w.Props, omit(props, computedWorkspaceProps...))

	w.Modified = now

	return nil
}

func uuid4() string { return newUUID() }

func builtinTables(now time.Time) map[string]*Table {
	out := map[string]*Table{}

	for name, columns := range builtinSchemas() {
		out[name] = &Table{Name: name, Columns: columns, Plan: planAnalytics, Created: now}
	}

	return out
}
