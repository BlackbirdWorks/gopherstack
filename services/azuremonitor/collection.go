package azuremonitor

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

const (
	streamPrefixCustom    = "Custom-"
	streamPrefixMicrosoft = "Microsoft-"
	kindDirect            = "Direct"
	sourceStream          = "source"
)

func (b *Backend) putDCE(ref Ref, body map[string]any) (map[string]any, error) {
	name := ref.leaf()
	if !ruleNameRe.MatchString(name) {
		return nil, invalid("data collection endpoint name %q is not valid", name)
	}

	b.mu.Lock("putDCE")
	defer b.mu.Unlock()

	k := key(ref.Sub, ref.RG, name)

	d, exists := b.dces[k]
	if !exists {
		d = &DCE{Name: name, Sub: ref.Sub, RG: ref.RG, ImmutableID: newImmutableID("dce-"), Created: b.now()}
	}

	d.Location = strOf(body["location"])
	if d.Location == "" {
		d.Location = defaultLocation
	}

	d.Kind = strOf(body["kind"])
	d.Tags = stringMap(body["tags"])
	d.Props = omit(mapOf(body[fieldProperties]),
		"immutableId", fieldProvisioning, "configurationAccess", "logsIngestion", "metricsIngestion")
	b.dces[k] = d

	return d.body(b.endpoint(ref.Host)), nil
}

func (d *DCE) body(endpoint string) map[string]any {
	out := envelope(d.armID(), d.Name, nsInsights+"/"+typeDCE)
	out["location"] = d.Location
	out["tags"] = d.Tags

	if d.Kind != "" {
		out["kind"] = d.Kind
	}

	props := cloneJSON(d.Props)
	if props == nil {
		props = map[string]any{}
	}

	if mapOf(props["networkAcls"]) == nil {
		props["networkAcls"] = map[string]any{"publicNetworkAccess": "Enabled"}
	}

	props["immutableId"] = d.ImmutableID
	props["configurationAccess"] = map[string]any{"endpoint": endpoint}
	props["logsIngestion"] = map[string]any{"endpoint": endpoint}
	props["metricsIngestion"] = map[string]any{"endpoint": endpoint}
	props[fieldProvisioning] = provisioningSucceeded
	out[fieldProperties] = props
	out["systemData"] = map[string]any{"createdAt": isoTime(d.Created), "lastModifiedAt": isoTime(d.Created)}

	return out
}

type flow struct {
	transform string
	output    string
	streams   []string
	dests     []string
}

type dcrSpec struct {
	decls map[string][]Column
	// logAnalytics maps a destination name to its workspace resource ID.
	logAnalytics map[string]string
	others       map[string]bool
	flows        []flow
}

func stringList(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))

	for _, e := range list {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func parseDCR(props map[string]any) (*dcrSpec, error) {
	spec := &dcrSpec{
		decls: map[string][]Column{}, logAnalytics: map[string]string{}, others: map[string]bool{},
	}

	for name, raw := range mapOf(props["streamDeclarations"]) {
		cols, err := parseDeclColumns(name, mapOf(raw)["columns"])
		if err != nil {
			return nil, err
		}

		spec.decls[name] = cols
	}

	dests := mapOf(props["destinations"])
	for kindName, raw := range dests {
		list, _ := raw.([]any)
		for _, item := range list {
			m := mapOf(item)
			name := strOf(m["name"])

			if name == "" {
				return nil, invalid("every destination needs a name")
			}

			if strings.EqualFold(kindName, "logAnalytics") {
				spec.logAnalytics[name] = strOf(m["workspaceResourceId"])
			} else {
				spec.others[name] = true
			}
		}
	}

	flows, _ := props["dataFlows"].([]any)
	for _, raw := range flows {
		m := mapOf(raw)
		f := flow{
			streams: stringList(m["streams"]), dests: stringList(m["destinations"]),
			transform: strOf(m["transformKql"]), output: strOf(m["outputStream"]),
		}

		if len(f.streams) == 0 || len(f.dests) == 0 {
			return nil, invalid("every data flow needs at least one stream and one destination")
		}

		spec.flows = append(spec.flows, f)
	}

	return spec, nil
}

func parseDeclColumns(stream string, raw any) ([]Column, error) {
	list, _ := raw.([]any)
	if len(list) == 0 {
		return nil, invalid("stream declaration %q needs at least one column", stream)
	}

	out := make([]Column, 0, len(list))

	for _, item := range list {
		m := mapOf(item)

		t, ok := kqlTypeOf(strOf(m["type"]))
		if !ok || strOf(m["name"]) == "" {
			return nil, invalid("stream declaration %q has an invalid column", stream)
		}

		out = append(out, Column{Name: strOf(m["name"]), Type: string(t)})
	}

	return out, nil
}

// streamColumns returns the input columns of a stream: its declaration or a Microsoft table schema.
func (s *dcrSpec) streamColumns(stream string) ([]Column, bool) {
	if cols, ok := s.decls[stream]; ok {
		return cols, true
	}

	if table, ok := strings.CutPrefix(stream, streamPrefixMicrosoft); ok {
		cols, found := builtinSchemas()[table]

		return cols, found
	}

	return nil, false
}

func outputTable(stream string) string {
	if t, ok := strings.CutPrefix(stream, streamPrefixCustom); ok {
		return t
	}

	return strings.TrimPrefix(stream, streamPrefixMicrosoft)
}

type sourceResolver struct{ rel *kql.Rel }

func (s sourceResolver) Table(_, name string) (*kql.Rel, bool) {
	if name == sourceStream {
		return s.rel, true
	}

	return nil, false
}

func columnsToKQL(cols []Column) []kql.Col {
	out := make([]kql.Col, len(cols))
	for i, c := range cols {
		out[i] = kql.Col{Name: c.Name, Type: kql.Type(c.Type)}
	}

	return out
}

func (b *Backend) validateDCR(spec *dcrSpec, props map[string]any) error {
	if dce := strOf(props["dataCollectionEndpointId"]); dce != "" {
		found := false

		for _, d := range b.dces {
			if strings.EqualFold(d.armID(), dce) {
				found = true
			}
		}

		if !found {
			return invalid("the data collection endpoint %q does not exist", dce)
		}
	}

	for name, id := range spec.logAnalytics {
		if _, ok := b.findWorkspaceLocked(id); !ok {
			return invalid("the Log Analytics workspace %q of destination %q does not exist", id, name)
		}
	}

	for _, f := range spec.flows {
		if err := b.validateFlow(spec, f); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) validateFlow(spec *dcrSpec, f flow) error {
	for _, d := range f.dests {
		if _, ok := spec.logAnalytics[d]; !ok && !spec.others[d] {
			return invalid("data flow destination %q is not declared in destinations", d)
		}
	}

	if f.output != "" && !strings.HasPrefix(f.output, streamPrefixCustom) &&
		!strings.HasPrefix(f.output, streamPrefixMicrosoft) {
		return invalid("outputStream %q must start with %q or %q", f.output, streamPrefixCustom, streamPrefixMicrosoft)
	}

	for _, s := range f.streams {
		cols, ok := spec.streamColumns(s)
		if !ok {
			return invalid("stream %q is not declared in streamDeclarations", s)
		}

		if f.transform != "" && f.transform != sourceStream {
			src := &kql.Rel{Cols: columnsToKQL(cols)}
			if _, err := kql.Execute(f.transform, kql.Options{Now: b.now(), Resolver: sourceResolver{src}}); err != nil {
				return invalid("transformKql of stream %q is not valid: %v", s, err)
			}
		}
	}

	return b.validateOutput(spec, f)
}

func (b *Backend) validateOutput(spec *dcrSpec, f flow) error {
	for _, d := range f.dests {
		wsID, ok := spec.logAnalytics[d]
		if !ok {
			continue
		}

		w, found := b.findWorkspaceLocked(wsID)
		if !found {
			continue
		}

		out := f.output
		if out == "" {
			out = f.streams[0]
		}

		if _, exists := w.table(outputTable(out)); !exists {
			return invalid("InvalidOutputTable: the table for output stream %q is not available for destination %q",
				out, d)
		}
	}

	return nil
}

func (b *Backend) putDCR(ref Ref, body map[string]any) (map[string]any, error) {
	name := ref.leaf()
	if !ruleNameRe.MatchString(name) {
		return nil, invalid("data collection rule name %q is not valid", name)
	}

	props := mapOf(body[fieldProperties])

	spec, err := parseDCR(props)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("putDCR")
	defer b.mu.Unlock()

	if err = b.validateDCR(spec, props); err != nil {
		return nil, err
	}

	k := key(ref.Sub, ref.RG, name)

	d, exists := b.dcrs[k]
	if !exists {
		d = &DCR{Name: name, Sub: ref.Sub, RG: ref.RG, ImmutableID: newImmutableID("dcr-"), Created: b.now()}
	}

	d.Location = strOf(body["location"])
	if d.Location == "" {
		d.Location = defaultLocation
	}

	d.Kind = strOf(body["kind"])
	d.Tags = stringMap(body["tags"])
	d.Identity = mapOf(cloneJSON(mapOf(body["identity"])))
	d.Props = omit(props, "immutableId", fieldProvisioning, "endpoints")
	b.dcrs[k] = d

	return b.dcrBody(d, b.endpoint(ref.Host)), nil
}

func (b *Backend) dcrBody(d *DCR, endpoint string) map[string]any {
	out := envelope(d.armID(), d.Name, nsInsights+"/"+typeDCR)
	out["location"] = d.Location
	out["tags"] = d.Tags

	if d.Kind != "" {
		out["kind"] = d.Kind
	}

	if d.Identity != nil {
		out["identity"] = d.Identity
	}

	props := cloneJSON(d.Props)
	if props == nil {
		props = map[string]any{}
	}

	if las, ok := mapOf(props["destinations"])["logAnalytics"].([]any); ok {
		for _, item := range las {
			m := mapOf(item)
			if w, found := b.findWorkspaceLocked(strOf(m["workspaceResourceId"])); found {
				m["workspaceId"] = w.CustomerID
			}
		}
	}

	props["immutableId"] = d.ImmutableID
	props[fieldProvisioning] = provisioningSucceeded

	if d.Kind == kindDirect {
		props["endpoints"] = map[string]any{"logsIngestion": endpoint, "metricsIngestion": endpoint}
	}

	out[fieldProperties] = props
	out["etag"] = `"` + d.ImmutableID[4:12] + `"`
	out["systemData"] = map[string]any{"createdAt": isoTime(d.Created), "lastModifiedAt": isoTime(d.Created)}

	return out
}

func (b *Backend) dcrByImmutableID(id string) (*DCR, bool) {
	for _, d := range b.dcrs {
		if d.ImmutableID == id {
			return d, true
		}
	}

	return nil, false
}

