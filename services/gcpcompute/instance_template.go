package gcpcompute

import (
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const collTemplates = "instanceTemplates"

func templateKey(project, loc, name string) string {
	if loc == "" {
		loc = scopeGlobal
	}

	return key(project, loc, name)
}

func templateTarget(project, loc string, t *computeapi.InstanceTemplate) target {
	if loc == "" {
		return target{project: project, scope: scopeGlobal, collection: collTemplates, name: t.Name, id: t.Id}
	}

	return target{project: project, scope: scopeRegion, loc: loc, collection: collTemplates, name: t.Name, id: t.Id}
}

func (b *InMemoryBackend) normalizePropsLocked(p *computeapi.InstanceProperties) *APIError {
	if p == nil {
		return errRequired("resource.properties")
	}

	if p.MachineType == "" {
		return errRequired("resource.properties.machineType")
	}

	name := lastSegment(p.MachineType)
	if _, ok := findMachine(name); !ok {
		if _, _, custom := customMachine(name); !custom {
			return errField("resource.properties.machineType", p.MachineType, "Unknown machine type.")
		}
	}

	if len(p.Disks) == 0 {
		return errRequired("resource.properties.disks")
	}

	if err := firstErr(validLabels(p.Labels), validMetadata(p.Metadata), validTagList(p.Tags)); err != nil {
		return err
	}

	sched, err := defaultScheduling(p.Scheduling)
	if err != nil {
		return err
	}

	p.Scheduling = sched
	p.Metadata = normalizeMetadata(p.Metadata)

	if p.Tags != nil {
		p.Tags.Fingerprint = tagsFP(p.Tags.Items)
	}

	for _, d := range p.Disks {
		d.Kind = "compute#attachedDisk"
		if d.Type == "" {
			d.Type = "PERSISTENT"
		}

		if d.Mode == "" {
			d.Mode = "READ_WRITE"
		}
	}

	return nil
}

func (b *InMemoryBackend) insertTemplate(project, region string, in *computeapi.InstanceTemplate) (*computeapi.Operation, error) {
	b.mu.Lock("InsertInstanceTemplate")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if in.SourceInstance != "" {
		return nil, errField("resource.sourceInstance", in.SourceInstance, "Creating a template from an instance is not supported by this emulator.")
	}

	k := templateKey(project, region, in.Name)
	if _, dup := b.templates[k]; dup {
		return nil, errExists(templatePath(project, region, in.Name))
	}

	if err := b.normalizePropsLocked(in.Properties); err != nil {
		return nil, err
	}

	t := &computeapi.InstanceTemplate{
		Kind: "compute#instanceTemplate", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name,
		Description: in.Description, Properties: in.Properties,
	}

	if region == "" {
		t.SelfLink = globalLink(project, collTemplates, t.Name)
	} else {
		t.Region = scopeURL(project, scopeRegion, region)
		t.SelfLink = regionLink(project, region, collTemplates, t.Name)
	}

	b.templates[k] = t

	return b.newOpLocked("insert", templateTarget(project, region, t), b.timing.Done), nil
}

func templatePath(project, region, name string) string {
	if region == "" {
		return relPath(project, scopeGlobal, "", collTemplates, name)
	}

	return relPath(project, scopeRegion, region, collTemplates, name)
}

func (b *InMemoryBackend) getTemplate(project, region, name string) (*computeapi.InstanceTemplate, error) {
	b.mu.RLock("GetInstanceTemplate")
	defer b.mu.RUnlock()

	t, ok := b.templates[templateKey(project, region, name)]
	if !ok {
		return nil, errNotFound(templatePath(project, region, name))
	}

	return clone(t), nil
}

func (b *InMemoryBackend) listTemplates(project, scope, loc string) []entry {
	b.mu.RLock("ListInstanceTemplates")
	defer b.mu.RUnlock()

	var out []entry

	for k, t := range b.templates {
		if !strings.HasPrefix(k, project+"/") {
			continue
		}

		tloc := lastSegment(t.Region)
		if scope == scopeGlobal && tloc != "" || scope == scopeRegion && tloc != loc {
			continue
		}

		es := scopeGlobal
		if tloc != "" {
			es = scopeRegion + "/" + tloc
		}

		out = append(out, entry{scope: es, item: clone(t)})
	}

	return out
}

func (b *InMemoryBackend) deleteTemplate(project, region, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteInstanceTemplate")
	defer b.mu.Unlock()

	t, ok := b.templates[templateKey(project, region, name)]
	if !ok {
		return nil, errNotFound(templatePath(project, region, name))
	}

	for _, m := range b.migs {
		if migUsesTemplate(m.Mgr, t.SelfLink) {
			return nil, errInUse("instanceTemplate", trimURL(t.SelfLink), trimURL(m.Mgr.SelfLink))
		}
	}

	delete(b.templates, templateKey(project, region, name))

	return b.newOpLocked("delete", templateTarget(project, region, t), b.timing.Done), nil
}

func (b *InMemoryBackend) resolveTemplateLocked(project, raw, field string) (*computeapi.InstanceTemplate, *APIError) {
	r, ok := parseRef(raw, collTemplates, project, "")
	if !ok || (r.scope != "" && r.scope != scopeGlobal && r.scope != scopeRegion) {
		return nil, errField(field, raw, "The URL is malformed.")
	}

	loc := ""
	if r.scope == scopeRegion {
		loc = r.loc
	}

	t, found := b.templates[templateKey(r.project, loc, r.name)]
	if !found {
		return nil, errNotFound(templatePath(r.project, loc, r.name))
	}

	return t, nil
}

// instanceFromProps builds an instance request from template properties; disk names are cleared so each
// instance gets a disk named after itself.
func instanceFromProps(p *computeapi.InstanceProperties, name string) *computeapi.Instance {
	p = clone(p)
	in := &computeapi.Instance{
		Name: name, Description: p.Description, MachineType: p.MachineType, CanIpForward: p.CanIpForward,
		Labels: p.Labels, Metadata: p.Metadata, MinCpuPlatform: p.MinCpuPlatform, Tags: p.Tags,
		Scheduling: p.Scheduling, ServiceAccounts: p.ServiceAccounts, Disks: p.Disks,
		NetworkInterfaces: p.NetworkInterfaces, GuestAccelerators: p.GuestAccelerators,
		ShieldedInstanceConfig: p.ShieldedInstanceConfig,
	}

	for _, d := range in.Disks {
		if d.InitializeParams != nil {
			d.InitializeParams.DiskName = ""
		}
	}

	return in
}

// instanceFromTemplate merges the request body over the template's properties.
func (b *InMemoryBackend) instanceFromTemplate(project, tmpl string, raw map[string]any) (*computeapi.Instance, error) {
	b.mu.RLock("InstanceFromTemplate")
	defer b.mu.RUnlock()

	t, err := b.resolveTemplateLocked(project, tmpl, "sourceInstanceTemplate")
	if err != nil {
		return nil, err
	}

	name, _ := raw["name"].(string)
	base := toDoc(instanceFromProps(t.Properties, name))
	mergePatch(base, raw)

	out, derr := fromDoc[computeapi.Instance](base)
	if derr != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", derr)
	}

	return out, nil
}
