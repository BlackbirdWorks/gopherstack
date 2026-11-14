package azurearm

import "maps"

func (p *ContainerRegistryProvider) syncAnonymousPull(rec *storedACR) error {
	on, _ := rec.extra["anonymousPullEnabled"].(bool)

	if err := p.dataPlane.SetAnonymousPull(rec.name, on); err != nil {
		return badACRRequest(err)
	}

	return nil
}

// patchChild merges a webhook or replication update body into the stored child; omitted fields stay.
func (p *ContainerRegistryProvider) patchChild(
	id ResourceID,
	kind acrKind,
	body map[string]any,
) (map[string]any, error) {
	parent, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	rec, ok := p.child(id, kind)
	if !ok {
		return nil, acrNotFound(kind)
	}

	merged := maps.Clone(rec.props)
	maps.Copy(merged, mapOf(body[fieldProperties]))

	props, err := childProps(kind, id, parent, map[string]any{fieldProperties: merged})
	if err != nil {
		return nil, badACRRequest(err)
	}

	next := *rec
	next.props = props

	if v, has := body[fieldTags]; has {
		next.tags = stringTags(v)
	}

	if kind == acrKindWebhook {
		if err = p.dataPlane.SetWebhook(parent.name, webhookOf(&next)); err != nil {
			return nil, badACRRequest(err)
		}
	}

	*rec = next

	return p.childBody(id, rec), nil
}
