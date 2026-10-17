package route53recoveryreadiness

import "github.com/blackbirdworks/gopherstack/pkgs/restroute"

// CheckView is the wire form of a readiness check.
type CheckView struct {
	Tags               map[string]string `json:"tags,omitempty"`
	ReadinessCheckArn  string            `json:"readinessCheckArn"`
	ReadinessCheckName string            `json:"readinessCheckName"`
	ResourceSet        string            `json:"resourceSet"`
}

func checkView(c *ReadinessCheck) CheckView {
	return CheckView{
		ReadinessCheckArn: c.ARN, ReadinessCheckName: c.Name, ResourceSet: c.ResourceSet, Tags: cloneTags(c.Tags),
	}
}

// CreateReadinessCheck binds a resource set to a new check.
func (b *InMemoryBackend) CreateReadinessCheck(name, resourceSet string, tags map[string]string) (CheckView, error) {
	if err := validName("readiness check", name); err != nil {
		return CheckView{}, err
	}

	b.mu.Lock("CreateReadinessCheck")
	defer b.mu.Unlock()

	if b.checks.Has(name) {
		return CheckView{}, restroute.Conflict("Readiness check %s already exists", name)
	}

	if _, err := b.getSet(resourceSet); err != nil {
		return CheckView{}, err
	}

	c := &ReadinessCheck{
		ARN:         b.arnFor("readiness-check", name),
		Name:        name,
		ResourceSet: resourceSet,
		Tags:        cloneTags(tags),
	}
	b.checks.Put(c)

	return checkView(c), nil
}

// GetReadinessCheck returns a check.
func (b *InMemoryBackend) GetReadinessCheck(name string) (CheckView, error) {
	b.mu.RLock("GetReadinessCheck")
	defer b.mu.RUnlock()

	c, err := b.getCheck(name)
	if err != nil {
		return CheckView{}, err
	}

	return checkView(c), nil
}

// ListReadinessChecks lists checks.
func (b *InMemoryBackend) ListReadinessChecks(token string, maxResults int) ([]CheckView, string, error) {
	b.mu.RLock("ListReadinessChecks")
	defer b.mu.RUnlock()

	pg, err := restroute.Paginate(sorted(b.checks.All(), func(c *ReadinessCheck) string { return c.Name }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]CheckView, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, checkView(c))
	}

	return out, pg.Next, nil
}

// UpdateReadinessCheck points a check at another resource set.
func (b *InMemoryBackend) UpdateReadinessCheck(name, resourceSet string) (CheckView, error) {
	b.mu.Lock("UpdateReadinessCheck")
	defer b.mu.Unlock()

	c, err := b.getCheck(name)
	if err != nil {
		return CheckView{}, err
	}

	if _, err = b.getSet(resourceSet); err != nil {
		return CheckView{}, err
	}

	c.ResourceSet = resourceSet

	return checkView(c), nil
}

// DeleteReadinessCheck deletes a check.
func (b *InMemoryBackend) DeleteReadinessCheck(name string) error {
	b.mu.Lock("DeleteReadinessCheck")
	defer b.mu.Unlock()

	if _, err := b.getCheck(name); err != nil {
		return err
	}

	b.checks.Delete(name)

	return nil
}
