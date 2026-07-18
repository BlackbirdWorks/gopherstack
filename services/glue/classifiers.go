package glue

// classifierName returns the primary name for a classifier.
func classifierName(c *Classifier) string {
	switch {
	case c.GrokClassifier != nil:
		return c.GrokClassifier.Name
	case c.XMLClassifier != nil:
		return c.XMLClassifier.Name
	case c.JSONClassifier != nil:
		return c.JSONClassifier.Name
	case c.CsvClassifier != nil:
		return c.CsvClassifier.Name
	}

	return ""
}

// CreateClassifier creates a new Glue classifier.
func (b *InMemoryBackend) CreateClassifier(c Classifier) error {
	b.mu.Lock("CreateClassifier")
	defer b.mu.Unlock()

	name := classifierName(&c)
	if name == "" {
		return ErrValidation
	}

	if b.classifiers.Has(name) {
		return ErrAlreadyExists
	}

	cp := c
	b.classifiers.Put(&cp)

	return nil
}

// GetClassifier retrieves a Glue classifier by name.
func (b *InMemoryBackend) GetClassifier(name string) (*Classifier, error) {
	b.mu.RLock("GetClassifier")
	defer b.mu.RUnlock()

	c, ok := b.classifiers.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *c

	return &cp, nil
}

// GetClassifiers returns all Glue classifiers sorted by name.
func (b *InMemoryBackend) GetClassifiers() []*Classifier {
	b.mu.RLock("GetClassifiers")
	defer b.mu.RUnlock()

	src := b.classifiers.Snapshot()
	out := make([]*Classifier, 0, len(src))
	for _, c := range src {
		cp := *c
		out = append(out, &cp)
	}

	return out
}

// UpdateClassifier updates an existing Glue classifier.
func (b *InMemoryBackend) UpdateClassifier(c Classifier) error {
	b.mu.Lock("UpdateClassifier")
	defer b.mu.Unlock()

	name := classifierName(&c)
	if name == "" {
		return ErrValidation
	}

	if !b.classifiers.Has(name) {
		return ErrNotFound
	}

	cp := c
	b.classifiers.Put(&cp)

	return nil
}

// DeleteClassifier deletes a Glue classifier by name.
func (b *InMemoryBackend) DeleteClassifier(name string) error {
	b.mu.Lock("DeleteClassifier")
	defer b.mu.Unlock()

	if !b.classifiers.Has(name) {
		return ErrNotFound
	}

	b.classifiers.Delete(name)

	return nil
}
