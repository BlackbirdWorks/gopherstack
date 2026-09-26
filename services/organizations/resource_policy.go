package organizations

// maxResourcePolicyContentLength is the ResourcePolicyContent shape's max
// (botocore 1.43.56, data/organizations/2016-11-28/service-2.json.gz),
// matching the "Maximum size of the resource-based delegation policy" row of
// docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html
// (40,000 characters). Unlike PolicyContent, this is a hard shape constraint,
// not account-quota state.
const maxResourcePolicyContentLength = 40000

// DeleteResourcePolicy removes the organization resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy() error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return ErrResourcePolicyNotFound
	}

	b.resourcePolicy = nil

	return nil
}

// DescribeResourcePolicy returns the organization resource policy.
func (b *InMemoryBackend) DescribeResourcePolicy() (*ResourcePolicy, error) {
	b.mu.RLock("DescribeResourcePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return nil, ErrResourcePolicyNotFound
	}

	cp := *b.resourcePolicy

	return &cp, nil
}

// PutResourcePolicy creates or replaces the organization resource policy.
// tags is only honored on initial creation (PutResourcePolicyInput doc
// comment: "Calls with tags apply to the initial creation of the resource
// policy, otherwise an exception is thrown" -- a subsequent Put reusing the
// same ID doesn't re-tag, matching real AWS since ListTagsForResource/
// TagResource are the documented way to change tags afterward).
func (b *InMemoryBackend) PutResourcePolicy(content string, tags []Tag) (*ResourcePolicy, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if len(content) > maxResourcePolicyContentLength {
		return nil, ErrPolicyContentLimitExceeded
	}

	rpID := "p-rp-default"
	creating := b.resourcePolicy == nil

	if creating {
		if err := validateNewTags(nil, tags); err != nil {
			return nil, err
		}
	}

	rp := &ResourcePolicy{
		ID:      rpID,
		ARN:     b.resourcePolicyARN(b.org.ID),
		Content: content,
	}

	b.resourcePolicy = rp

	if creating {
		tagMap := make(map[string]string, len(tags))
		for _, t := range tags {
			tagMap[t.Key] = t.Value
		}

		b.tags[rpID] = tagMap
	}

	cp := *rp

	return &cp, nil
}
