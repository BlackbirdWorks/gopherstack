package cleanrooms

func toSchemaSummary(s *Schema) *SchemaSummary {
	return &SchemaSummary{
		CollaborationArn:        s.CollaborationArn,
		CollaborationIdentifier: s.CollaborationIdentifier,
		CreatorAccountID:        s.CreatorAccountID,
		Name:                    s.Name,
		Type:                    s.Type,
		AnalysisRuleTypes:       s.AnalysisRuleTypes,
		AnalysisMethod:          s.AnalysisMethod,
		CreateTime:              s.CreateTime,
		UpdateTime:              s.UpdateTime,
	}
}

func (b *InMemoryBackend) GetSchema(collaborationID, name string) (*Schema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()
	s, ok := b.schemas.Get(collaborationKey(collaborationID, name))
	if !ok {
		return nil, ErrNotFound
	}

	return s, nil
}

func (b *InMemoryBackend) ListSchemas(
	collaborationID, schemaType, maxResults, nextToken string,
) ([]*SchemaSummary, string, error) {
	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.schemasByCollaboration.Get(collaborationID),
		func(s *Schema) bool { return schemaType == "" || s.Type == schemaType },
		toSchemaSummary,
		func(a, c *SchemaSummary) bool { return a.Name < c.Name },
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) BatchGetSchema(
	collaborationID string,
	names []string,
) ([]*Schema, []BatchError, error) {
	b.mu.RLock("BatchGetSchema")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	var results []*Schema
	var errors []BatchError
	for _, name := range names {
		s, ok := b.schemas.Get(collaborationKey(collaborationID, name))
		if ok {
			results = append(results, s)
		} else {
			errors = append(errors, BatchError{Name: name, Code: errCodeNotFound, Message: errMsgNotFound})
		}
	}

	return results, errors, nil
}

func (b *InMemoryBackend) GetSchemaAnalysisRule(
	collaborationID, name, ruleType string,
) (*SchemaAnalysisRule, error) {
	b.mu.RLock("GetSchemaAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.schemaAnalysisRules.Get(schemaAnalysisRuleKey(collaborationID, name, ruleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) BatchGetSchemaAnalysisRule(
	collaborationID string,
	names []string,
	ruleType string,
) ([]*SchemaAnalysisRule, []BatchError, error) {
	b.mu.RLock("BatchGetSchemaAnalysisRule")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	var results []*SchemaAnalysisRule
	var errors []BatchError
	for _, name := range names {
		rule, ok := b.schemaAnalysisRules.Get(schemaAnalysisRuleKey(collaborationID, name, ruleType))
		if ok {
			results = append(results, rule)

			continue
		}
		errors = append(
			errors,
			BatchError{Name: name, Code: errCodeNotFound, Message: errMsgNotFound},
		)
	}

	return results, errors, nil
}
