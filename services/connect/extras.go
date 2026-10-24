package connect

func (b *Backend) registerVersionTables() {
	r := b.registry
	b.flowVersions = newFamily(r, "flowVersions", "Version",
		func(v *flowVersionRec) string { return v.InstanceID },
		func(v *flowVersionRec) string { return versionKey(v.Kind, v.ParentID, v.Version) })
	b.flowAliases = newFamily(r, "flowAliases", "Alias",
		func(v *flowAliasRec) string { return v.InstanceID }, func(v *flowAliasRec) string { return v.AliasID })
}

func (b *Backend) registerViewTables() {
	r := b.registry
	b.views = newFamily(r, "views", "View",
		func(v *viewRec) string { return v.InstanceID }, func(v *viewRec) string { return v.View.ID })
	b.viewSnapshots = newFamily(r, "viewVersions", "ViewVersion",
		func(v *viewVersionRec) string { return v.InstanceID },
		func(v *viewVersionRec) string { return snapshotKey(v.View.ID, v.View.Version) })
	b.evalForms = newFamily(r, "evaluationForms", "EvaluationForm",
		func(v *evalFormRec) string { return v.InstanceID }, func(v *evalFormRec) string { return v.ID })
	b.dataTables = newFamily(r, "dataTables", dtLabelTable,
		func(v *dataTableRec) string { return v.InstanceID }, func(v *dataTableRec) string { return v.Table.ID })
	b.dataTables.taggable(
		func(v *dataTableRec) string { return v.Table.Arn },
		func(v *dataTableRec) *map[string]string { return &v.Table.Tags })
	b.emails = newFamily(r, "emailAddresses", labelEmailAddress,
		func(v *emailAddressRec) string { return v.InstanceID }, func(v *emailAddressRec) string { return v.ID })
	b.emails.taggable(
		func(v *emailAddressRec) string { return v.Arn },
		func(v *emailAddressRec) *map[string]string { return &v.Tags })
	b.evalForms.taggable(
		func(v *evalFormRec) string { return v.Arn }, func(v *evalFormRec) *map[string]string { return &v.Tags })
	b.trafficGroups = newFamily(r, "trafficDistributionGroups", "TrafficDistributionGroup",
		func(v *tdgRec) string { return v.InstanceID }, func(v *tdgRec) string { return v.ID })
	b.trafficGroups.taggable(
		func(v *tdgRec) string { return v.Arn }, func(v *tdgRec) *map[string]string { return &v.Tags })
	b.flowAssocs = newFamily(r, "flowAssociations", "FlowAssociation",
		func(v *flowAssocRec) string { return v.InstanceID },
		func(v *flowAssocRec) string { return assocKey(v.ResourceType, v.ResourceID) })
	b.entityProfileRecs = newFamily(
		r,
		"entityProfiles",
		"Entity",
		func(v *entityProfilesRec) string { return v.InstanceID },
		func(v *entityProfilesRec) string { return v.EntityARN },
	)
	b.views.taggable(
		func(v *viewRec) string { return v.View.Arn }, func(v *viewRec) *map[string]string { return &v.View.Tags })
}

func (b *Backend) registerExtras() {
	b.registerVersionTables()
	b.registerViewTables()

	r := b.registry
	b.rules = newFamily(r, "rules", "Rule",
		func(v *ruleRec) string { return v.InstanceID }, func(v *ruleRec) string { return v.Rule.RuleID })
	b.taskTemplates = newFamily(
		r,
		"taskTemplates",
		"TaskTemplate",
		func(v *taskTemplateRec) string { return v.InstanceID },
		func(v *taskTemplateRec) string { return v.Template.ID },
	)
	b.predefined = newFamily(
		r,
		"predefinedAttributes",
		"PredefinedAttribute",
		func(v *predefinedRec) string { return v.InstanceID },
		func(v *predefinedRec) string { return v.Attribute.Name },
	)
	b.hoursOverrides = newFamily(r, "hoursOverrides", "HoursOfOperationOverride",
		func(v *hoursOverrideRec) string { return v.InstanceID },
		func(v *hoursOverrideRec) string { return v.Override.HoursOfOperationOverrideID })
	b.vocabularies = newFamily(r, "vocabularies", "Vocabulary",
		func(v *vocabularyRec) string { return v.InstanceID }, func(v *vocabularyRec) string { return v.Vocabulary.ID })
	b.defaultVocabs = newFamily(
		r,
		"defaultVocabularies",
		"DefaultVocabulary",
		func(v *defaultVocabRec) string { return v.InstanceID },
		func(v *defaultVocabRec) string { return v.LanguageCode },
	)

	b.rules.taggable(
		func(v *ruleRec) string { return v.Rule.RuleArn }, func(v *ruleRec) *map[string]string { return &v.Rule.Tags })
	b.taskTemplates.taggable(
		func(v *taskTemplateRec) string { return v.Template.Arn },
		func(v *taskTemplateRec) *map[string]string { return &v.Template.Tags })
	b.vocabularies.taggable(
		func(v *vocabularyRec) string { return v.Vocabulary.Arn },
		func(v *vocabularyRec) *map[string]string { return &v.Vocabulary.Tags })
}

// extraFinders are the tag lookups of the extras families.
func (b *Backend) extraFinders() []func(string) *map[string]string {
	return []func(string) *map[string]string{
		b.rules.findARN,
		b.taskTemplates.findARN,
		b.vocabularies.findARN,
		b.views.findARN,
		b.evalForms.findARN,
		b.dataTables.findARN,
		b.emails.findARN,
		b.trafficGroups.findARN,
	}
}

func (b *Backend) dropExtras(inst string) {
	b.rules.dropInstance(inst)
	b.taskTemplates.dropInstance(inst)
	b.predefined.dropInstance(inst)
	b.hoursOverrides.dropInstance(inst)
	b.vocabularies.dropInstance(inst)
	b.defaultVocabs.dropInstance(inst)
	b.flowVersions.dropInstance(inst)
	b.flowAliases.dropInstance(inst)
	b.views.dropInstance(inst)
	b.flowAssocs.dropInstance(inst)
	b.entityProfileRecs.dropInstance(inst)
	b.trafficGroups.dropInstance(inst)
	b.evalForms.dropInstance(inst)
	b.dataTables.dropInstance(inst)
	b.emails.dropInstance(inst)
	b.viewSnapshots.dropInstance(inst)
}
