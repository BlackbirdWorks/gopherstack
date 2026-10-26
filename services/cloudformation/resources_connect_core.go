package cloudformation

import (
	"maps"
	"slices"
	"strconv"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// connectTypes maps each provisioned AWS::Connect::* type to its operations.
func connectTypes() map[string]connectOps {
	out := connectCoreTypes()
	maps.Copy(out, connectPeopleTypes())
	maps.Copy(out, connectLinkTypes())

	return out
}

func connectCoreTypes() map[string]connectOps {
	return map[string]connectOps{
		resTypeConnectInstance: {
			create: createConnectInstance,
			update: updateConnectInstance,
			del:    deleteConnectInstance,
		},
		resTypeConnectHoursOfOperation: {
			create: createConnectHours,
			update: updateConnectHours,
			del:    deleteConnectHours,
		},
		resTypeConnectQueue: {
			create: createConnectQueue,
			update: updateConnectQueue,
			del:    deleteConnectQueue,
		},
		resTypeConnectRoutingProfile: {
			create: createConnectRoutingProfile, update: updateConnectRoutingProfile, del: deleteConnectRoutingProfile,
		},
		resTypeConnectQuickConnect: {
			create: createConnectQuickConnect,
			update: updateConnectQuickConnect,
			del:    deleteConnectQuickConnect,
		},
		resTypeConnectPrompt: {
			create: createConnectPrompt,
			update: updateConnectPrompt,
			del:    deleteConnectPrompt,
		},
		resTypeConnectAgentStatus: {
			create: createConnectAgentStatus,
			update: updateConnectAgentStatus,
			del:    deleteConnectAgentStatus,
		},
		resTypeConnectContactFlow: {create: createConnectFlow, update: updateConnectFlow, del: deleteConnectFlow},
		resTypeConnectContactFlowModule: {
			create: createConnectFlowModule, update: updateConnectFlowModule, del: deleteConnectFlowModule,
		},
	}
}

func connectPeopleTypes() map[string]connectOps {
	return map[string]connectOps{
		resTypeConnectUser: {create: createConnectUser, update: updateConnectUser, del: deleteConnectUser},
		resTypeConnectSecurityProfile: {
			create: createConnectSecurityProfile,
			update: updateConnectSecurityProfile,
			del:    deleteConnectSecurityProfile,
		},
		resTypeConnectUserHierarchyGroup: {
			create: createConnectHierarchyGroup, update: updateConnectHierarchyGroup, del: deleteConnectHierarchyGroup,
		},
		resTypeConnectPhoneNumber: {
			create: createConnectPhoneNumber,
			update: updateConnectPhoneNumber,
			del:    deleteConnectPhoneNumber,
		},
		resTypeConnectRule:        {create: createConnectRule, update: updateConnectRule, del: deleteConnectRule},
		resTypeConnectView:        {create: createConnectView, update: updateConnectView, del: deleteConnectView},
		resTypeConnectViewVersion: {create: createConnectViewVersion, del: deleteConnectViewVersion},
		resTypeConnectVocabulary: {
			create: createConnectVocabulary,
			update: updateConnectVocabulary,
			del:    deleteConnectVocabulary,
		},
		resTypeConnectEmailAddress: {
			create: createConnectEmailAddress, update: updateConnectEmailAddress, del: deleteConnectEmailAddress,
		},
		resTypeConnectPredefinedAttribute: {
			create: createConnectPredefinedAttribute, update: updateConnectPredefinedAttribute,
			del: deleteConnectPredefinedAttribute,
		},
		resTypeConnectTaskTemplate: {
			create: createConnectTaskTemplate, update: updateConnectTaskTemplate, del: deleteConnectTaskTemplate,
		},
	}
}

func connectLinkTypes() map[string]connectOps {
	return map[string]connectOps{
		resTypeConnectApprovedOrigin: {create: createConnectApprovedOrigin, del: deleteConnectApprovedOrigin},
		resTypeConnectSecurityKey:    {create: createConnectSecurityKey, del: deleteConnectSecurityKey},
		resTypeConnectInstanceStorageConfig: {
			create: createConnectStorageConfig, update: updateConnectStorageConfig, del: deleteConnectStorageConfig,
		},
		resTypeConnectIntegrationAssociation: {
			create: createConnectIntegrationAssociation, update: updateConnectIntegrationAssociation,
			del: deleteConnectIntegrationAssociation,
		},
		resTypeConnectUseCase: {create: createConnectUseCase, update: updateConnectUseCase, del: deleteConnectUseCase},
		resTypeConnectTrafficDistributionGroup: {
			create: createConnectTrafficDistributionGroup, update: updateConnectTrafficDistributionGroup,
			del: deleteConnectTrafficDistributionGroup,
		},
		resTypeConnectWorkspace: {
			create: createConnectWorkspace,
			update: updateConnectWorkspace,
			del:    deleteConnectWorkspace,
		},
		resTypeConnectEvaluationForm: {
			create: createConnectEvaluationForm, update: updateConnectEvaluationForm, del: deleteConnectEvaluationForm,
		},
		resTypeConnectDataTable: {
			create: createConnectDataTable,
			update: updateConnectDataTable,
			del:    deleteConnectDataTable,
		},
		resTypeConnectDataTableAttribute: {
			create: createConnectDataTableAttribute, update: updateConnectDataTableAttribute,
			del: deleteConnectDataTableAttribute,
		},
	}
}

// connectImmutableProps lists, per type, the properties whose change needs replacement.
func connectImmutableProps() map[string][]string {
	return map[string][]string{
		resTypeConnectInstance:                 {"DirectoryId", "IdentityManagementType", "InstanceAlias"},
		resTypeConnectContactFlow:              {samKeyType},
		resTypeConnectSecurityProfile:          {"SecurityProfileName", connectInstanceArnProp},
		resTypeConnectUserHierarchyGroup:       {"ParentGroupArn"},
		resTypeConnectPhoneNumber:              {samKeyType, "Prefix", "CountryCode", "SourcePhoneNumberArn"},
		resTypeConnectRule:                     {"TriggerEventSource", connectInstanceArnProp},
		resTypeConnectVocabulary:               {"LanguageCode", "VocabularyName", "InstanceId", "Content"},
		resTypeConnectEmailAddress:             {"EmailAddress"},
		resTypeConnectPredefinedAttribute:      {connectInstanceArnProp, attrNameName},
		resTypeConnectViewVersion:              {"ViewArn", connectViewSha, "VersionDescription"},
		resTypeConnectInstanceStorageConfig:    {connectInstanceArnProp, "ResourceType"},
		resTypeConnectTrafficDistributionGroup: {attrNameName, "Description"},
		resTypeConnectWorkspace:                {connectInstanceArnProp},
		resTypeConnectDataTable:                {connectStatusProp, connectInstanceArnProp},
		resTypeConnectDataTableAttribute:       {connectInstanceArnProp, "DataTableArn", attrNameName},
	}
}

// connectTagsOnlyUpdate lists the types of which only Tags can change in place.
func connectTagsOnlyUpdate() []string {
	return []string{
		resTypeConnectApprovedOrigin, resTypeConnectSecurityKey, resTypeConnectIntegrationAssociation,
		resTypeConnectUseCase,
	}
}

func connectRecreation(resourceType, propName string) string {
	if slices.Contains(connectTagsOnlyUpdate(), resourceType) && propName != connectTagsProp {
		return recreateAlways
	}

	return alwaysIfOneOf(propName, connectImmutableProps()[resourceType]...)
}

// ---- Instance ----

func connectInstanceAttributeNames() map[string]string {
	return map[string]string{
		"InboundCalls":          "INBOUND_CALLS",
		"OutboundCalls":         "OUTBOUND_CALLS",
		"ContactflowLogs":       "CONTACTFLOW_LOGS",
		"ContactLens":           "CONTACT_LENS",
		"AutoResolveBestVoices": "AUTO_RESOLVE_BEST_VOICES",
		"UseCustomTTSVoices":    "USE_CUSTOM_TTS_VOICES",
		"EarlyMedia":            "EARLY_MEDIA",
	}
}

// createConnectInstance provisions an AWS::Connect::Instance. Ref and GetAtt Arn return the ARN.
func createConnectInstance(c *connectCall) (string, error) {
	var in connectbackend.CreateInstanceInput
	if err := c.decode(&in, "Attributes"); err != nil {
		return "", err
	}

	attrs, _ := c.wire["Attributes"].(map[string]any)
	in.InboundCallsEnabled = optionalBool(attrs["InboundCalls"], nil, nil)
	in.OutboundCallsEnabled = optionalBool(attrs["OutboundCalls"], nil, nil)

	inst, err := c.be.CreateInstance(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if err = applyConnectInstanceAttributes(c, inst.ID); err != nil {
		_ = c.be.DeleteInstance(c.ctx, inst.ID)

		return "", err
	}

	c.stash(map[string]string{
		"ServiceRole":    inst.ServiceRole,
		"InstanceStatus": inst.InstanceStatus,
		"CreatedTime":    connectTime(&inst.CreatedTime),
	})

	return inst.Arn, nil
}

func applyConnectInstanceAttributes(c *connectCall, instanceID string) error {
	attrs, _ := c.wire["Attributes"].(map[string]any)

	for _, cfn := range slices.Sorted(maps.Keys(connectInstanceAttributeNames())) {
		v, set := attrs[cfn]
		if !set {
			continue
		}

		enabled := optionalBool(v, nil, nil)
		if enabled == nil {
			continue
		}

		value := strconv.FormatBool(*enabled)

		if err := c.be.UpdateInstanceAttribute(instanceID, connectInstanceAttributeNames()[cfn], value); err != nil {
			return err
		}
	}

	return nil
}

func updateConnectInstance(c *connectCall, physicalID string) error {
	if err := applyConnectInstanceAttributes(c, physicalID); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectInstance(c *connectCall, physicalID string) error {
	return c.be.DeleteInstance(c.ctx, physicalID)
}

// ---- HoursOfOperation ----

func createConnectHours(c *connectCall) (string, error) {
	if err := c.reject("HoursOfOperationOverrides", "ParentHoursOfOperations", "ChildHoursOfOperations"); err != nil {
		return "", err
	}

	var in connectbackend.CreateHoursOfOperationInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateHoursOfOperation(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.Arn, nil
}

func updateConnectHours(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateHoursOfOperationInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.HoursID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateHoursOfOperation(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectHours(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteHoursOfOperation(inst, id)

	return err
}

// ---- Queue ----

func createConnectQueue(c *connectCall) (string, error) {
	if err := c.reject("AdditionalEmailAddresses"); err != nil {
		return "", err
	}

	var in connectbackend.CreateQueueInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateQueue(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if c.str(connectStatusProp) == samStateDisabled {
		status := samStateDisabled

		_, err = c.be.UpdateQueueStatus(c.ctx, &connectbackend.UpdateQueueInput{
			Status: &status, QueueID: out.QueueID,
		})
		if err != nil {
			return "", err
		}
	}

	inst, id := splitConnectARN(out.QueueArn)
	if q, derr := c.be.DescribeQueue(inst, id); derr == nil {
		c.stash(map[string]string{
			samKeyType:           "STANDARD",
			connectLastModified:  connectTime(q.LastModifiedTime),
			connectLastModRegion: q.LastModifiedRegion,
		})
	}

	return out.QueueArn, nil
}

func updateConnectQueue(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)

	var in connectbackend.UpdateQueueInput
	if err := c.decode(&in, connectTagsProp, connectStatusProp); err != nil {
		return err
	}

	in.QueueID = id
	in.InstanceID = inst
	in.Description = new(c.str("Description"))

	status := c.str(connectStatusProp)
	if status == "" {
		status = samStateEnabled
	}

	in.Status = &status

	if _, err := c.be.UpdateQueueName(c.ctx, &in); err != nil {
		return err
	}

	if _, err := c.be.UpdateQueueStatus(c.ctx, &in); err != nil {
		return err
	}

	return updateConnectQueueSettings(c, &in, physicalID)
}

func updateConnectQueueSettings(c *connectCall, in *connectbackend.UpdateQueueInput, physicalID string) error {
	if in.MaxContacts != nil {
		if _, err := c.be.UpdateQueueMaxContacts(c.ctx, in); err != nil {
			return err
		}
	}

	if in.HoursOfOperationID != nil {
		if _, err := c.be.UpdateQueueHoursOfOperation(c.ctx, in); err != nil {
			return err
		}
	}

	if in.OutboundCallerConfig != nil {
		if _, err := c.be.UpdateQueueOutboundCallerConfig(c.ctx, in); err != nil {
			return err
		}
	}

	if in.OutboundEmailConfig != nil {
		if _, err := c.be.UpdateQueueOutboundEmailConfig(c.ctx, in); err != nil {
			return err
		}
	}

	if err := updateConnectQueueQuickConnects(c, in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func updateConnectQueueQuickConnects(c *connectCall, in *connectbackend.UpdateQueueInput) error {
	var want connectbackend.QuickConnectIDsInput
	if err := c.decode(&want, connectTagsProp); err != nil {
		return err
	}

	have, _, err := c.be.ListQueueQuickConnects(in.InstanceID, in.QueueID, "", 0)
	if err != nil {
		return err
	}

	var haveIDs, drop []string
	for _, h := range have {
		haveIDs = append(haveIDs, h.ID)

		if !slices.Contains(want.QuickConnectIDs, h.ID) {
			drop = append(drop, h.ID)
		}
	}

	add := slices.DeleteFunc(
		slices.Clone(want.QuickConnectIDs),
		func(id string) bool { return slices.Contains(haveIDs, id) },
	)
	var ref connectbackend.QuickConnectIDsInput

	ref.QueueID, ref.InstanceID = in.QueueID, in.InstanceID

	if len(drop) > 0 {
		ref.QuickConnectIDs = drop
		if _, err = c.be.DisassociateQueueQuickConnects(&ref); err != nil {
			return err
		}
	}

	if len(add) > 0 {
		ref.QuickConnectIDs = add
		_, err = c.be.AssociateQueueQuickConnects(&ref)
	}

	return err
}

func deleteConnectQueue(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteQueue(inst, id)

	return err
}

// ---- RoutingProfile ----

func createConnectRoutingProfile(c *connectCall) (string, error) {
	if err := c.reject("ManualAssignmentQueueConfigs"); err != nil {
		return "", err
	}

	var in connectbackend.CreateRoutingProfileInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateRoutingProfile(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.RoutingProfileArn, nil
}

func updateConnectRoutingProfile(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)

	var in connectbackend.UpdateRoutingProfileInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.ProfileID = id
	in.InstanceID = inst
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateRoutingProfileName(c.ctx, &in); err != nil {
		return err
	}

	if _, err := c.be.UpdateRoutingProfileConcurrency(c.ctx, &in); err != nil {
		return err
	}

	if in.DefaultOutboundQueueID != nil {
		if _, err := c.be.UpdateRoutingProfileDefaultOutboundQueue(c.ctx, &in); err != nil {
			return err
		}
	}

	if in.AgentAvailabilityTimer != nil {
		if _, err := c.be.UpdateRoutingProfileAgentAvailabilityTimer(c.ctx, &in); err != nil {
			return err
		}
	}

	if err := syncConnectProfileQueues(c, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

// syncConnectProfileQueues makes the profile's queue configs equal QueueConfigs.
func syncConnectProfileQueues(c *connectCall, in *connectbackend.UpdateRoutingProfileInput) error {
	have, _, err := c.be.ListRoutingProfileQueues(in.InstanceID, in.ProfileID, "", 0)
	if err != nil {
		return err
	}

	haveRefs := make(map[connectbackend.QueueReference]connectbackend.RoutingProfileQueueConfig, len(have))
	for _, h := range have {
		ref := connectbackend.QueueReference{QueueID: h.QueueID, Channel: h.Channel}
		haveRefs[ref] = connectbackend.RoutingProfileQueueConfig{
			QueueReference: ref,
			Delay:          h.Delay,
			Priority:       h.Priority,
		}
	}

	var add, change []connectbackend.RoutingProfileQueueConfig

	wantRefs := make(map[connectbackend.QueueReference]bool, len(in.QueueConfigs))

	for _, w := range in.QueueConfigs {
		wantRefs[w.QueueReference] = true

		cur, exists := haveRefs[w.QueueReference]

		switch {
		case !exists:
			add = append(add, w)
		case cur != w:
			change = append(change, w)
		}
	}

	var drop []connectbackend.QueueReference

	for ref := range haveRefs {
		if !wantRefs[ref] {
			drop = append(drop, ref)
		}
	}

	return applyConnectProfileQueues(c, in, add, change, drop)
}

func applyConnectProfileQueues(
	c *connectCall,
	in *connectbackend.UpdateRoutingProfileInput,
	add, change []connectbackend.RoutingProfileQueueConfig,
	drop []connectbackend.QueueReference,
) error {
	var ref connectbackend.UpdateRoutingProfileInput

	ref.ProfileID, ref.InstanceID = in.ProfileID, in.InstanceID

	if len(drop) > 0 {
		ref.QueueReferences = drop
		if _, err := c.be.DisassociateRoutingProfileQueues(c.ctx, &ref); err != nil {
			return err
		}
	}

	if len(add) > 0 {
		ref.QueueConfigs = add
		if _, err := c.be.AssociateRoutingProfileQueues(c.ctx, &ref); err != nil {
			return err
		}
	}

	if len(change) > 0 {
		ref.QueueConfigs = change
		if _, err := c.be.UpdateRoutingProfileQueues(c.ctx, &ref); err != nil {
			return err
		}
	}

	return nil
}

func deleteConnectRoutingProfile(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteRoutingProfile(inst, id)

	return err
}

// ---- QuickConnect ----

func createConnectQuickConnect(c *connectCall) (string, error) {
	var in connectbackend.CreateQuickConnectInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateQuickConnect(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{"QuickConnectType": in.QuickConnectConfig.QuickConnectType})

	return out.QuickConnectARN, nil
}

func updateConnectQuickConnect(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateQuickConnectInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.QuickConnectID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateQuickConnectName(c.ctx, &in); err != nil {
		return err
	}

	if in.QuickConnectConfig != nil {
		if _, err := c.be.UpdateQuickConnectConfig(c.ctx, &in); err != nil {
			return err
		}

		c.stash(map[string]string{"QuickConnectType": in.QuickConnectConfig.QuickConnectType})
	}

	return c.syncTags(physicalID)
}

func deleteConnectQuickConnect(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteQuickConnect(inst, id)

	return err
}

// ---- Prompt ----

func createConnectPrompt(c *connectCall) (string, error) {
	var in connectbackend.CreatePromptInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreatePrompt(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.PromptARN, nil
}

func updateConnectPrompt(c *connectCall, physicalID string) error {
	var in connectbackend.UpdatePromptInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.PromptID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdatePrompt(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectPrompt(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeletePrompt(inst, id)

	return err
}

// ---- AgentStatus ----

func createConnectAgentStatus(c *connectCall) (string, error) {
	var in connectbackend.CreateAgentStatusInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateAgentStatus(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashAgentStatus(out.AgentStatusARN)

	return out.AgentStatusARN, nil
}

func (c *connectCall) stashAgentStatus(statusARN string) {
	inst, id := splitConnectARN(statusARN)

	if s, err := c.be.DescribeAgentStatus(inst, id); err == nil {
		c.stash(map[string]string{
			connectLastModified:  connectTime(s.LastModifiedTime),
			connectLastModRegion: s.LastModifiedRegion,
		})
	}
}

func updateConnectAgentStatus(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateAgentStatusInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.AgentStatusID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateAgentStatus(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

// deleteConnectAgentStatus leaves the status in place: Amazon Connect has no DeleteAgentStatus, so it goes
// away with its instance.
func deleteConnectAgentStatus(*connectCall, string) error { return nil }

// ---- ContactFlow ----

func createConnectFlow(c *connectCall) (string, error) {
	var in connectbackend.CreateContactFlowInput
	if err := c.decode(&in, "State"); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateContactFlow(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if state := c.str("State"); state != "" && state != connectStateActive {
		var upd connectbackend.UpdateContactFlowInput

		upd.FlowID, upd.InstanceID = out.ContactFlowID, in.InstanceID

		if _, err = c.be.UpdateContactFlowMetadata(c.ctx, &upd); err != nil {
			return "", err
		}
	}

	return out.ContactFlowArn, nil
}

func updateConnectFlow(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateContactFlowInput
	if err := c.decode(&in, connectTagsProp, "State"); err != nil {
		return err
	}

	in.InstanceID, in.FlowID = splitConnectARN(physicalID)

	if _, err := c.be.UpdateContactFlowContent(c.ctx, &in); err != nil {
		return err
	}

	in.Content = nil
	in.Description = new(c.str("Description"))

	if state := c.str("State"); state != "" {
		in.ContactFlowState = &state
	}

	if _, err := c.be.UpdateContactFlowMetadata(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectFlow(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteContactFlow(inst, id)

	return err
}

// ---- ContactFlowModule ----

func createConnectFlowModule(c *connectCall) (string, error) {
	var in connectbackend.CreateContactFlowModuleInput
	if err := c.decode(&in, "State"); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateContactFlowModule(c.ctx, &in)
	if err != nil {
		return "", err
	}

	inst, id := splitConnectARN(out.Arn)
	if state := c.str("State"); state != "" && state != connectStateActive {
		var upd connectbackend.UpdateContactFlowModuleInput

		upd.ModuleID, upd.State, upd.InstanceID = id, &state, inst

		if _, err = c.be.UpdateContactFlowModuleMetadata(c.ctx, &upd); err != nil {
			return "", err
		}
	}

	if m, derr := c.be.DescribeContactFlowModule(inst, id); derr == nil {
		c.stash(map[string]string{connectStatusProp: m.Status})
	}

	return out.Arn, nil
}

func updateConnectFlowModule(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateContactFlowModuleInput
	if err := c.decode(&in, connectTagsProp, "State"); err != nil {
		return err
	}

	in.InstanceID, in.ModuleID = splitConnectARN(physicalID)

	if _, err := c.be.UpdateContactFlowModuleContent(c.ctx, &in); err != nil {
		return err
	}

	in.Content = nil
	in.Description = new(c.str("Description"))

	if state := c.str("State"); state != "" {
		in.State = &state
	}

	if _, err := c.be.UpdateContactFlowModuleMetadata(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectFlowModule(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteContactFlowModule(inst, id)

	return err
}
