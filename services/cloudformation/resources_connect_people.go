package cloudformation

import (
	"encoding/json"
	"fmt"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// ---- User ----

func createConnectUser(c *connectCall) (string, error) {
	var in connectbackend.CreateUserInput
	if err := c.decode(&in, "UserProficiencies"); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateUser(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if err = c.associateProficiencies(out.UserArn); err != nil {
		_, _ = c.be.DeleteUser(splitConnectARN(out.UserArn))

		return "", err
	}

	return out.UserArn, nil
}

func (c *connectCall) associateProficiencies(userARN string) error {
	var in connectbackend.ProficienciesInput
	if err := c.decode(&in); err != nil {
		return err
	}

	if len(in.UserProficiencies) == 0 {
		return nil
	}

	in.InstanceID, in.UserID = splitConnectARN(userARN)
	_, err := c.be.AssociateUserProficiencies(c.ctx, &in)

	return err
}

func updateConnectUser(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateUserInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.UserID = splitConnectARN(physicalID)

	if in.IdentityInfo != nil {
		if _, err := c.be.UpdateUserIdentityInfo(c.ctx, &in); err != nil {
			return err
		}
	}

	if in.PhoneConfig != nil {
		if _, err := c.be.UpdateUserPhoneConfig(c.ctx, &in); err != nil {
			return err
		}
	}

	if err := updateConnectUserLinks(c, &in); err != nil {
		return err
	}

	if err := updateConnectUserConfig(c, in.InstanceID, in.UserID); err != nil {
		return err
	}

	if err := replaceProficiencies(c, in.InstanceID, in.UserID); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func updateConnectUserLinks(c *connectCall, in *connectbackend.UpdateUserInput) error {
	if _, err := c.be.UpdateUserRoutingProfile(c.ctx, in); err != nil {
		return err
	}

	if _, err := c.be.UpdateUserSecurityProfiles(c.ctx, in); err != nil {
		return err
	}

	_, err := c.be.UpdateUserHierarchy(c.ctx, in)

	return err
}

func updateConnectUserConfig(c *connectCall, inst, userID string) error {
	var cfg connectbackend.UpdateUserConfigInput
	if err := c.decode(&cfg, connectTagsProp); err != nil {
		return err
	}

	cfg.InstanceID, cfg.UserID = inst, userID
	_, err := c.be.UpdateUserConfig(c.ctx, &cfg)

	return err
}

// replaceProficiencies makes the user's proficiencies equal UserProficiencies.
func replaceProficiencies(c *connectCall, inst, userID string) error {
	var want connectbackend.ProficienciesInput
	if err := c.decode(&want); err != nil {
		return err
	}

	have := connectbackend.ProficienciesInput{}
	have.InstanceID, have.UserID = inst, userID

	cur, err := c.be.ListUserProficiencies(&have)
	if err != nil {
		return err
	}

	if len(cur.UserProficiencyList) > 0 {
		var drop connectbackend.DisassociateProficienciesInput

		drop.UserID, drop.InstanceID = userID, inst

		for _, p := range cur.UserProficiencyList {
			drop.UserProficiencies = append(drop.UserProficiencies, connectbackend.UserProficiencyDisassociate{
				AttributeName: p.AttributeName, AttributeValue: p.AttributeValue,
			})
		}

		if _, err = c.be.DisassociateUserProficiencies(c.ctx, &drop); err != nil {
			return err
		}
	}

	if len(want.UserProficiencies) == 0 {
		return nil
	}

	want.InstanceID, want.UserID = inst, userID
	_, err = c.be.AssociateUserProficiencies(c.ctx, &want)

	return err
}

func deleteConnectUser(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteUser(inst, id)

	return err
}

// ---- SecurityProfile ----

func (c *connectCall) accessControlTags() {
	if list, ok := c.wire["AllowedAccessControlTags"].([]any); ok {
		c.wire["AllowedAccessControlTags"] = tagListFromSlice(list, nil, nil)
	}
}

func createConnectSecurityProfile(c *connectCall) (string, error) {
	c.accessControlTags()

	var in connectbackend.CreateSecurityProfileInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateSecurityProfile(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashSecurityProfile(out.SecurityProfileArn)

	return out.SecurityProfileArn, nil
}

func (c *connectCall) stashSecurityProfile(profileARN string) {
	inst, id := splitConnectARN(profileARN)

	if p, err := c.be.DescribeSecurityProfile(inst, id); err == nil {
		c.stash(map[string]string{
			connectLastModified:  connectTime(p.LastModifiedTime),
			connectLastModRegion: p.LastModifiedRegion,
		})
	}
}

func updateConnectSecurityProfile(c *connectCall, physicalID string) error {
	c.accessControlTags()

	var in connectbackend.UpdateSecurityProfileInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.ProfileID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateSecurityProfile(c.ctx, &in); err != nil {
		return err
	}

	c.stashSecurityProfile(physicalID)

	return c.syncTags(physicalID)
}

func deleteConnectSecurityProfile(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteSecurityProfile(inst, id)

	return err
}

// ---- UserHierarchyGroup ----

func createConnectHierarchyGroup(c *connectCall) (string, error) {
	var in connectbackend.CreateUserHierarchyGroupInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateUserHierarchyGroup(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.HierarchyGroupArn, nil
}

func updateConnectHierarchyGroup(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateHierarchyGroupNameInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.GroupID = splitConnectARN(physicalID)

	if _, err := c.be.UpdateUserHierarchyGroupName(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectHierarchyGroup(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteUserHierarchyGroup(inst, id)

	return err
}

// ---- PhoneNumber ----

// createConnectPhoneNumber searches the number pool for the requested country, type and prefix and claims the
// first match. Ref and GetAtt PhoneNumberArn return the ARN; GetAtt Address the E.164 number.
func createConnectPhoneNumber(c *connectCall) (string, error) {
	if err := c.reject("SourcePhoneNumberArn"); err != nil {
		return "", err
	}

	found, err := c.be.SearchAvailablePhoneNumbers(&connectbackend.SearchAvailablePhoneNumbersInput{
		PhoneNumberCountryCode: c.str("CountryCode"),
		PhoneNumberType:        c.str(samKeyType),
		PhoneNumberPrefix:      c.str("Prefix"),
		TargetArn:              c.str("TargetArn"),
		MaxResults:             1,
	})
	if err != nil {
		return "", err
	}

	if len(found.AvailableNumbersList) == 0 {
		return "", fmt.Errorf("%w: no %s number with prefix %q is available in %s",
			errUnsupportedCFNValue, c.str(samKeyType), c.str("Prefix"), c.str("CountryCode"))
	}

	number := found.AvailableNumbersList[0].PhoneNumber

	out, err := c.be.ClaimPhoneNumber(c.ctx, &connectbackend.ClaimPhoneNumberInput{
		PhoneNumber:            number,
		TargetArn:              c.str("TargetArn"),
		PhoneNumberDescription: c.str("Description"),
		Tags:                   c.tags(),
	})
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{"Address": number})

	return out.PhoneNumberArn, nil
}

func updateConnectPhoneNumber(c *connectCall, physicalID string) error {
	id := lastPathSegment(physicalID)

	if _, err := c.be.UpdatePhoneNumber(&connectbackend.UpdatePhoneNumberInput{
		PhoneNumberID: id, TargetArn: c.str("TargetArn"),
	}); err != nil {
		return err
	}

	if _, err := c.be.UpdatePhoneNumberMetadata(&connectbackend.UpdatePhoneNumberInput{
		PhoneNumberID: id, PhoneNumberDescription: new(c.str("Description")),
	}); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectPhoneNumber(c *connectCall, physicalID string) error {
	_, err := c.be.ReleasePhoneNumber(lastPathSegment(physicalID))

	return err
}

// ---- Rule ----

func connectRuleActionMembers() map[string][2]string {
	return map[string][2]string{
		"TaskActions":                  {"CREATE_TASK", "TaskAction"},
		"AssignContactCategoryActions": {"ASSIGN_CONTACT_CATEGORY", "AssignContactCategoryAction"},
		"EventBridgeActions":           {"GENERATE_EVENTBRIDGE_EVENT", "EventBridgeAction"},
		"SendNotificationActions":      {"SEND_NOTIFICATION", "SendNotificationAction"},
		"CreateCaseActions":            {"CREATE_CASE", "CreateCaseAction"},
		"UpdateCaseActions":            {"UPDATE_CASE", "UpdateCaseAction"},
		"EndAssociatedTasksActions":    {"END_ASSOCIATED_TASKS", "EndAssociatedTasksAction"},
		"SubmitAutoEvaluationActions":  {"SUBMIT_AUTO_EVALUATION", "SubmitAutoEvaluationAction"},
		"AssignSlaActions":             {"ASSIGN_SLA", "AssignSlaAction"},
		"ExtractInformationActions":    {"EXTRACT_INFORMATION", "ExtractInformationAction"},
		"SendInAppNotificationActions": {"SEND_IN_APP_NOTIFICATION", "SendInAppNotificationAction"},
	}
}

// ruleActions turns the template's Actions object (one list per action kind) into the API's list of
// {ActionType, <Kind>Action} entries.
func (c *connectCall) ruleActions() ([]json.RawMessage, error) {
	actions, _ := c.wire["Actions"].(map[string]any)

	var out []json.RawMessage

	for _, key := range sortedKeys(actions) {
		kind, known := connectRuleActionMembers()[key]
		if !known {
			return nil, fmt.Errorf("%w: Actions.%s", errUnsupportedCFNValue, key)
		}

		list, _ := actions[key].([]any)
		for _, item := range list {
			raw, err := json.Marshal(map[string]any{"ActionType": kind[0], kind[1]: item})
			if err != nil {
				return nil, err
			}

			out = append(out, raw)
		}
	}

	return out, nil
}

func createConnectRule(c *connectCall) (string, error) {
	var in connectbackend.CreateRuleInput
	if err := c.decode(&in, "Actions"); err != nil {
		return "", err
	}

	actions, err := c.ruleActions()
	if err != nil {
		return "", err
	}

	in.InstanceID = c.instance()
	in.Actions = actions

	out, err := c.be.CreateRule(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.RuleArn, nil
}

func updateConnectRule(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateRuleInput
	if err := c.decode(&in, "Actions", connectTagsProp); err != nil {
		return err
	}

	actions, err := c.ruleActions()
	if err != nil {
		return err
	}

	in.InstanceID, in.RuleID = splitConnectARN(physicalID)
	in.Actions = actions

	if _, err = c.be.UpdateRule(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectRule(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteRule(inst, id)

	return err
}
