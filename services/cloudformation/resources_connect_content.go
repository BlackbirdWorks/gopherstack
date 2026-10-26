package cloudformation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

const connectViewPublished = "PUBLISHED"

// ---- View ----

// viewContent builds the API's {Template, Actions} from the Template object and Actions list.
func (c *connectCall) viewContent() (*connectbackend.ViewInputContent, error) {
	template := c.wire["Template"]
	if s, isString := template.(string); isString {
		return &connectbackend.ViewInputContent{Template: s, Actions: c.actionStrings()}, nil
	}

	raw, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}

	return &connectbackend.ViewInputContent{Template: string(raw), Actions: c.actionStrings()}, nil
}

func (c *connectCall) actionStrings() []string {
	var out []string

	list, _ := c.wire["Actions"].([]any)
	for _, a := range list {
		if s, ok := a.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func createConnectView(c *connectCall) (string, error) {
	content, err := c.viewContent()
	if err != nil {
		return "", err
	}

	var in connectbackend.CreateViewInput

	in.Tags, in.Content, in.Name, in.Description = c.tags(), content, c.str(attrNameName), c.str("Description")
	in.Status, in.InstanceID = connectViewPublished, c.instance()

	v, err := c.be.CreateView(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{connectViewSha: v.ViewContentSha256})

	return v.Arn, nil
}

func updateConnectView(c *connectCall, physicalID string) error {
	content, err := c.viewContent()
	if err != nil {
		return err
	}

	req := connectbackend.ViewRequest{Content: content, Status: connectViewPublished}
	req.InstanceID, req.ViewID = splitConnectARN(physicalID)

	v, err := c.be.UpdateViewContent(&req)
	if err != nil {
		return err
	}

	meta := connectbackend.ViewRequest{Name: new(c.str(attrNameName)), Description: new(c.str("Description"))}
	meta.InstanceID, meta.ViewID = req.InstanceID, req.ViewID

	if _, err = c.be.UpdateViewMetadata(&meta); err != nil {
		return err
	}

	c.stash(map[string]string{connectViewSha: v.ViewContentSha256})

	return c.syncTags(physicalID)
}

func deleteConnectView(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteView(inst, id)

	return err
}

// ---- ViewVersion ----

// createConnectViewVersion publishes a view version. Ref and GetAtt ViewVersionArn return <view ARN>:<version>.
func createConnectViewVersion(c *connectCall) (string, error) {
	req := connectbackend.ViewRequest{
		VersionDescription: c.str("VersionDescription"), ViewContentSha256: c.str(connectViewSha),
	}
	req.InstanceID, req.ViewID = splitConnectARN(c.str("ViewArn"))

	v, err := c.be.CreateViewVersion(&req)
	if err != nil {
		return "", err
	}

	version := strconv.Itoa(int(v.Version))
	c.stash(map[string]string{"Version": version})

	return v.Arn + ":" + version, nil
}

func deleteConnectViewVersion(c *connectCall, physicalID string) error {
	viewARN, version, _ := strings.CutLast(physicalID, ":")

	req := connectbackend.ViewRequest{PathVersion: version}
	req.InstanceID, req.ViewID = splitConnectARN(viewARN)
	_, err := c.be.DeleteViewVersion(&req)

	return err
}

// ---- Vocabulary ----

func createConnectVocabulary(c *connectCall) (string, error) {
	var in connectbackend.CreateVocabularyInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateVocabulary(c.ctx, &in)
	if err != nil {
		return "", err
	}

	inst, id := splitConnectARN(out.VocabularyArn)

	attrs := map[string]string{"State": out.State}
	if v, derr := c.be.DescribeVocabulary(inst, id); derr == nil {
		attrs[connectLastModified] = connectTime(&v.LastModifiedTime)
	}

	c.stash(attrs)

	return out.VocabularyArn, nil
}

func updateConnectVocabulary(c *connectCall, physicalID string) error {
	return c.syncTags(physicalID)
}

func deleteConnectVocabulary(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteVocabulary(inst, id)

	return err
}

// ---- EmailAddress ----

func createConnectEmailAddress(c *connectCall) (string, error) {
	var in connectbackend.CreateEmailAddressInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateEmailAddress(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if err = syncConnectEmailAliases(c, out.EmailAddressArn); err != nil {
		_, _ = c.be.DeleteEmailAddress(splitConnectARN(out.EmailAddressArn))

		return "", err
	}

	return out.EmailAddressArn, nil
}

func updateConnectEmailAddress(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateEmailAddressMetadataInput
	in.InstanceID, in.EmailID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))
	in.DisplayName = new(c.str("DisplayName"))

	if _, err := c.be.UpdateEmailAddressMetadata(&in); err != nil {
		return err
	}

	if err := syncConnectEmailAliases(c, physicalID); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

// syncConnectEmailAliases makes the address's aliases equal AliasConfigurations.
func syncConnectEmailAliases(c *connectCall, emailARN string) error {
	inst, id := splitConnectARN(emailARN)

	cur, err := c.be.DescribeEmailAddress(inst, id)
	if err != nil {
		return err
	}

	var have, want []string
	for _, a := range cur.AliasConfigurations {
		have = append(have, a.EmailAddressID)
	}

	list, _ := c.wire["AliasConfigurations"].([]any)
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			if s, isString := m["EmailAddressId"].(string); isString {
				want = append(want, s)
			}
		}
	}

	for _, drop := range diffStrings(have, want) {
		in := aliasInput(inst, id, drop)
		if _, err = c.be.DisassociateEmailAddressAlias(&in); err != nil {
			return err
		}
	}

	for _, add := range diffStrings(want, have) {
		in := aliasInput(inst, id, add)
		if _, err = c.be.AssociateEmailAddressAlias(&in); err != nil {
			return err
		}
	}

	return nil
}

func aliasInput(inst, id, alias string) connectbackend.EmailAliasInput {
	var in connectbackend.EmailAliasInput

	in.InstanceID, in.EmailID = inst, id
	in.AliasConfiguration = &connectbackend.AliasConfiguration{EmailAddressID: alias}

	return in
}

// diffStrings returns the members of a that are not in b.
func diffStrings(a, b []string) []string {
	return slices.DeleteFunc(slices.Clone(a), func(s string) bool { return slices.Contains(b, s) })
}

func deleteConnectEmailAddress(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteEmailAddress(inst, id)

	return err
}

// ---- PredefinedAttribute ----

// connectPredefinedID is the "<instance ID>|<attribute name>" physical ID of a predefined attribute.
func connectPredefinedID(instance, name string) string {
	return lastPathSegment(instance) + "|" + name
}

func createConnectPredefinedAttribute(c *connectCall) (string, error) {
	var in connectbackend.CreatePredefinedAttributeInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	if _, err := c.be.CreatePredefinedAttribute(c.ctx, &in); err != nil {
		return "", err
	}

	id := connectPredefinedID(in.InstanceID, in.Name)
	c.stashPredefined(id)

	return id, nil
}

func (c *connectCall) stashPredefined(physicalID string) {
	inst, name, _ := strings.Cut(physicalID, "|")

	if a, err := c.be.DescribePredefinedAttribute(inst, name); err == nil {
		c.stash(map[string]string{
			connectLastModified:  connectTime(a.LastModifiedTime),
			connectLastModRegion: a.LastModifiedRegion,
		})
	}
}

func updateConnectPredefinedAttribute(c *connectCall, physicalID string) error {
	var in connectbackend.UpdatePredefinedAttributeInput
	if err := c.decode(&in, attrNameName); err != nil {
		return err
	}

	in.InstanceID, in.Name, _ = strings.Cut(physicalID, "|")

	if _, err := c.be.UpdatePredefinedAttribute(c.ctx, &in); err != nil {
		return err
	}

	c.stashPredefined(physicalID)

	return nil
}

func deleteConnectPredefinedAttribute(c *connectCall, physicalID string) error {
	inst, name, _ := strings.Cut(physicalID, "|")
	_, err := c.be.DeletePredefinedAttribute(inst, name)

	return err
}

// ---- TaskTemplate ----

// taskTemplateBody maps the template's Defaults list and self-assign flow to the API's shape.
func (c *connectCall) taskTemplateWire() {
	if list, ok := c.wire["Defaults"].([]any); ok {
		c.wire["Defaults"] = map[string]any{"DefaultFieldValues": list}
	}
}

func createConnectTaskTemplate(c *connectCall) (string, error) {
	c.taskTemplateWire()

	var in connectbackend.CreateTaskTemplateInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()
	in.SelfAssignFlowID = c.str("SelfAssignContactFlowId")

	out, err := c.be.CreateTaskTemplate(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if tags := c.tags(); len(tags) > 0 {
		if err = c.be.TagResource(out.Arn, tags); err != nil {
			return "", fmt.Errorf("tag task template: %w", err)
		}
	}

	return out.Arn, nil
}

func updateConnectTaskTemplate(c *connectCall, physicalID string) error {
	c.taskTemplateWire()

	var in connectbackend.UpdateTaskTemplateInput
	if err := c.decode(&in, connectTagsProp); err != nil {
		return err
	}

	in.InstanceID, in.TemplateID = splitConnectARN(physicalID)
	in.SelfAssignFlowID = c.str("SelfAssignContactFlowId")
	in.Description = c.str("Description")

	if _, err := c.be.UpdateTaskTemplate(&in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectTaskTemplate(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteTaskTemplate(inst, id)

	return err
}
