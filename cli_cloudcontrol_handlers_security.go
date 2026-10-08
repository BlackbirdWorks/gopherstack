package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccTrue           = "true"
	ccKeyName        = "Name"
	ccDisplayName    = "DisplayName"
	ccKmsMasterKeyID = "KmsMasterKeyId"
)

func ccString(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func ccInt32(m map[string]any, key string) (int32, bool) {
	f, ok := m[key].(float64)

	return int32(f), ok
}

// ccTagDelta returns the tags to set and the keys to remove to move current to desired.
func ccTagDelta(current, desired map[string]string) (map[string]string, []string) {
	set := map[string]string{}

	for k, v := range desired {
		if cur, ok := current[k]; !ok || cur != v {
			set[k] = v
		}
	}

	var unset []string

	for k := range current {
		if _, ok := desired[k]; !ok {
			unset = append(unset, k)
		}
	}

	return set, unset
}

func ccModelTags(m map[string]any) (map[string]string, error) {
	tags, err := ccDecode[ccTags](m["Tags"])
	if err != nil {
		return nil, err
	}

	return tags.toMap(), nil
}

// ccPolicyJSON renders a policy document property (object or string) as a JSON string.
func ccPolicyJSON(v any) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("%w: %w", cloudcontrolbackend.ErrValidation, err)
	}

	return string(b), nil
}

func ccPolicyObject(doc string, unescape bool) any {
	if unescape {
		if dec, err := url.QueryUnescape(doc); err == nil {
			doc = dec
		}
	}

	var out any
	if json.Unmarshal([]byte(doc), &out) != nil {
		return doc
	}

	return out
}

// --- AWS::IAM::Role ---

type ccRole struct{ client *iam.Client }

func ccIAMTags(m map[string]string) []iamtypes.Tag {
	out := make([]iamtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, iamtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccRole) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "RoleName")
	if name == "" {
		name = ccGeneratedName("cc-role-")
	}

	doc, err := ccPolicyJSON(desired["AssumeRolePolicyDocument"])
	if err != nil {
		return "", err
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &iam.CreateRoleInput{
		RoleName: aws.String(name), AssumeRolePolicyDocument: aws.String(doc), Tags: ccIAMTags(tags),
	}
	if p := ccString(desired, "Path"); p != "" {
		in.Path = aws.String(p)
	}

	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if n, ok := ccInt32(desired, "MaxSessionDuration"); ok {
		in.MaxSessionDuration = aws.Int32(n)
	}

	if _, err = h.client.CreateRole(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	arns, _ := ccDecode[[]string](desired["ManagedPolicyArns"])
	for _, arn := range arns {
		if _, err = h.client.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName: aws.String(name), PolicyArn: aws.String(arn),
		}); err != nil {
			return "", ccMapError(err)
		}
	}

	return name, nil
}

func (h *ccRole) attachedPolicies(ctx context.Context, name string) ([]string, error) {
	var arns []string

	p := iam.NewListAttachedRolePoliciesPaginator(
		h.client,
		&iam.ListAttachedRolePoliciesInput{RoleName: aws.String(name)},
	)
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, a := range out.AttachedPolicies {
			arns = append(arns, aws.ToString(a.PolicyArn))
		}
	}

	return arns, nil
}

func (h *ccRole) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	r := out.Role
	model := map[string]any{
		"RoleName": id, ccKeyArn: aws.ToString(r.Arn), "RoleId": aws.ToString(r.RoleId),
		"Path": aws.ToString(r.Path),
	}

	if r.AssumeRolePolicyDocument != nil {
		model["AssumeRolePolicyDocument"] = ccPolicyObject(aws.ToString(r.AssumeRolePolicyDocument), true)
	}

	if r.Description != nil {
		model["Description"] = *r.Description
	}

	if r.MaxSessionDuration != nil {
		model["MaxSessionDuration"] = *r.MaxSessionDuration
	}

	if len(r.Tags) > 0 {
		tags := make(map[string]string, len(r.Tags))
		for _, t := range r.Tags {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(tags)
	}

	arns, err := h.attachedPolicies(ctx, id)
	if err != nil {
		return nil, err
	}

	if len(arns) > 0 {
		model["ManagedPolicyArns"] = arns
	}

	return model, nil
}

func (h *ccRole) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "AssumeRolePolicyDocument", "Description", "MaxSessionDuration", "Tags", "ManagedPolicyArns",
	); err != nil {
		return err
	}

	if err := h.updateRoleSettings(ctx, id, desired); err != nil {
		return err
	}

	if err := h.syncPolicies(ctx, id, current, desired); err != nil {
		return err
	}

	return h.syncTags(ctx, id, current, desired)
}

func (h *ccRole) updateRoleSettings(ctx context.Context, id string, desired map[string]any) error {
	if raw, ok := desired["AssumeRolePolicyDocument"]; ok {
		doc, err := ccPolicyJSON(raw)
		if err != nil {
			return err
		}

		if _, err = h.client.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
			RoleName: aws.String(id), PolicyDocument: aws.String(doc),
		}); err != nil {
			return ccMapError(err)
		}
	}

	in := &iam.UpdateRoleInput{RoleName: aws.String(id)}
	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if n, ok := ccInt32(desired, "MaxSessionDuration"); ok && n > 0 {
		in.MaxSessionDuration = aws.Int32(n)
	}

	if in.Description == nil && in.MaxSessionDuration == nil {
		return nil
	}

	_, err := h.client.UpdateRole(ctx, in)

	return ccMapError(err)
}

func (h *ccRole) syncPolicies(ctx context.Context, id string, current, desired map[string]any) error {
	have, _ := ccDecode[[]string](current["ManagedPolicyArns"])
	want, _ := ccDecode[[]string](desired["ManagedPolicyArns"])

	haveSet := map[string]string{}
	for _, a := range have {
		haveSet[a] = ""
	}

	wantSet := map[string]string{}
	for _, a := range want {
		wantSet[a] = ""
	}

	add, remove := ccTagDelta(haveSet, wantSet)

	for arn := range add {
		if _, err := h.client.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName: aws.String(id), PolicyArn: aws.String(arn),
		}); err != nil {
			return ccMapError(err)
		}
	}

	for _, arn := range remove {
		if _, err := h.client.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
			RoleName: aws.String(id), PolicyArn: aws.String(arn),
		}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccRole) syncTags(ctx context.Context, id string, current, desired map[string]any) error {
	have, err := ccModelTags(current)
	if err != nil {
		return err
	}

	want, err := ccModelTags(desired)
	if err != nil {
		return err
	}

	set, unset := ccTagDelta(have, want)

	if len(set) > 0 {
		if _, err = h.client.TagRole(
			ctx,
			&iam.TagRoleInput{RoleName: aws.String(id), Tags: ccIAMTags(set)},
		); err != nil {
			return ccMapError(err)
		}
	}

	if len(unset) > 0 {
		_, err = h.client.UntagRole(ctx, &iam.UntagRoleInput{RoleName: aws.String(id), TagKeys: unset})

		return ccMapError(err)
	}

	return nil
}

func (h *ccRole) Delete(ctx context.Context, id string) error {
	arns, err := h.attachedPolicies(ctx, id)
	if err != nil {
		return err
	}

	for _, arn := range arns {
		if _, err = h.client.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
			RoleName: aws.String(id), PolicyArn: aws.String(arn),
		}); err != nil {
			return ccMapError(err)
		}
	}

	_, err = h.client.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(id)})

	return ccMapError(err)
}

func (h *ccRole) List(ctx context.Context) ([]string, error) {
	var names []string

	p := iam.NewListRolesPaginator(h.client, &iam.ListRolesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, r := range out.Roles {
			names = append(names, aws.ToString(r.RoleName))
		}
	}

	return names, nil
}

// --- AWS::KMS::Key ---

type ccKey struct{ client *kms.Client }

func ccKMSTags(m map[string]string) []kmstypes.Tag {
	out := make([]kmstypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, kmstypes.Tag{TagKey: aws.String(k), TagValue: aws.String(v)})
	}

	return out
}

func (h *ccKey) Create(ctx context.Context, desired map[string]any) (string, error) {
	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &kms.CreateKeyInput{Tags: ccKMSTags(tags)}
	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if u := ccString(desired, "KeyUsage"); u != "" {
		in.KeyUsage = kmstypes.KeyUsageType(u)
	}

	if s := ccString(desired, "KeySpec"); s != "" {
		in.KeySpec = kmstypes.KeySpec(s)
	}

	if mr, ok := desired["MultiRegion"].(bool); ok {
		in.MultiRegion = aws.Bool(mr)
	}

	if p, ok := desired["KeyPolicy"]; ok {
		doc, polErr := ccPolicyJSON(p)
		if polErr != nil {
			return "", polErr
		}

		in.Policy = aws.String(doc)
	}

	out, err := h.client.CreateKey(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.KeyMetadata.KeyId)

	if err = h.applyState(ctx, id, desired); err != nil {
		return "", err
	}

	return id, nil
}

func (h *ccKey) applyState(ctx context.Context, id string, desired map[string]any) error {
	if rot, ok := desired["EnableKeyRotation"].(bool); ok && rot {
		if _, err := h.client.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyId: aws.String(id)}); err != nil {
			return ccMapError(err)
		}
	}

	if en, ok := desired["Enabled"].(bool); ok && !en {
		if _, err := h.client.DisableKey(ctx, &kms.DisableKeyInput{KeyId: aws.String(id)}); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccKey) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	md := out.KeyMetadata
	if md.KeyState == kmstypes.KeyStatePendingDeletion || md.KeyState == kmstypes.KeyStatePendingReplicaDeletion {
		return nil, fmt.Errorf("%w: key %s is pending deletion", cloudcontrolbackend.ErrNotFound, id)
	}

	model := map[string]any{
		"KeyId": aws.ToString(md.KeyId), ccKeyArn: aws.ToString(md.Arn), "Enabled": md.Enabled,
		"KeyUsage": string(md.KeyUsage), "KeySpec": string(md.KeySpec),
		"MultiRegion": aws.ToBool(md.MultiRegion), "Description": aws.ToString(md.Description),
	}

	if pol, polErr := h.client.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: aws.String(id)}); polErr == nil {
		model["KeyPolicy"] = ccPolicyObject(aws.ToString(pol.Policy), false)
	}

	if rot, rotErr := h.client.GetKeyRotationStatus(
		ctx, &kms.GetKeyRotationStatusInput{KeyId: aws.String(id)},
	); rotErr == nil {
		model["EnableKeyRotation"] = rot.KeyRotationEnabled
	}

	tags, err := h.client.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: aws.String(id)})
	if err == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.TagKey)] = aws.ToString(t.TagValue)
		}

		model["Tags"] = tagsProperty(m)
	}

	return model, nil
}

func (h *ccKey) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "Description", "Enabled", "EnableKeyRotation", "KeyPolicy", "Tags", "PendingWindowInDays",
	); err != nil {
		return err
	}

	if d, ok := desired["Description"].(string); ok && d != ccString(current, "Description") {
		if _, err := h.client.UpdateKeyDescription(ctx, &kms.UpdateKeyDescriptionInput{
			KeyId: aws.String(id), Description: aws.String(d),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if p, ok := desired["KeyPolicy"]; ok {
		doc, err := ccPolicyJSON(p)
		if err != nil {
			return err
		}

		if _, err = h.client.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
			KeyId: aws.String(id), Policy: aws.String(doc),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if err := h.updateToggles(ctx, id, current, desired); err != nil {
		return err
	}

	return h.syncTags(ctx, id, current, desired)
}

func (h *ccKey) updateToggles(ctx context.Context, id string, current, desired map[string]any) error {
	wantEnabled, hasEnabled := desired["Enabled"].(bool)
	haveEnabled, _ := current["Enabled"].(bool)

	if hasEnabled && wantEnabled != haveEnabled {
		var err error
		if wantEnabled {
			_, err = h.client.EnableKey(ctx, &kms.EnableKeyInput{KeyId: aws.String(id)})
		} else {
			_, err = h.client.DisableKey(ctx, &kms.DisableKeyInput{KeyId: aws.String(id)})
		}

		if err != nil {
			return ccMapError(err)
		}
	}

	wantRot, _ := desired["EnableKeyRotation"].(bool)
	haveRot, _ := current["EnableKeyRotation"].(bool)

	if wantRot == haveRot {
		return nil
	}

	var err error
	if wantRot {
		_, err = h.client.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyId: aws.String(id)})
	} else {
		_, err = h.client.DisableKeyRotation(ctx, &kms.DisableKeyRotationInput{KeyId: aws.String(id)})
	}

	return ccMapError(err)
}

func (h *ccKey) syncTags(ctx context.Context, id string, current, desired map[string]any) error {
	have, err := ccModelTags(current)
	if err != nil {
		return err
	}

	want, err := ccModelTags(desired)
	if err != nil {
		return err
	}

	set, unset := ccTagDelta(have, want)

	if len(set) > 0 {
		if _, err = h.client.TagResource(ctx, &kms.TagResourceInput{
			KeyId: aws.String(id), Tags: ccKMSTags(set),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if len(unset) > 0 {
		_, err = h.client.UntagResource(ctx, &kms.UntagResourceInput{KeyId: aws.String(id), TagKeys: unset})

		return ccMapError(err)
	}

	return nil
}

const ccKMSDefaultPendingWindow = 30

func (h *ccKey) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
		KeyId: aws.String(id), PendingWindowInDays: aws.Int32(ccKMSDefaultPendingWindow),
	})

	return ccMapError(err)
}

func (h *ccKey) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := kms.NewListKeysPaginator(h.client, &kms.ListKeysInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, k := range out.Keys {
			ids = append(ids, aws.ToString(k.KeyId))
		}
	}

	return ids, nil
}

// --- AWS::SecretsManager::Secret ---

type ccSecret struct{ client *secretsmanager.Client }

func ccSecretTags(m map[string]string) []smtypes.Tag {
	out := make([]smtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, smtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccSecret) Create(ctx context.Context, desired map[string]any) (string, error) {
	if _, ok := desired["GenerateSecretString"]; ok {
		return "", fmt.Errorf("%w: GenerateSecretString is not supported", cloudcontrolbackend.ErrValidation)
	}

	name := ccString(desired, "Name")
	if name == "" {
		name = ccGeneratedName("cc-secret-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	in := &secretsmanager.CreateSecretInput{Name: aws.String(name), Tags: ccSecretTags(tags)}
	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if k := ccString(desired, "KmsKeyId"); k != "" {
		in.KmsKeyId = aws.String(k)
	}

	if s := ccString(desired, "SecretString"); s != "" {
		in.SecretString = aws.String(s)
	}

	out, err := h.client.CreateSecret(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.ARN), nil
}

func (h *ccSecret) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	if out.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is scheduled for deletion", cloudcontrolbackend.ErrNotFound, id)
	}

	model := map[string]any{"Id": aws.ToString(out.ARN), ccKeyName: aws.ToString(out.Name)}

	if out.Description != nil {
		model["Description"] = *out.Description
	}

	if out.KmsKeyId != nil {
		model["KmsKeyId"] = *out.KmsKeyId
	}

	if len(out.Tags) > 0 {
		tags := make(map[string]string, len(out.Tags))
		for _, t := range out.Tags {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = tagsProperty(tags)
	}

	return model, nil
}

func (h *ccSecret) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current,
		desired,
		"Description",
		"KmsKeyId",
		"SecretString",
		"Tags",
	); err != nil {
		return err
	}

	in := &secretsmanager.UpdateSecretInput{SecretId: aws.String(id)}
	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if k := ccString(desired, "KmsKeyId"); k != "" {
		in.KmsKeyId = aws.String(k)
	}

	if s := ccString(desired, "SecretString"); s != "" {
		in.SecretString = aws.String(s)
	}

	if in.Description != nil || in.KmsKeyId != nil || in.SecretString != nil {
		if _, err := h.client.UpdateSecret(ctx, in); err != nil {
			return ccMapError(err)
		}
	}

	have, err := ccModelTags(current)
	if err != nil {
		return err
	}

	want, err := ccModelTags(desired)
	if err != nil {
		return err
	}

	set, unset := ccTagDelta(have, want)

	if len(set) > 0 {
		if _, err = h.client.TagResource(ctx, &secretsmanager.TagResourceInput{
			SecretId: aws.String(id), Tags: ccSecretTags(set),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if len(unset) > 0 {
		_, err = h.client.UntagResource(
			ctx,
			&secretsmanager.UntagResourceInput{SecretId: aws.String(id), TagKeys: unset},
		)

		return ccMapError(err)
	}

	return nil
}

func (h *ccSecret) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId: aws.String(id), ForceDeleteWithoutRecovery: aws.Bool(true),
	})

	return ccMapError(err)
}

func (h *ccSecret) List(ctx context.Context) ([]string, error) {
	var arns []string

	p := secretsmanager.NewListSecretsPaginator(h.client, &secretsmanager.ListSecretsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, s := range out.SecretList {
			arns = append(arns, aws.ToString(s.ARN))
		}
	}

	return arns, nil
}

// --- AWS::SSM::Parameter ---

type ccParameter struct{ client *ssm.Client }

func ccSSMTags(m map[string]string) []ssmtypes.Tag {
	out := make([]ssmtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, ssmtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

// ccMapTags reads the SSM Parameter Tags property, which is a plain string map.
func ccMapTags(m map[string]any) map[string]string {
	out, _ := ccDecode[map[string]string](m["Tags"])

	return out
}

func (h *ccParameter) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "Name")
	if name == "" {
		name = "/" + ccGeneratedName("cc-param-")
	}

	typ := ccString(desired, "Type")
	if typ == "" {
		typ = string(ssmtypes.ParameterTypeString)
	}

	in := &ssm.PutParameterInput{
		Name: aws.String(name), Type: ssmtypes.ParameterType(typ), Value: aws.String(ccString(desired, "Value")),
		Tags: ccSSMTags(ccMapTags(desired)),
	}
	h.applyOptional(in, desired)

	if _, err := h.client.PutParameter(ctx, in); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccParameter) applyOptional(in *ssm.PutParameterInput, desired map[string]any) {
	if d := ccString(desired, "Description"); d != "" {
		in.Description = aws.String(d)
	}

	if a := ccString(desired, "AllowedPattern"); a != "" {
		in.AllowedPattern = aws.String(a)
	}

	if t := ccString(desired, "Tier"); t != "" {
		in.Tier = ssmtypes.ParameterTier(t)
	}

	if d := ccString(desired, "DataType"); d != "" {
		in.DataType = aws.String(d)
	}
}

func (h *ccParameter) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	p := out.Parameter
	model := map[string]any{
		"Name": id, "Type": string(p.Type), "Value": aws.ToString(p.Value), "DataType": aws.ToString(p.DataType),
	}

	desc, err := h.client.DescribeParameters(ctx, &ssm.DescribeParametersInput{
		ParameterFilters: []ssmtypes.ParameterStringFilter{{
			Key: aws.String("Name"), Option: aws.String("Equals"), Values: []string{id},
		}},
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	for _, meta := range desc.Parameters {
		if aws.ToString(meta.Name) != id {
			continue
		}

		if meta.Description != nil {
			model["Description"] = *meta.Description
		}

		if meta.AllowedPattern != nil {
			model["AllowedPattern"] = *meta.AllowedPattern
		}

		if meta.Tier != "" {
			model["Tier"] = string(meta.Tier)
		}
	}

	tags, err := h.client.ListTagsForResource(ctx, &ssm.ListTagsForResourceInput{
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter, ResourceId: aws.String(id),
	})
	if err == nil && len(tags.TagList) > 0 {
		m := make(map[string]string, len(tags.TagList))
		for _, t := range tags.TagList {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model["Tags"] = m
	}

	return model, nil
}

func (h *ccParameter) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, "Value", "Description", "AllowedPattern", "Tier", "DataType", "Tags",
	); err != nil {
		return err
	}

	in := &ssm.PutParameterInput{
		Name: aws.String(id), Value: aws.String(ccString(desired, "Value")), Overwrite: aws.Bool(true),
		Type: ssmtypes.ParameterType(ccString(current, "Type")),
	}
	h.applyOptional(in, desired)

	if _, err := h.client.PutParameter(ctx, in); err != nil {
		return ccMapError(err)
	}

	set, unset := ccTagDelta(ccMapTags(current), ccMapTags(desired))

	if len(set) > 0 {
		if _, err := h.client.AddTagsToResource(ctx, &ssm.AddTagsToResourceInput{
			ResourceType: ssmtypes.ResourceTypeForTaggingParameter, ResourceId: aws.String(id), Tags: ccSSMTags(set),
		}); err != nil {
			return ccMapError(err)
		}
	}

	if len(unset) > 0 {
		_, err := h.client.RemoveTagsFromResource(ctx, &ssm.RemoveTagsFromResourceInput{
			ResourceType: ssmtypes.ResourceTypeForTaggingParameter, ResourceId: aws.String(id), TagKeys: unset,
		})

		return ccMapError(err)
	}

	return nil
}

func (h *ccParameter) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: aws.String(id)})

	return ccMapError(err)
}

func (h *ccParameter) List(ctx context.Context) ([]string, error) {
	var names []string

	p := ssm.NewDescribeParametersPaginator(h.client, &ssm.DescribeParametersInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, meta := range out.Parameters {
			names = append(names, aws.ToString(meta.Name))
		}
	}

	return names, nil
}
