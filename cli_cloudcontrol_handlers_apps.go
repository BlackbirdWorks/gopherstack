package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccCognitoPageSize   = 60
	ccKeyDescription    = "Description"
	ccKeyAPIKeySource   = "ApiKeySourceType"
	ccKeyMinCompression = "MinimumCompressionSize"
	ccKeyUserPoolName   = "UserPoolName"
	ccKeyUserPoolTags   = "UserPoolTags"
	ccKeySchema         = "Schema"
	ccKeyPolicy         = "Policy"
	ccKeySecurityPolicy = "SecurityPolicy"
	ccKeyAccessMode     = "EndpointAccessMode"
)

// --- AWS::Cognito::UserPool ---

type ccUserPool struct {
	client *cognitoidentityprovider.Client
	region string
}

func ccUserPoolMutable() []string {
	return []string{
		"Policies", "AutoVerifiedAttributes", "MfaConfiguration", ccKeyUserPoolTags, "AdminCreateUserConfig",
		"DeletionProtection", "EmailConfiguration", "LambdaConfig", "VerificationMessageTemplate",
		"UserPoolAddOns", "AccountRecoverySetting", "SmsConfiguration", "EmailVerificationMessage",
		"EmailVerificationSubject", "SmsVerificationMessage", "SmsAuthenticationMessage", "DeviceConfiguration",
		"UserAttributeUpdateSettings", "UserPoolTier",
	}
}

func (h *ccUserPool) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, ccKeyUserPoolName)
	if name == "" {
		name = ccGeneratedName("cc-pool-")
	}

	in, err := ccDecode[cognitoidentityprovider.CreateUserPoolInput](desired)
	if err != nil {
		return "", err
	}

	in.PoolName = aws.String(name)

	out, err := h.client.CreateUserPool(ctx, &in)
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.UserPool.Id), nil
}

func (h *ccUserPool) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeUserPool(
		ctx,
		&cognitoidentityprovider.DescribeUserPoolInput{UserPoolId: aws.String(id)},
	)
	if err != nil {
		return nil, ccMapError(err)
	}

	raw, _ := ccDropNulls(normalizeJSON(out.UserPool)).(map[string]any)
	model := map[string]any{
		"UserPoolId": id, ccKeyArn: aws.ToString(out.UserPool.Arn),
		ccKeyUserPoolName: aws.ToString(out.UserPool.Name),
		"ProviderName":    fmt.Sprintf("cognito-idp.%s.amazonaws.com/%s", h.region, id),
		"ProviderURL":     fmt.Sprintf("https://cognito-idp.%s.amazonaws.com/%s", h.region, id),
	}

	for _, k := range ccUserPoolMutable() {
		if v, ok := raw[k]; ok && !ccEmpty(v) {
			model[k] = v
		}
	}

	for _, k := range []string{"UsernameAttributes", "AliasAttributes", "UsernameConfiguration"} {
		if v, ok := raw[k]; ok && !ccEmpty(v) {
			model[k] = v
		}
	}

	if custom := ccCustomSchema(out.UserPool.SchemaAttributes); len(custom) > 0 {
		model[ccKeySchema] = custom
	}

	return model, nil
}

func ccEmpty(v any) bool {
	switch t := v.(type) {
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	case string:
		return t == ""
	default:
		return false
	}
}

func ccCustomSchema(attrs []cognitotypes.SchemaAttributeType) []cognitotypes.SchemaAttributeType {
	var out []cognitotypes.SchemaAttributeType

	for _, a := range attrs {
		name := aws.ToString(a.Name)
		if rest, ok := strings.CutPrefix(name, "custom:"); ok {
			a.Name = aws.String(rest)
			out = append(out, a)
		}
	}

	return out
}

func (h *ccUserPool) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, ccUserPoolMutable()...); err != nil {
		return err
	}

	in, err := ccDecode[cognitoidentityprovider.UpdateUserPoolInput](desired)
	if err != nil {
		return err
	}

	in.UserPoolId = aws.String(id)

	_, err = h.client.UpdateUserPool(ctx, &in)

	return ccMapError(err)
}

func (h *ccUserPool) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteUserPool(ctx, &cognitoidentityprovider.DeleteUserPoolInput{UserPoolId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccUserPool) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := cognitoidentityprovider.NewListUserPoolsPaginator(h.client, &cognitoidentityprovider.ListUserPoolsInput{
		MaxResults: aws.Int32(ccCognitoPageSize),
	})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, u := range out.UserPools {
			ids = append(ids, aws.ToString(u.Id))
		}
	}

	return ids, nil
}

// --- AWS::ApiGateway::RestApi ---

type ccRestAPI struct {
	client *apigateway.Client
	region string
}

type ccRestAPISpec struct {
	Policy                    any
	Body                      any
	EndpointConfiguration     *apigwtypes.EndpointConfiguration
	MinimumCompressionSize    *int32
	DisableExecuteAPIEndpoint *bool  `json:"DisableExecuteApiEndpoint"`
	APIKeySourceType          string `json:"ApiKeySourceType"`
	SecurityPolicy            string
	EndpointAccessMode        string
	Description               string
	Name                      string
	BinaryMediaTypes          []string
}

func (h *ccRestAPI) arn(id string) string {
	return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", h.region, id)
}

func (h *ccRestAPI) Create(ctx context.Context, desired map[string]any) (string, error) {
	spec, err := ccDecode[ccRestAPISpec](desired)
	if err != nil {
		return "", err
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	policy := ""
	if spec.Policy != nil {
		if policy, err = ccPolicyJSON(spec.Policy); err != nil {
			return "", err
		}
	}

	if spec.Body != nil {
		return h.importBody(ctx, spec, tags)
	}

	if spec.Name == "" {
		return "", fmt.Errorf("%w: Name is required when Body is not specified", cloudcontrolbackend.ErrValidation)
	}

	out, err := h.client.CreateRestApi(ctx, &apigateway.CreateRestApiInput{
		Name:             aws.String(spec.Name),
		Description:      ccOptional(spec.Description),
		ApiKeySource:     apigwtypes.ApiKeySourceType(spec.APIKeySourceType),
		BinaryMediaTypes: spec.BinaryMediaTypes,
		DisableExecuteApiEndpoint: aws.ToBool(
			spec.DisableExecuteAPIEndpoint,
		),
		EndpointConfiguration:  spec.EndpointConfiguration,
		SecurityPolicy:         apigwtypes.SecurityPolicy(spec.SecurityPolicy),
		EndpointAccessMode:     apigwtypes.EndpointAccessMode(spec.EndpointAccessMode),
		MinimumCompressionSize: spec.MinimumCompressionSize,
		Policy:                 ccOptional(policy),
		Tags:                   tags,
	})
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.Id), nil
}

func (h *ccRestAPI) importBody(ctx context.Context, spec ccRestAPISpec, tags map[string]string) (string, error) {
	body, err := ccPolicyJSON(spec.Body)
	if err != nil {
		return "", err
	}

	out, err := h.client.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(body)})
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.Id)

	var ops []apigwtypes.PatchOperation
	if spec.Name != "" {
		ops = append(ops, ccPatchReplace("/name", spec.Name))
	}

	if spec.Description != "" {
		ops = append(ops, ccPatchReplace("/description", spec.Description))
	}

	if len(ops) > 0 {
		if _, err = h.client.UpdateRestApi(ctx, &apigateway.UpdateRestApiInput{
			RestApiId: aws.String(id), PatchOperations: ops,
		}); err != nil {
			return "", ccMapError(err)
		}
	}

	if len(tags) > 0 {
		if _, err = h.client.TagResource(ctx, &apigateway.TagResourceInput{
			ResourceArn: aws.String(h.arn(id)), Tags: tags,
		}); err != nil {
			return "", ccMapError(err)
		}
	}

	return id, nil
}

func ccOptional(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}

func ccPatchReplace(path, value string) apigwtypes.PatchOperation {
	return apigwtypes.PatchOperation{Op: apigwtypes.OpReplace, Path: aws.String(path), Value: aws.String(value)}
}

func (h *ccRestAPI) Read(ctx context.Context, id string) (map[string]any, error) {
	a, err := h.client.GetRestApi(ctx, &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	model := map[string]any{
		"RestApiId": id, "RootResourceId": a.RootResourceId, ccKeyName: a.Name, ccKeyDescription: a.Description,
		ccKeyAPIKeySource: string(a.ApiKeySource), "BinaryMediaTypes": a.BinaryMediaTypes,
		ccKeyMinCompression: a.MinimumCompressionSize, "EndpointConfiguration": a.EndpointConfiguration,
		ccKeySecurityPolicy: string(a.SecurityPolicy), ccKeyAccessMode: string(a.EndpointAccessMode),
	}

	if a.DisableExecuteApiEndpoint {
		model["DisableExecuteApiEndpoint"] = true
	}

	if a.Policy != nil && *a.Policy != "" {
		model[ccKeyPolicy] = ccPolicyObject(*a.Policy, false)
	}

	if len(a.Tags) > 0 {
		model[ccKeyTags] = tagsProperty(a.Tags)
	}

	return ccCleanModel(model), nil
}

func (h *ccRestAPI) Update(ctx context.Context, id string, current, desired map[string]any) error {
	mutable := []string{
		ccKeyName, ccKeyDescription, ccKeyAPIKeySource, ccKeyMinCompression, "DisableExecuteApiEndpoint",
		ccKeyPolicy, "BinaryMediaTypes", ccKeyTags, ccKeySecurityPolicy, ccKeyAccessMode,
	}
	if err := ccRejectUnsupportedChanges(current, desired, mutable...); err != nil {
		return err
	}

	ops, err := h.patchOps(current, desired)
	if err != nil {
		return err
	}

	if len(ops) > 0 {
		if _, err = h.client.UpdateRestApi(ctx, &apigateway.UpdateRestApiInput{
			RestApiId: aws.String(id), PatchOperations: ops,
		}); err != nil {
			return ccMapError(err)
		}
	}

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, e := h.client.TagResource(ctx, &apigateway.TagResourceInput{ResourceArn: aws.String(h.arn(id)), Tags: m})

			return e
		},
		func(keys []string) error {
			_, e := h.client.UntagResource(ctx, &apigateway.UntagResourceInput{
				ResourceArn: aws.String(h.arn(id)), TagKeys: keys,
			})

			return e
		})
}

func (*ccRestAPI) patchOps(current, desired map[string]any) ([]apigwtypes.PatchOperation, error) {
	var ops []apigwtypes.PatchOperation

	scalars := map[string]string{
		ccKeyName: "/name", ccKeyDescription: "/description", ccKeyAPIKeySource: "/apiKeySource",
		ccKeyMinCompression: "/minimumCompressionSize", "DisableExecuteApiEndpoint": "/disableExecuteApiEndpoint",
		ccKeySecurityPolicy: "/securityPolicy", ccKeyAccessMode: "/endpointAccessMode",
	}

	for key, path := range scalars {
		if v, ok := desired[key]; ok && fmt.Sprint(v) != fmt.Sprint(current[key]) {
			ops = append(ops, ccPatchReplace(path, fmt.Sprint(v)))
		}
	}

	if v, ok := desired[ccKeyPolicy]; ok && fmt.Sprint(v) != fmt.Sprint(current[ccKeyPolicy]) {
		doc, err := ccPolicyJSON(v)
		if err != nil {
			return nil, err
		}

		ops = append(ops, ccPatchReplace("/policy", doc))
	}

	have, _ := ccDecodeOptional[[]string](current, "BinaryMediaTypes")
	want, _ := ccDecodeOptional[[]string](desired, "BinaryMediaTypes")

	for _, t := range have {
		if !slices.Contains(want, t) {
			ops = append(ops, apigwtypes.PatchOperation{
				Op: apigwtypes.OpRemove, Path: aws.String("/binaryMediaTypes/" + ccEscapePointer(t)),
			})
		}
	}

	for _, t := range want {
		if !slices.Contains(have, t) {
			ops = append(ops, apigwtypes.PatchOperation{
				Op: apigwtypes.OpAdd, Path: aws.String("/binaryMediaTypes/" + ccEscapePointer(t)),
			})
		}
	}

	return ops, nil
}

func ccEscapePointer(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

func (h *ccRestAPI) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteRestApi(ctx, &apigateway.DeleteRestApiInput{RestApiId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccRestAPI) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := apigateway.NewGetRestApisPaginator(h.client, &apigateway.GetRestApisInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, a := range out.Items {
			ids = append(ids, aws.ToString(a.Id))
		}
	}

	return ids, nil
}

// --- AWS::EFS::FileSystem ---

type ccFileSystem struct{ client *efs.Client }

const (
	ccKeyFSTags      = "FileSystemTags"
	ccKeyLifecycle   = "LifecyclePolicies"
	ccKeyBackup      = "BackupPolicy"
	ccKeyFSPolicy    = "FileSystemPolicy"
	ccKeyThroughput  = "ThroughputMode"
	ccKeyProvisioned = "ProvisionedThroughputInMibps"
	ccKeyProtection  = "FileSystemProtection"
	ccKeyBypass      = "BypassPolicyLockoutSafetyCheck"
)

func ccEFSTags(m map[string]string) []efstypes.Tag {
	out := make([]efstypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, efstypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func ccFSTagMap(m map[string]any) (map[string]string, error) {
	tags, err := ccDecode[ccTags](m[ccKeyFSTags])
	if err != nil {
		return nil, err
	}

	return tags.toMap(), nil
}

func (h *ccFileSystem) Create(ctx context.Context, desired map[string]any) (string, error) {
	in, err := ccDecode[efs.CreateFileSystemInput](desired)
	if err != nil {
		return "", err
	}

	tags, err := ccFSTagMap(desired)
	if err != nil {
		return "", err
	}

	in.Tags = ccEFSTags(tags)
	in.ProvisionedThroughputInMibps = nil

	if v, ok := desired[ccKeyProvisioned].(float64); ok {
		in.ProvisionedThroughputInMibps = aws.Float64(v)
	}

	out, err := h.client.CreateFileSystem(ctx, &in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := aws.ToString(out.FileSystemId)

	if err = h.applyExtras(ctx, id, desired); err != nil {
		return "", err
	}

	return id, nil
}

func (h *ccFileSystem) applyExtras(ctx context.Context, id string, desired map[string]any) error {
	if _, ok := desired[ccKeyLifecycle]; ok {
		pol, err := ccDecodeOptional[[]efstypes.LifecyclePolicy](desired, ccKeyLifecycle)
		if err != nil {
			return err
		}

		if _, err = h.client.PutLifecycleConfiguration(ctx, &efs.PutLifecycleConfigurationInput{
			FileSystemId: aws.String(id), LifecyclePolicies: pol,
		}); err != nil {
			return ccMapError(err)
		}
	}

	if _, ok := desired[ccKeyBackup]; ok {
		bp, err := ccDecodeOptional[efstypes.BackupPolicy](desired, ccKeyBackup)
		if err != nil {
			return err
		}

		if _, err = h.client.PutBackupPolicy(ctx, &efs.PutBackupPolicyInput{
			FileSystemId: aws.String(id), BackupPolicy: &bp,
		}); err != nil {
			return ccMapError(err)
		}
	}

	if doc, ok := desired[ccKeyFSPolicy]; ok && doc != nil {
		s, err := ccPolicyJSON(doc)
		if err != nil {
			return err
		}

		bypass, _ := ccBool(desired, ccKeyBypass)

		if _, err = h.client.PutFileSystemPolicy(ctx, &efs.PutFileSystemPolicyInput{
			FileSystemId: aws.String(id), Policy: aws.String(s), BypassPolicyLockoutSafetyCheck: bypass,
		}); err != nil {
			return ccMapError(err)
		}
	}

	return h.applyProtection(ctx, id, desired)
}

func (h *ccFileSystem) applyProtection(ctx context.Context, id string, desired map[string]any) error {
	if _, ok := desired[ccKeyProtection]; !ok {
		return nil
	}

	prot, err := ccDecodeOptional[efstypes.FileSystemProtectionDescription](desired, ccKeyProtection)
	if err != nil {
		return err
	}

	if prot.ReplicationOverwriteProtection == "" {
		return nil
	}

	_, err = h.client.UpdateFileSystemProtection(ctx, &efs.UpdateFileSystemProtectionInput{
		FileSystemId: aws.String(id), ReplicationOverwriteProtection: prot.ReplicationOverwriteProtection,
	})

	return ccMapError(err)
}

func (h *ccFileSystem) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{FileSystemId: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.FileSystems) == 0 {
		return nil, fmt.Errorf("%w: file system %s", cloudcontrolbackend.ErrNotFound, id)
	}

	f := out.FileSystems[0]
	model := map[string]any{
		"FileSystemId": id, ccKeyArn: f.FileSystemArn, "Encrypted": f.Encrypted, "KmsKeyId": f.KmsKeyId,
		"PerformanceMode": string(f.PerformanceMode), ccKeyThroughput: string(f.ThroughputMode),
		ccKeyProvisioned: f.ProvisionedThroughputInMibps, "AvailabilityZoneName": f.AvailabilityZoneName,
	}

	if len(f.Tags) > 0 {
		m := make(map[string]string, len(f.Tags))
		for _, t := range f.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model[ccKeyFSTags] = tagsProperty(m)
	}

	if f.FileSystemProtection != nil && f.FileSystemProtection.ReplicationOverwriteProtection != "" {
		model[ccKeyProtection] = f.FileSystemProtection
	}

	h.readExtras(ctx, id, model)

	return ccCleanModel(model), nil
}

func (h *ccFileSystem) readExtras(ctx context.Context, id string, model map[string]any) {
	if lc, err := h.client.DescribeLifecycleConfiguration(ctx, &efs.DescribeLifecycleConfigurationInput{
		FileSystemId: aws.String(id),
	}); err == nil && len(lc.LifecyclePolicies) > 0 {
		model[ccKeyLifecycle] = lc.LifecyclePolicies
	}

	if bp, err := h.client.DescribeBackupPolicy(ctx, &efs.DescribeBackupPolicyInput{
		FileSystemId: aws.String(id),
	}); err == nil && bp.BackupPolicy != nil {
		model[ccKeyBackup] = bp.BackupPolicy
	}

	if fp, err := h.client.DescribeFileSystemPolicy(ctx, &efs.DescribeFileSystemPolicyInput{
		FileSystemId: aws.String(id),
	}); err == nil && fp.Policy != nil {
		model[ccKeyFSPolicy] = ccPolicyObject(*fp.Policy, false)
	}
}

func (h *ccFileSystem) Update(ctx context.Context, id string, current, desired map[string]any) error {
	mutable := []string{
		ccKeyFSTags, ccKeyLifecycle, ccKeyBackup, ccKeyFSPolicy, ccKeyThroughput, ccKeyProvisioned,
		ccKeyProtection, ccKeyBypass,
	}
	if err := ccRejectUnsupportedChanges(current, desired, mutable...); err != nil {
		return err
	}

	if len(ccChanged(current, desired, ccKeyThroughput, ccKeyProvisioned)) > 0 {
		in := &efs.UpdateFileSystemInput{
			FileSystemId: aws.String(id), ThroughputMode: efstypes.ThroughputMode(ccString(desired, ccKeyThroughput)),
		}
		if v, ok := desired[ccKeyProvisioned].(float64); ok {
			in.ProvisionedThroughputInMibps = aws.Float64(v)
		}

		if _, err := h.client.UpdateFileSystem(ctx, in); err != nil {
			return ccMapError(err)
		}
	}

	if err := h.applyExtras(
		ctx,
		id,
		ccChanged(current, desired, ccKeyLifecycle, ccKeyBackup, ccKeyFSPolicy, ccKeyProtection),
	); err != nil {
		return err
	}

	have, err := ccFSTagMap(current)
	if err != nil {
		return err
	}

	want, err := ccFSTagMap(desired)
	if err != nil {
		return err
	}

	add, remove := ccTagDelta(have, want)

	if len(add) > 0 {
		if _, err = h.client.TagResource(
			ctx,
			&efs.TagResourceInput{ResourceId: aws.String(id), Tags: ccEFSTags(add)},
		); err != nil {
			return ccMapError(err)
		}
	}

	if len(remove) > 0 {
		_, err = h.client.UntagResource(ctx, &efs.UntagResourceInput{ResourceId: aws.String(id), TagKeys: remove})

		return ccMapError(err)
	}

	return nil
}

func (h *ccFileSystem) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{FileSystemId: aws.String(id)})

	return ccMapError(err)
}

func (h *ccFileSystem) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := efs.NewDescribeFileSystemsPaginator(h.client, &efs.DescribeFileSystemsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, f := range out.FileSystems {
			ids = append(ids, aws.ToString(f.FileSystemId))
		}
	}

	return ids, nil
}

// --- AWS::Glue::Database ---

type ccGlueDatabase struct{ client *glue.Client }

const (
	ccKeyDBInput = "DatabaseInput"
	ccKeyDBName  = "DatabaseName"

	ccKeyCatalogID = "CatalogId"
)

func (h *ccGlueDatabase) Create(ctx context.Context, desired map[string]any) (string, error) {
	catalog := ccString(desired, ccKeyCatalogID)
	if catalog == "" {
		return "", fmt.Errorf("%w: CatalogId is required", cloudcontrolbackend.ErrValidation)
	}

	in, err := ccDecodeOptional[gluetypes.DatabaseInput](desired, ccKeyDBInput)
	if err != nil {
		return "", err
	}

	name := ccString(desired, ccKeyDBName)
	if name == "" {
		name = aws.ToString(in.Name)
	}

	if name == "" {
		name = ccGeneratedName("cc-db-")
	}

	in.Name = aws.String(name)

	if _, err = h.client.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &in, CatalogId: aws.String(catalog),
	}); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccGlueDatabase) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	d := out.Database
	input := map[string]any{
		ccKeyName: id, ccKeyDescription: d.Description, "LocationUri": d.LocationUri, "Parameters": d.Parameters,
		"TargetDatabase": d.TargetDatabase, "FederatedDatabase": d.FederatedDatabase,
		"CreateTableDefaultPermissions": d.CreateTableDefaultPermissions,
	}
	model := map[string]any{ccKeyDBName: id, ccKeyCatalogID: d.CatalogId, ccKeyDBInput: input}

	return ccCleanModel(model), nil
}

func (h *ccGlueDatabase) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, ccKeyDBInput); err != nil {
		return err
	}

	if len(ccChanged(current, desired, ccKeyDBInput)) > 0 {
		in, err := ccDecodeOptional[gluetypes.DatabaseInput](desired, ccKeyDBInput)
		if err != nil {
			return err
		}

		in.Name = aws.String(id)

		if _, err = h.client.UpdateDatabase(
			ctx,
			&glue.UpdateDatabaseInput{Name: aws.String(id), DatabaseInput: &in},
		); err != nil {
			return ccMapError(err)
		}
	}

	return nil
}

func (h *ccGlueDatabase) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteDatabase(ctx, &glue.DeleteDatabaseInput{Name: aws.String(id)})

	return ccMapError(err)
}

func (h *ccGlueDatabase) List(ctx context.Context) ([]string, error) {
	var names []string

	p := glue.NewGetDatabasesPaginator(h.client, &glue.GetDatabasesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, d := range out.DatabaseList {
			names = append(names, aws.ToString(d.Name))
		}
	}

	return names, nil
}
