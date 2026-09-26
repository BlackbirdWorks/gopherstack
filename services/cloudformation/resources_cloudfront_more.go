package cloudformation

import (
	"errors"
	"fmt"

	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const (
	resTypeCFOriginRequestPolicy        = "AWS::CloudFront::OriginRequestPolicy"
	resTypeCFKeyGroup                   = "AWS::CloudFront::KeyGroup"
	resTypeCFPublicKey                  = "AWS::CloudFront::PublicKey"
	resTypeCFOAI                        = "AWS::CloudFront::CloudFrontOriginAccessIdentity"
	resTypeCFKeyValueStore              = "AWS::CloudFront::KeyValueStore"
	resTypeCFContinuousDeploymentPolicy = "AWS::CloudFront::ContinuousDeploymentPolicy"
)

// ---- CloudFront OriginRequestPolicy ----

func (rc *ResourceCreator) createCFOriginRequestPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg, _ := props["OriginRequestPolicyConfig"].(map[string]any)

	name := strProp(cfg, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	policy, err := rc.backends.CloudFront.Backend.CreateOriginRequestPolicy(
		name, strProp(cfg, "Comment", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront origin request policy %s: %w", name, err)
	}

	physicalIDs[logicalID+"/LastModifiedTime"] = policy.LastModifiedTime

	return policy.ID, nil
}

func (rc *ResourceCreator) deleteCFOriginRequestPolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteOriginRequestPolicy(id)
	if errors.Is(err, cloudfrontbackend.ErrOriginRequestPolicyNotFound) {
		return nil
	}

	return err
}

// ---- CloudFront KeyGroup ----

func (rc *ResourceCreator) createCFKeyGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg, _ := props["KeyGroupConfig"].(map[string]any)

	name := strProp(cfg, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	kg, err := rc.backends.CloudFront.Backend.CreateKeyGroup(
		name,
		strProp(cfg, "Comment", params, physicalIDs),
		strSliceProp(cfg["Items"], params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront key group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/LastModifiedTime"] = kg.LastModifiedTime

	return kg.ID, nil
}

func (rc *ResourceCreator) deleteCFKeyGroup(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteKeyGroup(id)
	if errors.Is(err, cloudfrontbackend.ErrKeyGroupNotFound) {
		return nil
	}

	return err
}

// ---- CloudFront PublicKey ----

func (rc *ResourceCreator) createCFPublicKey(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg, _ := props["PublicKeyConfig"].(map[string]any)

	name := strProp(cfg, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	callerRef := strProp(cfg, "CallerReference", params, physicalIDs)
	if callerRef == "" {
		callerRef = logicalID
	}

	pk, err := rc.backends.CloudFront.Backend.CreatePublicKey(
		callerRef, name,
		strProp(cfg, "Comment", params, physicalIDs),
		strProp(cfg, "EncodedKey", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront public key %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CreatedTime"] = pk.CreatedTime

	return pk.ID, nil
}

func (rc *ResourceCreator) deleteCFPublicKey(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeletePublicKey(id)
	if errors.Is(err, cloudfrontbackend.ErrPublicKeyNotFound) {
		return nil
	}

	return err
}

// ---- CloudFront CloudFrontOriginAccessIdentity ----

func (rc *ResourceCreator) createCFOAI(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg, _ := props["CloudFrontOriginAccessIdentityConfig"].(map[string]any)

	callerRef := strProp(cfg, "CallerReference", params, physicalIDs)
	if callerRef == "" {
		callerRef = logicalID
	}

	oai, err := rc.backends.CloudFront.Backend.CreateOAI(callerRef, strProp(cfg, "Comment", params, physicalIDs))
	if err != nil {
		return "", fmt.Errorf("create CloudFront origin access identity: %w", err)
	}

	physicalIDs[logicalID+"/S3CanonicalUserId"] = oai.S3CanonicalUserID

	return oai.ID, nil
}

func (rc *ResourceCreator) deleteCFOAI(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteOAI(id)
	if errors.Is(err, cloudfrontbackend.ErrOAINotFound) {
		return nil
	}

	return err
}

// ---- CloudFront RealtimeLogConfig ----

func realtimeLogEndPointsProp(
	props map[string]any, params, physicalIDs map[string]string,
) []cloudfrontbackend.RealtimeLogEndPoint {
	raw, ok := props["EndPoints"].([]any)
	if !ok {
		return nil
	}

	out := make([]cloudfrontbackend.RealtimeLogEndPoint, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		ksc, _ := m["KinesisStreamConfig"].(map[string]any)

		out = append(out, cloudfrontbackend.RealtimeLogEndPoint{
			StreamType: strProp(m, "StreamType", params, physicalIDs),
			RoleARN:    strProp(ksc, "RoleArn", params, physicalIDs),
			StreamARN:  strProp(ksc, "StreamArn", params, physicalIDs),
		})
	}

	return out
}

func (rc *ResourceCreator) createCFRealtimeLogConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cfg, err := rc.backends.CloudFront.Backend.CreateRealtimeLogConfig(
		name,
		int64(intProp(props, "SamplingRate")),
		strSliceProp(props["Fields"], params, physicalIDs),
		realtimeLogEndPointsProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront realtime log config %s: %w", name, err)
	}

	return cfg.ARN, nil
}

func (rc *ResourceCreator) deleteCFRealtimeLogConfig(arn string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteRealtimeLogConfig(arn)
	if errors.Is(err, cloudfrontbackend.ErrRealtimeLogConfigNotFound) {
		return nil
	}

	return err
}

// ---- CloudFront KeyValueStore ----

func (rc *ResourceCreator) createCFKeyValueStore(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	kvs, err := rc.backends.CloudFront.Backend.CreateKeyValueStore(
		name,
		strProp(props, "Comment", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront key value store %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = kvs.ARN
	physicalIDs[logicalID+"/Id"] = kvs.ID
	physicalIDs[logicalID+"/Status"] = kvs.Status

	return kvs.Name, nil
}

func (rc *ResourceCreator) deleteCFKeyValueStore(name string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteKeyValueStore(name)
	if errors.Is(err, cloudfrontbackend.ErrKeyValueStoreNotFound) {
		return nil
	}

	return err
}

// ---- CloudFront ContinuousDeploymentPolicy ----

func (rc *ResourceCreator) createCFContinuousDeploymentPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudFront == nil {
		return logicalID + "-stub", nil
	}

	cfg, _ := props["ContinuousDeploymentPolicyConfig"].(map[string]any)

	policy, err := rc.backends.CloudFront.Backend.CreateContinuousDeploymentPolicyWithConfig(
		boolProp(cfg, "Enabled"),
		strSliceProp(cfg["StagingDistributionDnsNames"], params, physicalIDs),
		cloudfrontbackend.ContinuousDeploymentTrafficConfig{},
	)
	if err != nil {
		return "", fmt.Errorf("create CloudFront continuous deployment policy: %w", err)
	}

	physicalIDs[logicalID+"/LastModifiedTime"] = policy.LastModifiedTime

	return policy.ID, nil
}

func (rc *ResourceCreator) deleteCFContinuousDeploymentPolicy(id string) error {
	if rc.backends.CloudFront == nil {
		return nil
	}

	err := rc.backends.CloudFront.Backend.DeleteContinuousDeploymentPolicy(id)
	if errors.Is(err, cloudfrontbackend.ErrContinuousDeploymentPolicyNotFound) {
		return nil
	}

	return err
}
