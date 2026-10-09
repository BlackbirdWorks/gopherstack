package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccKeyClusterName = "ClusterName"
	ccKeyCapProv     = "CapacityProviders"
	ccKeyCapStrategy = "DefaultCapacityProviderStrategy"
	ccKeyClusterCfg  = "Configuration"
	ccKeySettings    = "ClusterSettings"
	ccKeyConnect     = "ServiceConnectDefaults"
)

// ccDropNulls removes nil values from decoded JSON so SDK structs render only the properties that are set.
func ccDropNulls(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if val == nil {
				delete(t, k)

				continue
			}

			t[k] = ccDropNulls(val)
		}

		return t
	case []any:
		for i, val := range t {
			t[i] = ccDropNulls(val)
		}

		return t
	default:
		return v
	}
}

func ccCleanModel(model map[string]any) map[string]any {
	out, _ := ccDropNulls(normalizeJSON(model)).(map[string]any)

	return out
}

// --- AWS::ECS::Cluster ---

type ccCluster struct{ client *ecs.Client }

func ccECSTags(m map[string]string) []ecstypes.Tag {
	out := make([]ecstypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, ecstypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

type ccClusterSpec struct {
	Configuration *ecstypes.ClusterConfiguration
	Connect       *ecstypes.ClusterServiceConnectDefaultsRequest
	Settings      []ecstypes.ClusterSetting
	CapProviders  []string
	Strategy      []ecstypes.CapacityProviderStrategyItem
}

func ccDecodeCluster(m map[string]any) (ccClusterSpec, error) {
	var (
		spec ccClusterSpec
		err  error
	)

	if raw, ok := m[ccKeyClusterCfg]; ok && raw != nil {
		var cfg ecstypes.ClusterConfiguration
		if cfg, err = ccDecode[ecstypes.ClusterConfiguration](raw); err != nil {
			return spec, err
		}

		spec.Configuration = &cfg
	}

	if spec.Settings, err = ccDecodeOptional[[]ecstypes.ClusterSetting](m, ccKeySettings); err != nil {
		return spec, err
	}

	if spec.CapProviders, err = ccDecodeOptional[[]string](m, ccKeyCapProv); err != nil {
		return spec, err
	}

	if spec.Strategy, err = ccDecodeOptional[[]ecstypes.CapacityProviderStrategyItem](m, ccKeyCapStrategy); err != nil {
		return spec, err
	}

	if raw, ok := m[ccKeyConnect]; ok && raw != nil {
		var connect ecstypes.ClusterServiceConnectDefaultsRequest
		if connect, err = ccDecode[ecstypes.ClusterServiceConnectDefaultsRequest](raw); err != nil {
			return spec, err
		}

		spec.Connect = &connect
	}

	return spec, nil
}

func ccDecodeOptional[T any](m map[string]any, key string) (T, error) {
	var zero T

	raw, ok := m[key]
	if !ok || raw == nil {
		return zero, nil
	}

	return ccDecode[T](raw)
}

func (h *ccCluster) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, ccKeyClusterName)
	if name == "" {
		name = ccGeneratedName("cc-cluster-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	spec, err := ccDecodeCluster(desired)
	if err != nil {
		return "", err
	}

	if _, err = h.client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(name), Tags: ccECSTags(tags), Settings: spec.Settings,
		Configuration: spec.Configuration, CapacityProviders: spec.CapProviders,
		DefaultCapacityProviderStrategy: spec.Strategy, ServiceConnectDefaults: spec.Connect,
	}); err != nil {
		return "", ccMapError(err)
	}

	return name, nil
}

func (h *ccCluster) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.DescribeClusters(ctx, &ecs.DescribeClustersInput{
		Clusters: []string{id},
		Include: []ecstypes.ClusterField{
			ecstypes.ClusterFieldTags, ecstypes.ClusterFieldSettings, ecstypes.ClusterFieldConfigurations,
		},
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.Clusters) == 0 || aws.ToString(out.Clusters[0].Status) == "INACTIVE" {
		return nil, fmt.Errorf("%w: cluster %s", cloudcontrolbackend.ErrNotFound, id)
	}

	c := out.Clusters[0]
	model := map[string]any{ccKeyClusterName: id, ccKeyArn: aws.ToString(c.ClusterArn)}

	if len(c.Settings) > 0 {
		model[ccKeySettings] = c.Settings
	}

	if c.Configuration != nil {
		model[ccKeyClusterCfg] = c.Configuration
	}

	if c.ServiceConnectDefaults != nil {
		model[ccKeyConnect] = c.ServiceConnectDefaults
	}

	if len(c.CapacityProviders) > 0 {
		model[ccKeyCapProv] = c.CapacityProviders
	}

	if len(c.DefaultCapacityProviderStrategy) > 0 {
		model[ccKeyCapStrategy] = c.DefaultCapacityProviderStrategy
	}

	if len(c.Tags) > 0 {
		m := make(map[string]string, len(c.Tags))
		for _, t := range c.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model[ccKeyTags] = tagsProperty(m)
	}

	return ccCleanModel(model), nil
}

func (h *ccCluster) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(
		current, desired, ccKeyTags, ccKeySettings, ccKeyClusterCfg, ccKeyCapProv, ccKeyCapStrategy, ccKeyConnect,
	); err != nil {
		return err
	}

	spec, err := ccDecodeCluster(desired)
	if err != nil {
		return err
	}

	if len(ccChanged(current, desired, ccKeySettings, ccKeyClusterCfg, ccKeyConnect)) > 0 {
		if _, err = h.client.UpdateCluster(ctx, &ecs.UpdateClusterInput{
			Cluster: aws.String(id), Settings: spec.Settings, Configuration: spec.Configuration,
			ServiceConnectDefaults: spec.Connect,
		}); err != nil {
			return ccMapError(err)
		}
	}

	if len(ccChanged(current, desired, ccKeyCapProv, ccKeyCapStrategy)) > 0 {
		if _, err = h.client.PutClusterCapacityProviders(ctx, &ecs.PutClusterCapacityProvidersInput{
			Cluster: aws.String(id), CapacityProviders: spec.CapProviders,
			DefaultCapacityProviderStrategy: spec.Strategy,
		}); err != nil {
			return ccMapError(err)
		}
	}

	arn := ccString(current, ccKeyArn)

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, e := h.client.TagResource(ctx, &ecs.TagResourceInput{ResourceArn: aws.String(arn), Tags: ccECSTags(m)})

			return e
		},
		func(keys []string) error {
			_, e := h.client.UntagResource(ctx, &ecs.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: keys})

			return e
		})
}

func (h *ccCluster) Delete(ctx context.Context, id string) error {
	if _, err := h.Read(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String(id)})

	return ccMapError(err)
}

func (h *ccCluster) List(ctx context.Context) ([]string, error) {
	var names []string

	p := ecs.NewListClustersPaginator(h.client, &ecs.ListClustersInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, arn := range out.ClusterArns {
			names = append(names, arn[strings.LastIndex(arn, "/")+1:])
		}
	}

	return names, nil
}

// --- AWS::ElasticLoadBalancingV2::TargetGroup ---

type ccTargetGroup struct{ client *elbv2.Client }

func ccELBTags(m map[string]string) []elbv2types.Tag {
	out := make([]elbv2types.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, elbv2types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

type ccTargetGroupSpec struct {
	HealthCheckEnabled         *bool
	HealthCheckIntervalSeconds *int32
	HealthCheckPath            *string
	HealthCheckPort            *string
	HealthCheckTimeoutSeconds  *int32
	HealthyThresholdCount      *int32
	UnhealthyThresholdCount    *int32
	Matcher                    *elbv2types.Matcher
	HealthCheckProtocol        string
}

func ccDecodeTargetGroup(m map[string]any) (ccTargetGroupSpec, error) {
	return ccDecode[ccTargetGroupSpec](m)
}

func (h *ccTargetGroup) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, ccKeyName)
	if name == "" {
		name = ccGeneratedName("cc-tg-")
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	hc, err := ccDecodeTargetGroup(desired)
	if err != nil {
		return "", err
	}

	in := &elbv2.CreateTargetGroupInput{
		Name: aws.String(name), Tags: ccELBTags(tags), HealthCheckEnabled: hc.HealthCheckEnabled,
		HealthCheckIntervalSeconds: hc.HealthCheckIntervalSeconds, HealthCheckPath: hc.HealthCheckPath,
		HealthCheckPort: hc.HealthCheckPort, HealthCheckProtocol: elbv2types.ProtocolEnum(hc.HealthCheckProtocol),
		HealthCheckTimeoutSeconds: hc.HealthCheckTimeoutSeconds, HealthyThresholdCount: hc.HealthyThresholdCount,
		UnhealthyThresholdCount: hc.UnhealthyThresholdCount, Matcher: hc.Matcher,
	}

	if v, ok := ccInt32(desired, "Port"); ok {
		in.Port = aws.Int32(v)
	}

	if v := ccString(desired, ccKeyVpcID); v != "" {
		in.VpcId = aws.String(v)
	}

	in.Protocol = elbv2types.ProtocolEnum(ccString(desired, "Protocol"))
	in.TargetType = elbv2types.TargetTypeEnum(ccString(desired, "TargetType"))
	in.IpAddressType = elbv2types.TargetGroupIpAddressTypeEnum(ccString(desired, "IpAddressType"))

	if v := ccString(desired, "ProtocolVersion"); v != "" {
		in.ProtocolVersion = aws.String(v)
	}

	out, err := h.client.CreateTargetGroup(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	return aws.ToString(out.TargetGroups[0].TargetGroupArn), nil
}

func (h *ccTargetGroup) describe(ctx context.Context, id string) (*elbv2types.TargetGroup, error) {
	out, err := h.client.DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{TargetGroupArns: []string{id}})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.TargetGroups) == 0 {
		return nil, fmt.Errorf("%w: target group %s", cloudcontrolbackend.ErrNotFound, id)
	}

	return &out.TargetGroups[0], nil
}

func (h *ccTargetGroup) Read(ctx context.Context, id string) (map[string]any, error) {
	g, err := h.describe(ctx, id)
	if err != nil {
		return nil, err
	}

	model := map[string]any{
		"TargetGroupArn": id, ccKeyName: aws.ToString(g.TargetGroupName), "Protocol": string(g.Protocol),
		"TargetType": string(g.TargetType), "HealthCheckEnabled": g.HealthCheckEnabled,
		"HealthCheckIntervalSeconds": g.HealthCheckIntervalSeconds, "HealthCheckPath": g.HealthCheckPath,
		"HealthCheckPort": g.HealthCheckPort, "HealthCheckProtocol": string(g.HealthCheckProtocol),
		"HealthCheckTimeoutSeconds": g.HealthCheckTimeoutSeconds, "HealthyThresholdCount": g.HealthyThresholdCount,
		"UnhealthyThresholdCount": g.UnhealthyThresholdCount, "Matcher": g.Matcher, "Port": g.Port,
		ccKeyVpcID: g.VpcId, "IpAddressType": string(g.IpAddressType), "ProtocolVersion": g.ProtocolVersion,
		"LoadBalancerArns": g.LoadBalancerArns,
	}

	model["TargetGroupName"] = aws.ToString(g.TargetGroupName)

	if i := strings.Index(id, "targetgroup/"); i >= 0 {
		model["TargetGroupFullName"] = id[i:]
	}

	tags, tagErr := h.client.DescribeTags(ctx, &elbv2.DescribeTagsInput{ResourceArns: []string{id}})
	if tagErr == nil && len(tags.TagDescriptions) > 0 && len(tags.TagDescriptions[0].Tags) > 0 {
		m := make(map[string]string)
		for _, t := range tags.TagDescriptions[0].Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model[ccKeyTags] = tagsProperty(m)
	}

	return ccCleanModel(model), nil
}

func (h *ccTargetGroup) Update(ctx context.Context, id string, current, desired map[string]any) error {
	mutable := []string{
		ccKeyTags, "HealthCheckEnabled", "HealthCheckIntervalSeconds", "HealthCheckPath", "HealthCheckPort",
		"HealthCheckProtocol", "HealthCheckTimeoutSeconds", "HealthyThresholdCount", "UnhealthyThresholdCount",
		"Matcher",
	}
	if err := ccRejectUnsupportedChanges(current, desired, mutable...); err != nil {
		return err
	}

	hc, err := ccDecodeTargetGroup(desired)
	if err != nil {
		return err
	}

	if len(ccChanged(current, desired, mutable[1:]...)) > 0 {
		if _, err = h.client.ModifyTargetGroup(ctx, &elbv2.ModifyTargetGroupInput{
			TargetGroupArn: aws.String(id), HealthCheckEnabled: hc.HealthCheckEnabled,
			HealthCheckIntervalSeconds: hc.HealthCheckIntervalSeconds, HealthCheckPath: hc.HealthCheckPath,
			HealthCheckPort:           hc.HealthCheckPort,
			HealthCheckProtocol:       elbv2types.ProtocolEnum(hc.HealthCheckProtocol),
			HealthCheckTimeoutSeconds: hc.HealthCheckTimeoutSeconds, HealthyThresholdCount: hc.HealthyThresholdCount,
			UnhealthyThresholdCount: hc.UnhealthyThresholdCount, Matcher: hc.Matcher,
		}); err != nil {
			return ccMapError(err)
		}
	}

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, e := h.client.AddTags(ctx, &elbv2.AddTagsInput{ResourceArns: []string{id}, Tags: ccELBTags(m)})

			return e
		},
		func(keys []string) error {
			_, e := h.client.RemoveTags(ctx, &elbv2.RemoveTagsInput{ResourceArns: []string{id}, TagKeys: keys})

			return e
		})
}

func (h *ccTargetGroup) Delete(ctx context.Context, id string) error {
	if _, err := h.describe(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteTargetGroup(ctx, &elbv2.DeleteTargetGroupInput{TargetGroupArn: aws.String(id)})

	return ccMapError(err)
}

func (h *ccTargetGroup) List(ctx context.Context) ([]string, error) {
	var arns []string

	p := elbv2.NewDescribeTargetGroupsPaginator(h.client, &elbv2.DescribeTargetGroupsInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, g := range out.TargetGroups {
			arns = append(arns, aws.ToString(g.TargetGroupArn))
		}
	}

	return arns, nil
}
