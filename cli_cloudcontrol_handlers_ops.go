package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/google/uuid"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccKeyZoneConfig = "HostedZoneConfig"
	ccKeyZoneTags   = "HostedZoneTags"
	ccKeyZoneVPCs   = "VPCs"
)

// --- AWS::Route53::HostedZone ---

type ccHostedZone struct{ client *route53.Client }

func ccZoneID(id string) string {
	return strings.TrimPrefix(id, "/hostedzone/")
}

type ccZoneVPC struct {
	VPCId     string `json:"VPCId"`
	VPCRegion string `json:"VPCRegion"`
}

func ccZoneTagMap(m map[string]any) (map[string]string, error) {
	tags, err := ccDecode[ccTags](m[ccKeyZoneTags])
	if err != nil {
		return nil, err
	}

	return tags.toMap(), nil
}

func ccR53Tags(m map[string]string) []r53types.Tag {
	out := make([]r53types.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, r53types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccHostedZone) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, ccKeyName)
	if name == "" {
		return "", fmt.Errorf("%w: Name is required", cloudcontrolbackend.ErrValidation)
	}

	cfg, err := ccDecodeOptional[r53types.HostedZoneConfig](desired, ccKeyZoneConfig)
	if err != nil {
		return "", err
	}

	vpcs, err := ccDecodeOptional[[]ccZoneVPC](desired, ccKeyZoneVPCs)
	if err != nil {
		return "", err
	}

	tags, err := ccZoneTagMap(desired)
	if err != nil {
		return "", err
	}

	in := &route53.CreateHostedZoneInput{
		Name: aws.String(name), CallerReference: aws.String(uuid.NewString()), HostedZoneConfig: &cfg,
	}

	if len(vpcs) > 0 {
		cfg.PrivateZone = true
		in.VPC = &r53types.VPC{VPCId: aws.String(vpcs[0].VPCId), VPCRegion: r53types.VPCRegion(vpcs[0].VPCRegion)}
	}

	out, err := h.client.CreateHostedZone(ctx, in)
	if err != nil {
		return "", ccMapError(err)
	}

	id := ccZoneID(aws.ToString(out.HostedZone.Id))

	for _, v := range vpcs[min(1, len(vpcs)):] {
		if _, err = h.client.AssociateVPCWithHostedZone(ctx, &route53.AssociateVPCWithHostedZoneInput{
			HostedZoneId: aws.String(id),
			VPC:          &r53types.VPC{VPCId: aws.String(v.VPCId), VPCRegion: r53types.VPCRegion(v.VPCRegion)},
		}); err != nil {
			return "", ccMapError(err)
		}
	}

	return id, ccMapError(h.changeTags(ctx, id, tags, nil))
}

func (h *ccHostedZone) changeTags(ctx context.Context, id string, add map[string]string, remove []string) error {
	if len(add) == 0 && len(remove) == 0 {
		return nil
	}

	_, err := h.client.ChangeTagsForResource(ctx, &route53.ChangeTagsForResourceInput{
		ResourceType: r53types.TagResourceTypeHostedzone, ResourceId: aws.String(id),
		AddTags: ccR53Tags(add), RemoveTagKeys: remove,
	})

	return err
}

func (h *ccHostedZone) Read(ctx context.Context, id string) (map[string]any, error) {
	out, err := h.client.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: aws.String(id)})
	if err != nil {
		return nil, ccMapError(err)
	}

	z := out.HostedZone
	model := map[string]any{"Id": id, ccKeyName: aws.ToString(z.Name)}

	if z.Config != nil && aws.ToString(z.Config.Comment) != "" {
		model[ccKeyZoneConfig] = map[string]any{"Comment": aws.ToString(z.Config.Comment)}
	}

	if out.DelegationSet != nil {
		model["NameServers"] = out.DelegationSet.NameServers
	}

	if len(out.VPCs) > 0 {
		vpcs := make([]ccZoneVPC, 0, len(out.VPCs))
		for _, v := range out.VPCs {
			vpcs = append(vpcs, ccZoneVPC{VPCId: aws.ToString(v.VPCId), VPCRegion: string(v.VPCRegion)})
		}

		model[ccKeyZoneVPCs] = vpcs
	}

	if tags, tagErr := h.client.ListTagsForResource(ctx, &route53.ListTagsForResourceInput{
		ResourceType: r53types.TagResourceTypeHostedzone, ResourceId: aws.String(id),
	}); tagErr == nil && tags.ResourceTagSet != nil && len(tags.ResourceTagSet.Tags) > 0 {
		m := make(map[string]string, len(tags.ResourceTagSet.Tags))
		for _, t := range tags.ResourceTagSet.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model[ccKeyZoneTags] = tagsProperty(m)
	}

	return ccCleanModel(model), nil
}

func (h *ccHostedZone) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, ccKeyZoneConfig, ccKeyZoneTags); err != nil {
		return err
	}

	if len(ccChanged(current, desired, ccKeyZoneConfig)) > 0 {
		cfg, err := ccDecodeOptional[r53types.HostedZoneConfig](desired, ccKeyZoneConfig)
		if err != nil {
			return err
		}

		if _, err = h.client.UpdateHostedZoneComment(ctx, &route53.UpdateHostedZoneCommentInput{
			Id: aws.String(id), Comment: cfg.Comment,
		}); err != nil {
			return ccMapError(err)
		}
	}

	have, err := ccZoneTagMap(current)
	if err != nil {
		return err
	}

	want, err := ccZoneTagMap(desired)
	if err != nil {
		return err
	}

	add, remove := ccTagDelta(have, want)

	return ccMapError(h.changeTags(ctx, id, add, remove))
}

func (h *ccHostedZone) Delete(ctx context.Context, id string) error {
	_, err := h.client.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: aws.String(id)})

	return ccMapError(err)
}

func (h *ccHostedZone) List(ctx context.Context) ([]string, error) {
	var ids []string

	p := route53.NewListHostedZonesPaginator(h.client, &route53.ListHostedZonesInput{})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, z := range out.HostedZones {
			ids = append(ids, ccZoneID(aws.ToString(z.Id)))
		}
	}

	return ids, nil
}

// --- AWS::CloudWatch::Alarm ---

type ccAlarm struct{ client *cloudwatch.Client }

// ccAlarmSpec holds the AWS::CloudWatch::Alarm properties that map onto PutMetricAlarm.
type ccAlarmSpec struct {
	AlarmDescription                 *string
	ActionsEnabled                   *bool
	AlarmActions                     []string
	OKActions                        []string
	InsufficientDataActions          []string
	ComparisonOperator               string
	DatapointsToAlarm                *int32
	Dimensions                       []cwtypes.Dimension
	EvaluationPeriods                *int32
	EvaluateLowSampleCountPercentile *string
	ExtendedStatistic                *string
	MetricName                       *string
	Namespace                        *string
	Period                           *int32
	Statistic                        string
	Threshold                        *float64
	ThresholdMetricID                *string `json:"ThresholdMetricId"`
	TreatMissingData                 *string
	Unit                             string
	Metrics                          []cwtypes.MetricDataQuery
}

func ccCWTags(m map[string]string) []cwtypes.Tag {
	out := make([]cwtypes.Tag, 0, len(m))
	for k, v := range m {
		out = append(out, cwtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	return out
}

func (h *ccAlarm) put(ctx context.Context, name string, desired map[string]any, tags map[string]string) error {
	spec, err := ccDecode[ccAlarmSpec](desired)
	if err != nil {
		return err
	}

	_, err = h.client.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:               aws.String(name),
		AlarmDescription:        spec.AlarmDescription,
		ActionsEnabled:          spec.ActionsEnabled,
		AlarmActions:            spec.AlarmActions,
		OKActions:               spec.OKActions,
		InsufficientDataActions: spec.InsufficientDataActions,
		ComparisonOperator: cwtypes.ComparisonOperator(
			spec.ComparisonOperator,
		),
		DatapointsToAlarm:                spec.DatapointsToAlarm,
		Dimensions:                       spec.Dimensions,
		EvaluationPeriods:                spec.EvaluationPeriods,
		EvaluateLowSampleCountPercentile: spec.EvaluateLowSampleCountPercentile,
		ExtendedStatistic:                spec.ExtendedStatistic,
		MetricName:                       spec.MetricName,
		Namespace:                        spec.Namespace,
		Period:                           spec.Period,
		Statistic:                        cwtypes.Statistic(spec.Statistic),
		Threshold:                        spec.Threshold,
		ThresholdMetricId:                spec.ThresholdMetricID,
		TreatMissingData:                 spec.TreatMissingData,
		Unit:                             cwtypes.StandardUnit(spec.Unit),
		Metrics:                          spec.Metrics,
		Tags:                             ccCWTags(tags),
	})

	return ccMapError(err)
}

func (h *ccAlarm) Create(ctx context.Context, desired map[string]any) (string, error) {
	name := ccString(desired, "AlarmName")
	if name == "" {
		name = ccGeneratedName("cc-alarm-")
	}

	if _, err := h.describe(ctx, name); err == nil {
		return "", fmt.Errorf("%w: alarm %s", cloudcontrolbackend.ErrAlreadyExists, name)
	}

	tags, err := ccModelTags(desired)
	if err != nil {
		return "", err
	}

	return name, h.put(ctx, name, desired, tags)
}

func (h *ccAlarm) describe(ctx context.Context, name string) (*cwtypes.MetricAlarm, error) {
	out, err := h.client.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{
		AlarmNames: []string{name}, AlarmTypes: []cwtypes.AlarmType{cwtypes.AlarmTypeMetricAlarm},
	})
	if err != nil {
		return nil, ccMapError(err)
	}

	if len(out.MetricAlarms) == 0 {
		return nil, fmt.Errorf("%w: alarm %s", cloudcontrolbackend.ErrNotFound, name)
	}

	return &out.MetricAlarms[0], nil
}

func (h *ccAlarm) Read(ctx context.Context, id string) (map[string]any, error) {
	a, err := h.describe(ctx, id)
	if err != nil {
		return nil, err
	}

	model := map[string]any{
		"AlarmName": id, ccKeyArn: aws.ToString(a.AlarmArn), "AlarmDescription": a.AlarmDescription,
		"ActionsEnabled": a.ActionsEnabled, "AlarmActions": a.AlarmActions, "OKActions": a.OKActions,
		"InsufficientDataActions": a.InsufficientDataActions, "ComparisonOperator": string(a.ComparisonOperator),
		"DatapointsToAlarm": a.DatapointsToAlarm, "Dimensions": a.Dimensions, "EvaluationPeriods": a.EvaluationPeriods,
		"EvaluateLowSampleCountPercentile": a.EvaluateLowSampleCountPercentile,
		"ExtendedStatistic":                a.ExtendedStatistic, "MetricName": a.MetricName, "Namespace": a.Namespace,
		"Period": a.Period, "Statistic": string(a.Statistic), "Threshold": a.Threshold,
		"ThresholdMetricId": a.ThresholdMetricId, "TreatMissingData": a.TreatMissingData, "Unit": string(a.Unit),
		"Metrics": a.Metrics,
	}

	if tags, tagErr := h.client.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{
		ResourceARN: a.AlarmArn,
	}); tagErr == nil && len(tags.Tags) > 0 {
		m := make(map[string]string, len(tags.Tags))
		for _, t := range tags.Tags {
			m[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}

		model[ccKeyTags] = tagsProperty(m)
	}

	return ccCleanModel(model), nil
}

func (h *ccAlarm) Update(ctx context.Context, id string, current, desired map[string]any) error {
	if err := ccRejectUnsupportedChanges(current, desired, h.mutable()...); err != nil {
		return err
	}

	if err := h.put(ctx, id, desired, nil); err != nil {
		return err
	}

	arn := ccString(current, ccKeyArn)

	return ccSyncModelTags(current, desired,
		func(m map[string]string) error {
			_, e := h.client.TagResource(
				ctx,
				&cloudwatch.TagResourceInput{ResourceARN: aws.String(arn), Tags: ccCWTags(m)},
			)

			return e
		},
		func(keys []string) error {
			_, e := h.client.UntagResource(
				ctx,
				&cloudwatch.UntagResourceInput{ResourceARN: aws.String(arn), TagKeys: keys},
			)

			return e
		})
}

func (*ccAlarm) mutable() []string {
	return []string{
		ccKeyTags,
		"AlarmDescription",
		"ActionsEnabled",
		"AlarmActions",
		"OKActions",
		"InsufficientDataActions",
		"ComparisonOperator",
		"DatapointsToAlarm",
		"Dimensions",
		"EvaluationPeriods",
		"EvaluateLowSampleCountPercentile",
		"ExtendedStatistic",
		"MetricName",
		"Namespace",
		"Period",
		"Statistic",
		"Threshold",
		"ThresholdMetricId",
		"TreatMissingData",
		"Unit",
		"Metrics",
	}
}

func (h *ccAlarm) Delete(ctx context.Context, id string) error {
	if _, err := h.describe(ctx, id); err != nil {
		return err
	}

	_, err := h.client.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: []string{id}})

	return ccMapError(err)
}

func (h *ccAlarm) List(ctx context.Context) ([]string, error) {
	var names []string

	p := cloudwatch.NewDescribeAlarmsPaginator(h.client, &cloudwatch.DescribeAlarmsInput{
		AlarmTypes: []cwtypes.AlarmType{cwtypes.AlarmTypeMetricAlarm},
	})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, ccMapError(err)
		}

		for _, a := range out.MetricAlarms {
			names = append(names, aws.ToString(a.AlarmName))
		}
	}

	return names, nil
}
