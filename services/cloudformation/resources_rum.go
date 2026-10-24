package cloudformation

import (
	"context"
	"fmt"
	"strconv"

	rumbackend "github.com/blackbirdworks/gopherstack/services/rum"
)

const (
	resTypeRUMAppMonitor = "AWS::RUM::AppMonitor"
	// rumDefaultSampleRate is the AppMonitorConfiguration.SessionSampleRate default when it is omitted.
	rumDefaultSampleRate = 0.1
)

func (rc *ResourceCreator) createRUMResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeRUMAppMonitor {
		return "", false, nil
	}

	id, err := rc.createRUMAppMonitor(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

func (rc *ResourceCreator) deleteRUMResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeRUMAppMonitor {
		return false, nil
	}

	if rc.backends.RUM == nil {
		return true, nil
	}

	err := rc.backends.RUM.Backend.CFNDelete(ctx, physicalID)
	if rumbackend.CFNNotFound(err) {
		return true, nil
	}

	return true, err
}

// createRUMAppMonitor provisions an AWS::RUM::AppMonitor. Ref returns the monitor Name and Fn::GetAtt Id the
// monitor ID, per the resource specification (primary identifier Name, sole attribute Id).
func (rc *ResourceCreator) createRUMAppMonitor(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RUM == nil {
		return logicalID + "-stub", nil
	}

	spec := rumSpecFromProps(props, params, physicalIDs)
	if spec.Name == "" {
		spec.Name = logicalID
	}

	id, err := rc.backends.RUM.Backend.CFNCreate(ctx, spec)
	if err != nil {
		return "", fmt.Errorf("create RUM app monitor %s: %w", spec.Name, err)
	}

	physicalIDs[logicalID+"/Id"] = id

	return spec.Name, nil
}

func (rc *ResourceCreator) updateRUMAppMonitor(
	ctx context.Context, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.RUM == nil {
		return nil
	}

	spec := rumSpecFromProps(props, params, physicalIDs)
	spec.Name = physicalID

	if err := rc.backends.RUM.Backend.CFNUpdate(ctx, spec); err != nil {
		return fmt.Errorf("update RUM app monitor %s: %w", physicalID, err)
	}

	return nil
}

func rumSpecFromProps(props map[string]any, params, physicalIDs map[string]string) rumbackend.CFNAppMonitor {
	spec := rumbackend.CFNAppMonitor{
		Name:         strProp(props, "Name", params, physicalIDs),
		Platform:     strProp(props, "Platform", params, physicalIDs),
		Domain:       strProp(props, "Domain", params, physicalIDs),
		DomainList:   strSliceProp(props["DomainList"], params, physicalIDs),
		CwLogEnabled: optionalBool(props["CwLogEnabled"], params, physicalIDs),
		Tags:         tagListProp(props, params, physicalIDs),
	}

	if ce := asMap(props["CustomEvents"]); ce != nil {
		spec.CustomEvents = strProp(ce, "Status", params, physicalIDs)
	}

	if js := asMap(asMap(props["DeobfuscationConfiguration"])["JavaScriptSourceMaps"]); js != nil {
		spec.SourceMaps = &rumbackend.SourceMaps{
			Status: strProp(js, "Status", params, physicalIDs),
			S3URI:  strProp(js, "S3Uri", params, physicalIDs),
		}
	}

	if rp := asMap(props["ResourcePolicy"]); rp != nil {
		spec.PolicyDocument = resolvedJSONProp(rp, "PolicyDocument", params, physicalIDs)
		spec.PolicyRevisionID = strProp(rp, "PolicyRevisionId", params, physicalIDs)
	}

	if cfg := asMap(props["AppMonitorConfiguration"]); cfg != nil {
		spec.Config = rumMonitorConfig(cfg, params, physicalIDs)
		spec.Destinations = rumDestinations(cfg["MetricDestinations"], params, physicalIDs)
	}

	return spec
}

func rumMonitorConfig(cfg map[string]any, params, physicalIDs map[string]string) *rumbackend.MonitorConfig {
	out := &rumbackend.MonitorConfig{
		AllowCookies:      optionalBool(cfg["AllowCookies"], params, physicalIDs),
		EnableXRay:        optionalBool(cfg["EnableXRay"], params, physicalIDs),
		ExcludedPages:     strSliceProp(cfg["ExcludedPages"], params, physicalIDs),
		FavoritePages:     strSliceProp(cfg["FavoritePages"], params, physicalIDs),
		IncludedPages:     strSliceProp(cfg["IncludedPages"], params, physicalIDs),
		Telemetries:       strSliceProp(cfg["Telemetries"], params, physicalIDs),
		SessionSampleRate: optionalFloat(cfg["SessionSampleRate"], params, physicalIDs),
	}

	if _, set := cfg["SessionSampleRate"]; !set {
		out.SessionSampleRate = rumDefaultSampleRate
	}

	if s := strProp(cfg, "GuestRoleArn", params, physicalIDs); s != "" {
		out.GuestRoleArn = &s
	}

	if s := strProp(cfg, "IdentityPoolId", params, physicalIDs); s != "" {
		out.IdentityPoolID = &s
	}

	return out
}

func rumDestinations(v any, params, physicalIDs map[string]string) []rumbackend.CFNMetricDestination {
	var out []rumbackend.CFNMetricDestination

	for _, item := range asList(v) {
		m := asMap(item)
		if m == nil {
			continue
		}

		d := rumbackend.CFNMetricDestination{
			Destination:    strProp(m, "Destination", params, physicalIDs),
			DestinationARN: strProp(m, "DestinationArn", params, physicalIDs),
			IAMRoleARN:     strProp(m, "IamRoleArn", params, physicalIDs),
		}

		for _, def := range asList(m["MetricDefinitions"]) {
			dm := asMap(def)
			if dm == nil {
				continue
			}

			d.Definitions = append(d.Definitions, rumbackend.WireMetricRequest{
				Name:          strProp(dm, "Name", params, physicalIDs),
				EventPattern:  strProp(dm, "EventPattern", params, physicalIDs),
				Namespace:     strProp(dm, "Namespace", params, physicalIDs),
				UnitLabel:     strProp(dm, "UnitLabel", params, physicalIDs),
				ValueKey:      strProp(dm, "ValueKey", params, physicalIDs),
				DimensionKeys: stringMapProp(dm["DimensionKeys"], params, physicalIDs),
			})
		}

		out = append(out, d)
	}

	return out
}

// optionalBool reads a boolean property that may be a JSON bool or the string a Ref/parameter resolves to.
func optionalBool(v any, params, physicalIDs map[string]string) *bool {
	if v == nil {
		return nil
	}

	if b, ok := v.(bool); ok {
		return &b
	}

	b, err := strconv.ParseBool(resolve(v, params, physicalIDs))
	if err != nil {
		return nil
	}

	return &b
}

func optionalFloat(v any, params, physicalIDs map[string]string) float64 {
	if f, ok := v.(float64); ok {
		return f
	}

	f, _ := strconv.ParseFloat(resolve(v, params, physicalIDs), 64)

	return f
}
