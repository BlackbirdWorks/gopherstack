package cloudformation

import (
	"context"
	"fmt"
	"maps"

	oambackend "github.com/blackbirdworks/gopherstack/services/oam"
)

const (
	resTypeOAMSink = "AWS::Oam::Sink"
	resTypeOAMLink = "AWS::Oam::Link"
)

func (rc *ResourceCreator) createOAMResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeOAMSink:
		id, err := rc.createOAMSink(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeOAMLink:
		id, err := rc.createOAMLink(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteOAMResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	var del func(context.Context, string) error

	switch resourceType {
	case resTypeOAMSink:
		if rc.backends.OAM != nil {
			del = rc.backends.OAM.Backend.DeleteSink
		}
	case resTypeOAMLink:
		if rc.backends.OAM != nil {
			del = rc.backends.OAM.Backend.DeleteLink
		}
	default:
		return false, nil
	}

	if del == nil {
		return true, nil
	}

	return true, ignoreNotFound(del(ctx, physicalID), oambackend.ErrResourceNotFound)
}

// createOAMSink provisions an AWS::Oam::Sink. Ref and Fn::GetAtt Arn return the sink ARN (primary identifier Arn).
func (rc *ResourceCreator) createOAMSink(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.OAM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	be := rc.backends.OAM.Backend

	sink, err := be.CreateSink(ctx, name, oamTags(props, params, physicalIDs))
	if err != nil {
		return "", fmt.Errorf("create Oam sink %s: %w", name, err)
	}

	if policy := resolvedJSONProp(props, "Policy", params, physicalIDs); policy != "" {
		if _, err = be.PutSinkPolicy(ctx, sink.ARN, policy); err != nil {
			_ = be.DeleteSink(ctx, sink.ARN)

			return "", fmt.Errorf("put Oam sink policy for %s: %w", name, err)
		}
	}

	return sink.ARN, nil
}

// createOAMLink provisions an AWS::Oam::Link. Ref and Fn::GetAtt Arn return the link ARN; Label is stashed.
func (rc *ResourceCreator) createOAMLink(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.OAM == nil {
		return logicalID + "-stub", nil
	}

	link, err := rc.backends.OAM.Backend.CreateLink(ctx, oambackend.LinkInput{
		SinkARN:       strProp(props, "SinkIdentifier", params, physicalIDs),
		LabelTemplate: strProp(props, "LabelTemplate", params, physicalIDs),
		ResourceTypes: strSliceProp(props["ResourceTypes"], params, physicalIDs),
		Config:        oamLinkConfig(props, params, physicalIDs),
		Tags:          oamTags(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create Oam link %s: %w", logicalID, err)
	}

	physicalIDs[logicalID+"/Label"] = link.Label

	return link.ARN, nil
}

func (rc *ResourceCreator) updateOAMResource(
	ctx context.Context, resourceType, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	if rc.backends.OAM == nil {
		return nil
	}

	be := rc.backends.OAM.Backend
	tags := oamTags(props, params, physicalIDs)

	switch resourceType {
	case resTypeOAMSink:
		if policy := resolvedJSONProp(props, "Policy", params, physicalIDs); policy != "" {
			if _, err := be.PutSinkPolicy(ctx, physicalID, policy); err != nil {
				return fmt.Errorf("update Oam sink %s: %w", physicalID, err)
			}
		}
	case resTypeOAMLink:
		cfg := oamLinkConfig(props, params, physicalIDs)
		types := strSliceProp(props["ResourceTypes"], params, physicalIDs)

		if _, err := be.UpdateLink(ctx, physicalID, types, &cfg); err != nil {
			return fmt.Errorf("update Oam link %s: %w", physicalID, err)
		}
	}

	return syncOAMTags(ctx, be, physicalID, tags)
}

func syncOAMTags(ctx context.Context, be *oambackend.InMemoryBackend, arn string, want map[string]string) error {
	current, err := be.ListTagsForResource(ctx, arn)
	if err != nil {
		return err
	}

	var drop []string

	for k := range current {
		if _, keep := want[k]; !keep {
			drop = append(drop, k)
		}
	}

	if len(drop) > 0 {
		if err = be.UntagResource(ctx, arn, drop); err != nil {
			return err
		}
	}

	if len(want) > 0 {
		return be.TagResource(ctx, arn, maps.Clone(want))
	}

	return nil
}

// oamTags reads the Map-typed Tags property of the Oam resources.
func oamTags(props map[string]any, params, physicalIDs map[string]string) map[string]string {
	return strMapProp(props, "Tags", params, physicalIDs)
}

func oamLinkConfig(props map[string]any, params, physicalIDs map[string]string) oambackend.LinkConfig {
	cfg := asMap(props["LinkConfiguration"])

	var out oambackend.LinkConfig

	if f := strProp(asMap(cfg["LogGroupConfiguration"]), "Filter", params, physicalIDs); f != "" {
		out.LogGroupFilter = &f
	}

	if f := strProp(asMap(cfg["MetricConfiguration"]), "Filter", params, physicalIDs); f != "" {
		out.MetricFilter = &f
	}

	return out
}
