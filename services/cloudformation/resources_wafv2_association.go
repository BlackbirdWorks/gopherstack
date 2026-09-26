package cloudformation

import (
	"context"
	"fmt"
	"strings"
)

const resTypeWAFv2WebACLAssociation = "AWS::WAFv2::WebACLAssociation"

// createWAFv2AssociationResource handles AWS::WAFv2::WebACLAssociation.
// Returns handled=false when resourceType isn't that type.
func (rc *ResourceCreator) createWAFv2AssociationResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeWAFv2WebACLAssociation {
		return "", false, nil
	}

	id, err := rc.createWAFv2WebACLAssociation(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteWAFv2AssociationResource handles deletion for
// AWS::WAFv2::WebACLAssociation. Returns handled=false otherwise.
func (rc *ResourceCreator) deleteWAFv2AssociationResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeWAFv2WebACLAssociation {
		return false, nil
	}

	if rc.backends.WAFv2 == nil {
		return true, nil
	}

	resourceARN, _, ok := strings.Cut(physicalID, "|")
	if !ok {
		return true, nil
	}

	return true, rc.backends.WAFv2.Backend.DisassociateWebACL(ctx, resourceARN)
}

// ---- AWS::WAFv2::WebACLAssociation ----
// AWS's Template Reference page for this type documents Ref as
// "name|id|scope", which is the format used by AWS::WAFv2::WebACL's own Ref
// (see resources_wafv2.go) -- this resource's own properties are just
// ResourceArn/WebACLArn (required, createOnly, and, per the CloudFormation
// registry schema for this type, its actual primaryIdentifier), so the docs
// text is a copy/paste error from a neighboring page, the same class of bug
// already logged for AWS::Glue::Registry in this file's PARITY.md. This
// backend instead uses ResourceArn|WebACLArn (the schema's own
// primaryIdentifier, pipe-joined per CloudFormation's composite-key
// convention) as Ref/physical ID.

func (rc *ResourceCreator) createWAFv2WebACLAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.WAFv2 == nil {
		return logicalID + "-stub", nil
	}

	resourceARN := strProp(props, "ResourceArn", params, physicalIDs)
	webACLARN := strProp(props, "WebACLArn", params, physicalIDs)

	if err := rc.backends.WAFv2.Backend.AssociateWebACL(ctx, webACLARN, resourceARN); err != nil {
		return "", fmt.Errorf("associate WAFv2 WebACL %s with %s: %w", webACLARN, resourceARN, err)
	}

	return resourceARN + "|" + webACLARN, nil
}
