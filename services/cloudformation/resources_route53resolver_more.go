package cloudformation

import (
	"context"
	"fmt"
	"strconv"

	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
)

const (
	resTypeR53RResolverRuleAssoc  = "AWS::Route53Resolver::ResolverRuleAssociation"
	resTypeR53RFirewallDomainList = "AWS::Route53Resolver::FirewallDomainList"
	resTypeR53RFirewallRuleGroup  = "AWS::Route53Resolver::FirewallRuleGroup"
	resTypeR53RFirewallRGAssoc    = "AWS::Route53Resolver::FirewallRuleGroupAssociation"
	resTypeR53RQueryLogConfig     = "AWS::Route53Resolver::ResolverQueryLoggingConfig"
	resTypeR53RQueryLogConfigAssc = "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation"
	resTypeR53ROutpostResolver    = "AWS::Route53Resolver::OutpostResolver"
)

// createRoute53ResolverMoreResource handles the Route53Resolver resource
// types listed above, added after resources_route53.go's own budget was
// spent. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createRoute53ResolverMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeR53RResolverRuleAssoc:
		id, err := rc.createR53RResolverRuleAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53RFirewallDomainList:
		id, err := rc.createR53RFirewallDomainList(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53RFirewallRuleGroup:
		id, err := rc.createR53RFirewallRuleGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53RFirewallRGAssoc:
		id, err := rc.createR53RFirewallRuleGroupAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53RQueryLogConfig:
		id, err := rc.createR53RResolverQueryLogConfig(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53RQueryLogConfigAssc:
		id, err := rc.createR53RResolverQueryLogConfigAssociation(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeR53ROutpostResolver:
		id, err := rc.createR53ROutpostResolver(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteRoute53ResolverMoreResource handles deletion for the types described
// in createRoute53ResolverMoreResource.
func (rc *ResourceCreator) deleteRoute53ResolverMoreResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if rc.backends.Route53Resolver == nil {
		switch resourceType {
		case resTypeR53RResolverRuleAssoc, resTypeR53RFirewallDomainList, resTypeR53RFirewallRuleGroup,
			resTypeR53RFirewallRGAssoc, resTypeR53RQueryLogConfig, resTypeR53RQueryLogConfigAssc,
			resTypeR53ROutpostResolver:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.Route53Resolver.Backend

	switch resourceType {
	case resTypeR53RResolverRuleAssoc:
		return true, rc.deleteR53RResolverRuleAssociation(ctx, physicalID)
	case resTypeR53RFirewallDomainList:
		_, err := b.DeleteFirewallDomainList(ctx, physicalID)

		return true, ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	case resTypeR53RFirewallRuleGroup:
		_, err := b.DeleteFirewallRuleGroup(ctx, physicalID)

		return true, ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	case resTypeR53RFirewallRGAssoc:
		_, err := b.DisassociateFirewallRuleGroup(ctx, physicalID)

		return true, ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	case resTypeR53RQueryLogConfig:
		_, err := b.DeleteResolverQueryLogConfig(ctx, physicalID)

		return true, ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	case resTypeR53RQueryLogConfigAssc:
		return true, rc.deleteR53RResolverQueryLogConfigAssociation(ctx, physicalID)
	case resTypeR53ROutpostResolver:
		_, err := b.DeleteOutpostResolver(ctx, physicalID)

		return true, ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	default:
		return false, nil
	}
}

// deleteR53RResolverRuleAssociation looks the association up by ID to
// recover the (ResolverRuleId, VPCId) pair DisassociateResolverRule actually
// keys on -- unlike DisassociateFirewallRuleGroup, it isn't ID-keyed.
func (rc *ResourceCreator) deleteR53RResolverRuleAssociation(ctx context.Context, id string) error {
	b := rc.backends.Route53Resolver.Backend

	assoc, err := b.GetResolverRuleAssociation(ctx, id)
	if err != nil {
		return ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	}

	_, err = b.DisassociateResolverRule(ctx, assoc.ResolverRuleID, assoc.VPCID)

	return ignoreNotFound(err, route53resolverbackend.ErrNotFound)
}

// deleteR53RResolverQueryLogConfigAssociation mirrors
// deleteR53RResolverRuleAssociation: DisassociateResolverQueryLogConfig is
// keyed on (ResolverQueryLogConfigId, ResourceId), not the association ID.
func (rc *ResourceCreator) deleteR53RResolverQueryLogConfigAssociation(ctx context.Context, id string) error {
	b := rc.backends.Route53Resolver.Backend

	assoc, err := b.GetResolverQueryLogConfigAssociation(ctx, id)
	if err != nil {
		return ignoreNotFound(err, route53resolverbackend.ErrNotFound)
	}

	_, err = b.DisassociateResolverQueryLogConfig(ctx, assoc.ResolverQueryLogConfigID, assoc.ResourceID)

	return ignoreNotFound(err, route53resolverbackend.ErrNotFound)
}

// ---- AWS::Route53Resolver::ResolverRuleAssociation ----
// Ref returns the ResolverRuleAssociationId (docs).

func (rc *ResourceCreator) createR53RResolverRuleAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	assoc, err := rc.backends.Route53Resolver.Backend.AssociateResolverRule(
		ctx,
		strProp(props, "ResolverRuleId", params, physicalIDs),
		strProp(props, "VPCId", params, physicalIDs),
		name,
	)
	if err != nil {
		return "", fmt.Errorf("associate Route53Resolver rule %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Name"] = assoc.Name
	physicalIDs[logicalID+"/ResolverRuleId"] = assoc.ResolverRuleID
	physicalIDs[logicalID+"/VPCId"] = assoc.VPCID

	return assoc.ID, nil
}

// ---- AWS::Route53Resolver::FirewallDomainList ----
// Ref returns the FirewallDomainList object; the FirewallDomainList Id is
// used as the physical ID (GetFirewallDomainList/DeleteFirewallDomainList
// are Id-keyed, and Id is a documented GetAtt attribute) -- the docs'
// "returns the FirewallDomainList object" phrasing doesn't name its own
// identifier field, the same doc-imprecision class already logged for
// Athena::NamedQuery (resources_athena.go).

func (rc *ResourceCreator) createR53RFirewallDomainList(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	dl, err := rc.backends.Route53Resolver.Backend.CreateFirewallDomainList(ctx, name, logicalID)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver firewall domain list %s: %w", name, err)
	}

	if domains := strSliceProp(props["Domains"], params, physicalIDs); len(domains) > 0 {
		updated, updateErr := rc.backends.Route53Resolver.Backend.UpdateFirewallDomains(ctx, dl.ID, "ADD", domains)
		if updateErr != nil {
			return "", fmt.Errorf("set Route53Resolver firewall domain list %s domains: %w", name, updateErr)
		}

		dl = updated
	}

	r53rFirewallDomainListStash(logicalID, dl, physicalIDs)

	return dl.ID, nil
}

func r53rFirewallDomainListStash(
	logicalID string, dl *route53resolverbackend.FirewallDomainList, physicalIDs map[string]string,
) {
	physicalIDs[logicalID+"/Arn"] = dl.ARN
	physicalIDs[logicalID+"/CreationTime"] = dl.CreationTime
	physicalIDs[logicalID+"/CreatorRequestId"] = dl.CreatorRequestID
	physicalIDs[logicalID+"/DomainCount"] = strconv.Itoa(int(dl.DomainCount))
	physicalIDs[logicalID+"/ManagedOwnerName"] = dl.ManagedOwnerName
	physicalIDs[logicalID+"/ModificationTime"] = dl.ModificationTime
	physicalIDs[logicalID+"/Status"] = dl.Status
	physicalIDs[logicalID+"/StatusMessage"] = dl.StatusMessage
}

// ---- AWS::Route53Resolver::FirewallRuleGroup ----
// Ref returns the FirewallRuleGroupId (docs).

func (rc *ResourceCreator) createR53RFirewallRuleGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	g, err := rc.backends.Route53Resolver.Backend.CreateFirewallRuleGroup(ctx, name, logicalID)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver firewall rule group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = g.ARN
	physicalIDs[logicalID+"/CreationTime"] = g.CreationTime
	physicalIDs[logicalID+"/CreatorRequestId"] = g.CreatorRequestID
	physicalIDs[logicalID+"/ModificationTime"] = g.ModificationTime
	physicalIDs[logicalID+"/OwnerId"] = g.OwnerID
	physicalIDs[logicalID+"/RuleCount"] = strconv.Itoa(int(g.RuleCount))
	physicalIDs[logicalID+"/ShareStatus"] = g.ShareStatus
	physicalIDs[logicalID+"/Status"] = g.Status
	physicalIDs[logicalID+"/StatusMessage"] = g.StatusMessage

	return g.ID, nil
}

// ---- AWS::Route53Resolver::FirewallRuleGroupAssociation ----
// Ref returns the FirewallRuleGroupAssociation ID (docs).

func (rc *ResourceCreator) createR53RFirewallRuleGroupAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	mutationProtection := strProp(props, "MutationProtection", params, physicalIDs)
	priority := int32PtrProp(props, "Priority", params, physicalIDs)

	var priorityVal int32
	if priority != nil {
		priorityVal = *priority
	}

	assoc, err := rc.backends.Route53Resolver.Backend.AssociateFirewallRuleGroup(
		ctx,
		strProp(props, "FirewallRuleGroupId", params, physicalIDs),
		strProp(props, "VpcId", params, physicalIDs),
		name,
		logicalID,
		mutationProtection,
		priorityVal,
	)
	if err != nil {
		return "", fmt.Errorf("associate Route53Resolver firewall rule group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = assoc.ARN
	physicalIDs[logicalID+"/CreationTime"] = assoc.CreationTime
	physicalIDs[logicalID+"/CreatorRequestId"] = assoc.CreatorRequestID
	physicalIDs[logicalID+"/ManagedOwnerName"] = assoc.ManagedOwnerName
	physicalIDs[logicalID+"/ModificationTime"] = assoc.ModificationTime
	physicalIDs[logicalID+"/Status"] = assoc.Status
	physicalIDs[logicalID+"/StatusMessage"] = assoc.StatusMessage

	return assoc.ID, nil
}

// ---- AWS::Route53Resolver::ResolverQueryLoggingConfig ----
// Ref returns the ID of the query logging config (docs).

func (rc *ResourceCreator) createR53RResolverQueryLogConfig(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cfg, err := rc.backends.Route53Resolver.Backend.CreateResolverQueryLogConfig(
		ctx, name, logicalID, strProp(props, "DestinationArn", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver query log config %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = cfg.ARN
	physicalIDs[logicalID+"/AssociationCount"] = strconv.Itoa(int(cfg.AssociationCount))
	physicalIDs[logicalID+"/CreationTime"] = cfg.CreationTime
	physicalIDs[logicalID+"/CreatorRequestId"] = cfg.CreatorRequestID
	physicalIDs[logicalID+"/OwnerId"] = cfg.OwnerID
	physicalIDs[logicalID+"/ShareStatus"] = cfg.ShareStatus
	physicalIDs[logicalID+"/Status"] = cfg.Status

	return cfg.ID, nil
}

// ---- AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation ----
// Ref returns the ID of the configuration association (docs).

func (rc *ResourceCreator) createR53RResolverQueryLogConfigAssociation(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	assoc, err := rc.backends.Route53Resolver.Backend.AssociateResolverQueryLogConfig(
		ctx,
		strProp(props, "ResolverQueryLogConfigId", params, physicalIDs),
		strProp(props, "ResourceId", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("associate Route53Resolver query log config for %s: %w", logicalID, err)
	}

	physicalIDs[logicalID+"/CreationTime"] = assoc.CreationTime
	physicalIDs[logicalID+"/Error"] = assoc.Error
	physicalIDs[logicalID+"/ErrorMessage"] = assoc.ErrorMessage
	physicalIDs[logicalID+"/Status"] = assoc.Status

	return assoc.ID, nil
}

// ---- AWS::Route53Resolver::OutpostResolver ----
// Ref returns the Id of the Outpost Resolver (docs).

func (rc *ResourceCreator) createR53ROutpostResolver(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53Resolver == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	instanceCount := int32PtrProp(props, "InstanceCount", params, physicalIDs)

	var count int32
	if instanceCount != nil {
		count = *instanceCount
	}

	r, err := rc.backends.Route53Resolver.Backend.CreateOutpostResolver(
		ctx,
		name,
		logicalID,
		strProp(props, "OutpostArn", params, physicalIDs),
		strProp(props, "PreferredInstanceType", params, physicalIDs),
		count,
	)
	if err != nil {
		return "", fmt.Errorf("create Route53Resolver outpost resolver %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = r.ARN
	physicalIDs[logicalID+"/CreationTime"] = r.CreationTime
	physicalIDs[logicalID+"/CreatorRequestId"] = r.CreatorRequestID
	physicalIDs[logicalID+"/ModificationTime"] = r.ModificationTime
	physicalIDs[logicalID+"/Status"] = r.Status
	physicalIDs[logicalID+"/StatusMessage"] = r.StatusMessage

	return r.ID, nil
}
