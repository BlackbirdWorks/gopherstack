package cloudformation

import (
	"fmt"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

const (
	resTypeAppSyncDomainName       = "AWS::AppSync::DomainName"
	resTypeAppSyncGraphQLSchema    = "AWS::AppSync::GraphQLSchema"
	resTypeAppSyncChannelNamespace = "AWS::AppSync::ChannelNamespace"
)

// createAppSyncMoreResource handles the AppSync resource types listed above,
// added after createAppSyncSupplementalResource's own switch reached its
// cyclop budget. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createAppSyncMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAppSyncDomainName:
		id, err := rc.createAppSyncDomainName(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAppSyncGraphQLSchema:
		id, err := rc.createAppSyncGraphQLSchema(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAppSyncChannelNamespace:
		id, err := rc.createAppSyncChannelNamespace(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAppSyncMoreResource handles deletion for the types described in
// createAppSyncMoreResource.
func (rc *ResourceCreator) deleteAppSyncMoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeAppSyncDomainName:
		return true, rc.deleteAppSyncDomainName(physicalID)
	case resTypeAppSyncGraphQLSchema:
		// No independent lifecycle to tear down: a GraphQL schema lives and
		// dies with its owning Api (docs' Ref is "<ApiId>GraphQLSchema", not
		// its own resource) -- same class as AWS::ECS::PrimaryTaskSet.
		return true, nil
	case resTypeAppSyncChannelNamespace:
		return true, rc.deleteAppSyncChannelNamespace(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::AppSync::DomainName ----
// Ref returns the domain name (docs); AppSyncDomainName/DomainNameArn/
// HostedZoneId are backend-computed and stashed for Fn::GetAtt the same way
// MemoryDB::Cluster's side-channel attrs are.

func (rc *ResourceCreator) createAppSyncDomainName(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	if domainName == "" {
		domainName = logicalID
	}

	dn, err := rc.backends.AppSync.Backend.CreateDomainName(
		domainName,
		strProp(props, "CertificateArn", params, physicalIDs),
		strProp(props, "Description", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AppSync domain name %s: %w", domainName, err)
	}

	physicalIDs[logicalID+"/AppSyncDomainName"] = dn.AppsyncDomain
	physicalIDs[logicalID+"/DomainNameArn"] = dn.DomainNameARN
	physicalIDs[logicalID+"/HostedZoneId"] = dn.HostedZoneID

	return domainName, nil
}

func (rc *ResourceCreator) deleteAppSyncDomainName(physicalID string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	return ignoreNotFound(rc.backends.AppSync.Backend.DeleteDomainName(physicalID), appsyncbackend.ErrNotFound)
}

// ---- AWS::AppSync::GraphQLSchema ----
// Ref returns the GraphQL API ID with the literal string "GraphQLSchema"
// appended (docs); this backend only accepts inline SDL (the Definition
// property) -- DefinitionS3Location isn't fetched, matching this codebase's
// no-network-fetch convention for CFN-referenced S3 content elsewhere.

func (rc *ResourceCreator) createAppSyncGraphQLSchema(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	sdl := strProp(props, "Definition", params, physicalIDs)

	if _, err := rc.backends.AppSync.Backend.StartSchemaCreation(apiID, sdl); err != nil {
		return "", fmt.Errorf("create AppSync GraphQL schema for %s: %w", apiID, err)
	}

	return apiID + "GraphQLSchema", nil
}

// ---- AWS::AppSync::ChannelNamespace ----
// Ref returns the ARN of the channel namespace (docs).

func (rc *ResourceCreator) createAppSyncChannelNamespace(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	cfg := &appsyncbackend.ChannelNamespaceConfig{
		CodeHandlers: strProp(props, "CodeHandlers", params, physicalIDs),
	}

	ns, err := rc.backends.AppSync.Backend.CreateChannelNamespace(
		apiID, name, tagListProp(props, params, physicalIDs), cfg,
	)
	if err != nil {
		return "", fmt.Errorf("create AppSync channel namespace %s: %w", name, err)
	}

	return ns.ChannelNamespaceARN, nil
}

func (rc *ResourceCreator) deleteAppSyncChannelNamespace(physicalID string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	apiID, name := parseAppSyncARNParts(physicalID, "channelNamespaces")
	if apiID == "" || name == "" {
		return nil
	}

	return ignoreNotFound(rc.backends.AppSync.Backend.DeleteChannelNamespace(apiID, name), appsyncbackend.ErrNotFound)
}
