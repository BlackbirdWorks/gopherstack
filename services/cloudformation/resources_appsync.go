package cloudformation

import (
	"fmt"
	"strings"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

func (rc *ResourceCreator) createAppSyncSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::AppSync::DataSource":
		id, err := rc.createAppSyncDataSource(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::AppSync::Resolver":
		id, err := rc.createAppSyncResolver(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::AppSync::FunctionConfiguration":
		id, err := rc.createAppSyncFunction(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::AppSync::ApiKey":
		id, err := rc.createAppSyncAPIKey(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createAppSyncDataSource(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	dsType := strProp(props, "Type", params, physicalIDs)

	if name == "" {
		name = logicalID
	}
	if dsType == "" {
		dsType = "NONE"
	}

	ds, err := rc.backends.AppSync.Backend.CreateDataSource(apiID, &appsyncbackend.DataSource{
		Name: name,
		Type: appsyncbackend.DataSourceType(dsType),
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync data source %s: %w", name, err)
	}

	return ds.DataSourceARN, nil
}

func (rc *ResourceCreator) deleteAppSyncDataSource(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/datasources/<name>
	apiID, name := parseAppSyncARNParts(arn, "datasources")
	if apiID == "" || name == "" {
		return nil
	}

	return rc.backends.AppSync.Backend.DeleteDataSource(apiID, name)
}

func (rc *ResourceCreator) createAppSyncResolver(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	typeName := strProp(props, "TypeName", params, physicalIDs)
	fieldName := strProp(props, "FieldName", params, physicalIDs)
	dataSourceName := strProp(props, "DataSourceName", params, physicalIDs)
	kind := strProp(props, "Kind", params, physicalIDs)

	if typeName == "" {
		typeName = "Query"
	}
	if fieldName == "" {
		fieldName = strings.ToLower(logicalID)
	}
	if kind == "" {
		kind = "UNIT"
	}
	if kind == "UNIT" && dataSourceName == "" {
		dataSourceName = "none"
	}

	r, err := rc.backends.AppSync.Backend.CreateResolver(apiID, typeName, &appsyncbackend.Resolver{
		FieldName:      fieldName,
		Kind:           kind,
		DataSourceName: dataSourceName,
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync resolver %s.%s: %w", typeName, fieldName, err)
	}

	return r.ResolverARN, nil
}

func (rc *ResourceCreator) deleteAppSyncResolver(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/types/<typeName>/resolvers/<fieldName>
	_, afterAPIs, hasAPIs := strings.Cut(arn, "apis/")
	if !hasAPIs {
		return nil
	}

	apiID, rest1, hasTypes := strings.Cut(afterAPIs, "/types/")
	if !hasTypes {
		return nil
	}

	typeName, fieldName, hasResolvers := strings.Cut(rest1, "/resolvers/")
	if !hasResolvers {
		return nil
	}

	return rc.backends.AppSync.Backend.DeleteResolver(apiID, typeName, fieldName)
}

func (rc *ResourceCreator) createAppSyncFunction(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	dataSourceName := strProp(props, "DataSourceName", params, physicalIDs)

	if name == "" {
		name = logicalID
	}
	if dataSourceName == "" {
		dataSourceName = "none"
	}

	f, err := rc.backends.AppSync.Backend.CreateFunction(apiID, &appsyncbackend.Function{
		Name:           name,
		DataSourceName: dataSourceName,
	})
	if err != nil {
		return "", fmt.Errorf("create AppSync function %s: %w", name, err)
	}

	return f.FunctionARN, nil
}

func (rc *ResourceCreator) deleteAppSyncFunction(arn string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	apiID, funcID := parseAppSyncARNParts(arn, "functions")
	if apiID == "" || funcID == "" {
		return nil
	}

	return rc.backends.AppSync.Backend.DeleteFunction(apiID, funcID)
}

func (rc *ResourceCreator) createAppSyncAPIKey(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppSync == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	key, err := rc.backends.AppSync.Backend.CreateAPIKey(apiID, description, 0)
	if err != nil {
		return "", fmt.Errorf("create AppSync API key for %s: %w", apiID, err)
	}

	return apiID + "/" + key.ID, nil
}

func (rc *ResourceCreator) deleteAppSyncAPIKey(physicalID string) error {
	if rc.backends.AppSync == nil {
		return nil
	}

	const parts = 2
	split := strings.SplitN(physicalID, "/", parts)
	if len(split) < parts {
		return nil
	}

	return rc.backends.AppSync.Backend.DeleteAPIKey(split[0], split[1])
}

// deleteAppSyncSupplementalResource handles deletion for AppSync supplemental resource types.
func (rc *ResourceCreator) deleteAppSyncSupplementalResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::AppSync::DataSource":
		return true, rc.deleteAppSyncDataSource(physicalID)
	case "AWS::AppSync::Resolver":
		return true, rc.deleteAppSyncResolver(physicalID)
	case "AWS::AppSync::FunctionConfiguration":
		return true, rc.deleteAppSyncFunction(physicalID)
	case "AWS::AppSync::ApiKey":
		return true, rc.deleteAppSyncAPIKey(physicalID)
	}

	return false, nil
}

// parseAppSyncARNParts extracts apiID and resource name from an AppSync ARN.
// ARN format: arn:aws:appsync:<region>:<account>:apis/<apiID>/<resourceType>/<name>.
func parseAppSyncARNParts(arn, resourceType string) (string, string) {
	_, afterAPIs, hasAPIs := strings.Cut(arn, "apis/")
	if !hasAPIs {
		return "", ""
	}

	apiID, name, found := strings.Cut(afterAPIs, "/"+resourceType+"/")
	if !found {
		return "", ""
	}

	return apiID, name
}
