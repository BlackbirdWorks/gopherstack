package cloudformation

import (
	"errors"
	"fmt"
	"strings"

	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

const defaultGlueDB = "default"

var (
	errGluePartition         = errors.New("glue partition create failed")
	errGluePartitionMissProp = errors.New("create Glue partition: DatabaseName and TableName are required")
)

func (rc *ResourceCreator) createGlueSupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Glue::Crawler":
		id, err := rc.createGlueCrawler(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Glue::Table":
		id, err := rc.createGlueTable(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Glue::Trigger":
		id, err := rc.createGlueTrigger(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Glue::Connection":
		id, err := rc.createGlueConnection(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Glue::Partition":
		id, err := rc.createGluePartition(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createGlueCrawler(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	role := strProp(props, "Role", params, physicalIDs)
	if role == "" {
		role = "AWSGlueServiceRole"
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)

	crawler, err := rc.backends.Glue.Backend.CreateCrawler(name, role, dbName, gluebackend.CrawlerTarget{}, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue crawler %s: %w", name, err)
	}

	return crawler.Name, nil
}

func (rc *ResourceCreator) deleteGlueCrawler(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteCrawler(name)
}

func (rc *ResourceCreator) createGlueTable(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)
	if dbName == "" {
		dbName = defaultGlueDB
	}

	var tableName string
	if ti, ok := props["TableInput"].(map[string]any); ok {
		tableName = strProp(ti, "Name", params, physicalIDs)
	}
	if tableName == "" {
		tableName = strings.ToLower(logicalID)
	}

	tableInput := gluebackend.TableInput{Name: tableName}
	_, err := rc.backends.Glue.Backend.CreateTable(dbName, tableInput)
	if err != nil {
		return "", fmt.Errorf("create Glue table %s in database %s: %w", tableName, dbName, err)
	}

	return dbName + "/" + tableName, nil
}

func (rc *ResourceCreator) deleteGlueTable(physicalID string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	const parts = 2
	split := strings.SplitN(physicalID, "/", parts)
	if len(split) < parts {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteTable(split[0], split[1])
}

func (rc *ResourceCreator) createGlueTrigger(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	triggerType := strProp(props, "Type", params, physicalIDs)
	if triggerType == "" {
		triggerType = "ON_DEMAND"
	}

	trigger, err := rc.backends.Glue.Backend.CreateTrigger(gluebackend.Trigger{
		Name: name,
		Type: triggerType,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue trigger %s: %w", name, err)
	}

	return trigger.Name, nil
}

func (rc *ResourceCreator) deleteGlueTrigger(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteTrigger(name)
}

func (rc *ResourceCreator) createGlueConnection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	var connName, connType string
	if ci, ok := props["ConnectionInput"].(map[string]any); ok {
		connName = strProp(ci, "Name", params, physicalIDs)
		connType = strProp(ci, "ConnectionType", params, physicalIDs)
	}

	if connName == "" {
		connName = logicalID
	}
	if connType == "" {
		connType = "JDBC"
	}

	conn, err := rc.backends.Glue.Backend.CreateConnection(connName, connType, nil, nil)
	if err != nil {
		return "", fmt.Errorf("create Glue connection %s: %w", connName, err)
	}

	return conn.Name, nil
}

func (rc *ResourceCreator) deleteGlueConnection(name string) error {
	if rc.backends.Glue == nil {
		return nil
	}

	return rc.backends.Glue.Backend.DeleteConnection(name)
}

func (rc *ResourceCreator) createGluePartition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Glue == nil {
		return logicalID + "-stub", nil
	}

	dbName := strProp(props, "DatabaseName", params, physicalIDs)
	tableName := strProp(props, "TableName", params, physicalIDs)

	if dbName == "" || tableName == "" {
		return "", errGluePartitionMissProp
	}

	var values []string
	if pi, hasPartition := props["PartitionInput"].(map[string]any); hasPartition {
		if vs, hasValues := pi["Values"].([]any); hasValues {
			for _, v := range vs {
				if s, isStr := v.(string); isStr {
					values = append(values, s)
				}
			}
		}
	}

	if len(values) == 0 {
		values = []string{defaultGlueDB}
	}

	_, errs := rc.backends.Glue.Backend.BatchCreatePartition(dbName, tableName, []gluebackend.PartitionInput{
		{Values: values},
	})
	if len(errs) > 0 {
		return "", fmt.Errorf("%w in %s/%s: %s", errGluePartition, dbName, tableName, errs[0].ErrorDetail.ErrorMessage)
	}

	return dbName + "/" + tableName + "/" + strings.Join(values, ","), nil
}

// deleteGlueSupplementalResource handles deletion for Glue supplemental resource types.
func (rc *ResourceCreator) deleteGlueSupplementalResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Glue::Crawler":
		return true, rc.deleteGlueCrawler(physicalID)
	case "AWS::Glue::Table":
		return true, rc.deleteGlueTable(physicalID)
	case "AWS::Glue::Trigger":
		return true, rc.deleteGlueTrigger(physicalID)
	case "AWS::Glue::Connection":
		return true, rc.deleteGlueConnection(physicalID)
	case "AWS::Glue::Partition":
		return true, nil
	}

	return false, nil
}
