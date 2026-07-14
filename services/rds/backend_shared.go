package rds

import (
	"maps"
	"slices"
)

// copyParameterGroupTo returns a new DBParameterGroup that is a copy of src with the given
// target name and description. The caller is responsible for storing it in the appropriate map.
func copyParameterGroupTo(src *DBParameterGroup, targetName, targetDescription string) *DBParameterGroup {
	if targetDescription == "" {
		targetDescription = src.Description
	}

	pg := &DBParameterGroup{
		DBParameterGroupName:   targetName,
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            targetDescription,
		Parameters:             make(map[string]DBParameter, len(src.Parameters)),
	}
	maps.Copy(pg.Parameters, src.Parameters)

	return pg
}

// applySnapshotAttributeChange modifies a list of snapshot attributes by adding and removing values.
func applySnapshotAttributeChange(
	attrs *[]DBSnapshotAttribute,
	attributeName string,
	valuesToAdd, valuesToRemove []string,
) {
	var attr *DBSnapshotAttribute
	for i := range *attrs {
		if (*attrs)[i].AttributeName == attributeName {
			attr = &(*attrs)[i]

			break
		}
	}
	if attr == nil {
		*attrs = append(*attrs, DBSnapshotAttribute{AttributeName: attributeName})
		attr = &(*attrs)[len(*attrs)-1]
	}
	for _, v := range valuesToAdd {
		if !slices.Contains(attr.AttributeValues, v) {
			attr.AttributeValues = append(attr.AttributeValues, v)
		}
	}
	attr.AttributeValues = slices.DeleteFunc(attr.AttributeValues, func(v string) bool {
		return slices.Contains(valuesToRemove, v)
	})
}
