package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleAddRoleToDBInstance(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.AddRoleToDBInstance(instanceID, roleARN); err != nil {
		return nil, err
	}

	return &addRoleToDBInstanceResponse{Xmlns: rdsXMLNS}, nil
}

type addRoleToDBInstanceResponse struct {
	XMLName xml.Name `xml:"AddRoleToDBInstanceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleRemoveRoleFromDBInstance(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.RemoveRoleFromDBInstance(instanceID, roleARN); err != nil {
		return nil, err
	}

	return &removeRoleFromDBInstanceResponse{Xmlns: rdsXMLNS}, nil
}

type removeRoleFromDBInstanceResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromDBInstanceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}
