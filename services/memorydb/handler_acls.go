package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req createACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	acl, err := h.Backend.CreateACL(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createACLResponse{ACL: toACLObject(acl, []string{})})
}

func (h *Handler) handleDescribeACLs(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	acls, err := h.Backend.DescribeACLs(ctx, req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Fetch all clusters once to compute the Clusters field on each ACL.
	allClusters, _ := h.Backend.DescribeClusters(ctx, "")

	acls, nextToken := paginateItems(acls, req.NextToken, req.MaxResults, func(a *ACL) string { return a.Name })

	objs := make([]aclObject, 0, len(acls))

	for _, a := range acls {
		clusterNames := clustersForACL(allClusters, a.Name)
		objs = append(objs, toACLObject(a, clusterNames))
	}

	return c.JSON(http.StatusOK, describeACLResponse{ACLs: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.DeleteACL(ctx, req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteACLResponse{ACL: toACLObject(acl, []string{})})
}

func (h *Handler) handleUpdateACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.UpdateACL(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	allClusters, _ := h.Backend.DescribeClusters(ctx, "")
	clusterNames := clustersForACL(allClusters, acl.Name)

	return c.JSON(http.StatusOK, updateACLResponse{ACL: toACLObject(acl, clusterNames)})
}

// -- SubnetGroup handlers --------------------------------------------------------

// clustersForACL returns the names of clusters that reference the given ACL name.
func clustersForACL(clusters []*Cluster, aclName string) []string {
	names := make([]string, 0, len(clusters))

	for _, c := range clusters {
		if c.ACLName == aclName {
			names = append(names, c.Name)
		}
	}

	return names
}

// toACLObject converts an ACL to its JSON representation.
// clusterNames is the list of cluster names that reference this ACL.
func toACLObject(a *ACL, clusterNames []string) aclObject {
	return aclObject{
		Name:                 a.Name,
		ARN:                  a.ARN,
		Status:               a.Status,
		UserNames:            a.UserNames,
		Clusters:             clusterNames,
		MinimumEngineVersion: engineVersion62,
		PendingChanges:       &aclPendingChangesObject{UserNamesToAdd: []string{}, UserNamesToRemove: []string{}},
	}
}
