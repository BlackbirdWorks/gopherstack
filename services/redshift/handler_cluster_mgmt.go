package redshift

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- ModifyCluster ----

type modifyClusterResponse struct {
	XMLName xml.Name   `xml:"ModifyClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ModifyClusterResult>Cluster"`
}

func (h *Handler) handleModifyCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	nodeType := vals.Get("NodeType")
	masterUserPassword := vals.Get("MasterUserPassword")
	numberOfNodesStr := vals.Get("NumberOfNodes")
	applyImmediatelyStr := vals.Get("ApplyImmediately")

	// Encrypted/EnhancedVpcRouting are tri-state on the wire (real
	// ModifyClusterInput fields are *bool): only build a pointer when the
	// form actually included the key, so "not specified" is distinguishable
	// from "explicitly false" (e.g. decrypting a cluster).
	var encrypted *bool
	if v := vals.Get("Encrypted"); v != "" {
		b := v == paramValueTrue
		encrypted = &b
	}

	var enhancedVpcRouting *bool
	if v := vals.Get("EnhancedVpcRouting"); v != "" {
		b := v == paramValueTrue
		enhancedVpcRouting = &b
	}

	applyImmediately := applyImmediatelyStr != "false"

	numberOfNodes := 0

	if numberOfNodesStr != "" {
		n, err := strconv.Atoi(numberOfNodesStr)
		if err != nil {
			return nil, fmt.Errorf("%w: NumberOfNodes must be an integer", ErrInvalidParameter)
		}

		numberOfNodes = n
	}

	cluster, err := h.Backend.ModifyCluster(
		id,
		nodeType,
		numberOfNodes,
		masterUserPassword,
		encrypted,
		enhancedVpcRouting,
		applyImmediately,
	)
	if err != nil {
		return nil, err
	}

	return &modifyClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- RebootCluster ----

type rebootClusterResponse struct {
	XMLName xml.Name   `xml:"RebootClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"RebootClusterResult>Cluster"`
}

func (h *Handler) handleRebootCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.RebootCluster(id)
	if err != nil {
		return nil, err
	}

	return &rebootClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- PauseCluster ----

type pauseClusterResponse struct {
	XMLName xml.Name   `xml:"PauseClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"PauseClusterResult>Cluster"`
}

func (h *Handler) handlePauseCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.PauseCluster(id)
	if err != nil {
		return nil, err
	}

	return &pauseClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- ResumeCluster ----

type resumeClusterResponse struct {
	XMLName xml.Name   `xml:"ResumeClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ResumeClusterResult>Cluster"`
}

func (h *Handler) handleResumeCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.ResumeCluster(id)
	if err != nil {
		return nil, err
	}

	return &resumeClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- ResizeCluster ----

type resizeClusterResponse struct {
	XMLName xml.Name   `xml:"ResizeClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ResizeClusterResult>Cluster"`
}

func (h *Handler) handleResizeCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	nodeType := vals.Get("NodeType")
	clusterType := vals.Get("ClusterType")
	classic := vals.Get("Classic") == paramValueTrue
	numberOfNodesStr := vals.Get("NumberOfNodes")

	numberOfNodes := 0

	if numberOfNodesStr != "" {
		n, err := strconv.Atoi(numberOfNodesStr)
		if err != nil {
			return nil, fmt.Errorf("%w: NumberOfNodes must be an integer", ErrInvalidParameter)
		}

		numberOfNodes = n
	}

	cluster, err := h.Backend.ResizeCluster(id, nodeType, clusterType, numberOfNodes, classic)
	if err != nil {
		return nil, err
	}

	return &resizeClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- RotateEncryptionKey ----

type rotateEncryptionKeyResponse struct {
	XMLName xml.Name   `xml:"RotateEncryptionKeyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"RotateEncryptionKeyResult>Cluster"`
}

func (h *Handler) handleRotateEncryptionKey(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.RotateEncryptionKey(id)
	if err != nil {
		return nil, err
	}

	return &rotateEncryptionKeyResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- ModifyClusterIamRoles ----

type modifyClusterIamRolesResponse struct {
	XMLName xml.Name   `xml:"ModifyClusterIamRolesResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ModifyClusterIamRolesResult>Cluster"`
}

func (h *Handler) handleModifyClusterIamRoles(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	addRoles := parseStringList(vals, "AddIamRoles.IamRoleArn.")
	removeRoles := parseStringList(vals, "RemoveIamRoles.IamRoleArn.")

	cluster, err := h.Backend.ModifyClusterIamRoles(id, addRoles, removeRoles)
	if err != nil {
		return nil, err
	}

	return &modifyClusterIamRolesResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- ModifyClusterMaintenance ----

type modifyClusterMaintenanceResponse struct {
	XMLName xml.Name   `xml:"ModifyClusterMaintenanceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ModifyClusterMaintenanceResult>Cluster"`
}

func (h *Handler) handleModifyClusterMaintenance(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	maintenanceTrack := vals.Get("MaintenanceTrackName")
	deferMaintenance := vals.Get("DeferMaintenance") == paramValueTrue

	cluster, err := h.Backend.ModifyClusterMaintenance(id, maintenanceTrack, deferMaintenance)
	if err != nil {
		return nil, err
	}

	return &modifyClusterMaintenanceResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}
