package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ---- CreateSnapshotCopyGrant ----

type xmlSnapshotCopyGrant struct {
	SnapshotCopyGrantName string       `xml:"SnapshotCopyGrantName"`
	KMSKeyID              string       `xml:"KmsKeyId,omitempty"`
	Tags                  []svcTags.KV `xml:"Tags>Tag,omitempty"`
}

type createSnapshotCopyGrantResponse struct {
	XMLName xml.Name             `xml:"CreateSnapshotCopyGrantResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Grant   xmlSnapshotCopyGrant `xml:"CreateSnapshotCopyGrantResult>SnapshotCopyGrant"`
}

func (h *Handler) handleCreateSnapshotCopyGrant(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")
	kmsKeyID := vals.Get("KmsKeyId")
	tagMap := parseRedshiftTags(vals)

	grant, err := h.Backend.CreateSnapshotCopyGrant(name, kmsKeyID, tagMap)
	if err != nil {
		return nil, err
	}

	return &createSnapshotCopyGrantResponse{
		Xmlns: redshiftXMLNS,
		Grant: xmlSnapshotCopyGrant{
			SnapshotCopyGrantName: grant.SnapshotCopyGrantName,
			KMSKeyID:              grant.KMSKeyID,
			Tags:                  tagMapToKVList(grant.Tags),
		},
	}, nil
}

// ---- DeleteSnapshotCopyGrant ----

type deleteSnapshotCopyGrantResponse struct {
	XMLName   xml.Name `xml:"DeleteSnapshotCopyGrantResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteSnapshotCopyGrant(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")

	if err := h.Backend.DeleteSnapshotCopyGrant(name); err != nil {
		return nil, err
	}

	return &deleteSnapshotCopyGrantResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeSnapshotCopyGrants ----

type xmlSnapshotCopyGrantList struct {
	Grants []xmlSnapshotCopyGrant `xml:"SnapshotCopyGrant"`
}

type describeSnapshotCopyGrantsResponse struct {
	XMLName xml.Name                 `xml:"DescribeSnapshotCopyGrantsResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Grants  xmlSnapshotCopyGrantList `xml:"DescribeSnapshotCopyGrantsResult>SnapshotCopyGrants"`
}

func (h *Handler) handleDescribeSnapshotCopyGrants(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")
	tagKeys := parseRedshiftTagKeysAt(vals, "TagKeys.TagKey.")
	tagValues := parseRedshiftTagKeysAt(vals, "TagValues.TagValue.")

	grants, err := h.Backend.DescribeSnapshotCopyGrants(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlSnapshotCopyGrant, 0, len(grants))

	for _, g := range grants {
		if !anyTagMatchesFilter(g.Tags, tagKeys, tagValues) {
			continue
		}

		members = append(members, xmlSnapshotCopyGrant{
			SnapshotCopyGrantName: g.SnapshotCopyGrantName,
			KMSKeyID:              g.KMSKeyID,
			Tags:                  tagMapToKVList(g.Tags),
		})
	}

	return &describeSnapshotCopyGrantsResponse{
		Xmlns:  redshiftXMLNS,
		Grants: xmlSnapshotCopyGrantList{Grants: members},
	}, nil
}

// ---- EnableSnapshotCopy ----

type enableSnapshotCopyResponse struct {
	XMLName xml.Name   `xml:"EnableSnapshotCopyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"EnableSnapshotCopyResult>Cluster"`
}

func (h *Handler) handleEnableSnapshotCopy(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	destinationRegion := vals.Get("DestinationRegion")
	grantName := vals.Get("SnapshotCopyGrantName")
	retentionPeriod, _ := strconv.Atoi(vals.Get("RetentionPeriod"))

	cluster, err := h.Backend.EnableSnapshotCopy(clusterID, destinationRegion, grantName, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &enableSnapshotCopyResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: h.toXMLCluster(cluster),
	}, nil
}

// ---- DisableSnapshotCopy ----

type disableSnapshotCopyResponse struct {
	XMLName xml.Name   `xml:"DisableSnapshotCopyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"DisableSnapshotCopyResult>Cluster"`
}

func (h *Handler) handleDisableSnapshotCopy(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.DisableSnapshotCopy(clusterID)
	if err != nil {
		return nil, err
	}

	return &disableSnapshotCopyResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: h.toXMLCluster(cluster),
	}, nil
}

// ---- ModifySnapshotCopyRetentionPeriod ----

type modifySnapshotCopyRetentionPeriodResponse struct {
	XMLName xml.Name   `xml:"ModifySnapshotCopyRetentionPeriodResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ModifySnapshotCopyRetentionPeriodResult>Cluster"`
}

func (h *Handler) handleModifySnapshotCopyRetentionPeriod(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	retentionPeriod, _ := strconv.Atoi(vals.Get("RetentionPeriod"))

	cluster, err := h.Backend.ModifySnapshotCopyRetentionPeriod(clusterID, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &modifySnapshotCopyRetentionPeriodResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: h.toXMLCluster(cluster),
	}, nil
}
