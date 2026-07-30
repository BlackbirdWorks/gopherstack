package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// handler_trunk_enclave.go implements the HTTP handlers for the Trunk Interface association
// family and the Enclave Certificate IAM Role association family, backed by
// trunk_enclave.go.

// ---- Registration ----

func registerTrunkEnclaveOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["AssociateTrunkInterface"] = h.handleAssociateTrunkInterface
	ops["DisassociateTrunkInterface"] = h.handleDisassociateTrunkInterface
	ops["DescribeTrunkInterfaceAssociations"] = h.handleDescribeTrunkInterfaceAssociations

	ops["AssociateEnclaveCertificateIamRole"] = h.handleAssociateEnclaveCertificateIamRole
	ops["DisassociateEnclaveCertificateIamRole"] = h.handleDisassociateEnclaveCertificateIamRole
	ops["GetAssociatedEnclaveCertificateIamRoles"] = h.handleGetAssociatedEnclaveCertificateIamRoles
}

// trunkEnclaveSupportedOperations lists the operation names registered by
// registerTrunkEnclaveOps, for GetSupportedOperations().
func trunkEnclaveSupportedOperations() []string {
	return []string{
		"AssociateTrunkInterface",
		"DisassociateTrunkInterface",
		"DescribeTrunkInterfaceAssociations",
		"AssociateEnclaveCertificateIamRole",
		"DisassociateEnclaveCertificateIamRole",
		"GetAssociatedEnclaveCertificateIamRoles",
	}
}

// ---- Response shapes: Trunk Interface associations ----

type interfaceAssociationItem struct {
	AssociationID     string             `xml:"associationId,omitempty"`
	BranchInterfaceID string             `xml:"branchInterfaceId,omitempty"`
	TrunkInterfaceID  string             `xml:"trunkInterfaceId,omitempty"`
	InterfaceProtocol string             `xml:"interfaceProtocol,omitempty"`
	TagSet            instanceTagItemSet `xml:"tagSet"`
	VlanID            int32              `xml:"vlanId,omitempty"`
	GreKey            int32              `xml:"greKey,omitempty"`
}

func toInterfaceAssociationItem(a *TrunkInterfaceAssociation) interfaceAssociationItem {
	item := interfaceAssociationItem{
		AssociationID:     a.AssociationID,
		BranchInterfaceID: a.BranchInterfaceID,
		TrunkInterfaceID:  a.TrunkInterfaceID,
		InterfaceProtocol: a.InterfaceProtocol,
		VlanID:            a.VlanID,
		GreKey:            a.GreKey,
	}
	for k, v := range a.Tags {
		item.TagSet.Items = append(item.TagSet.Items, instanceTagItem{Key: k, Value: v})
	}

	return item
}

type associateTrunkInterfaceResponse struct {
	XMLName              xml.Name                 `xml:"AssociateTrunkInterfaceResponse"`
	Xmlns                string                   `xml:"xmlns,attr"`
	RequestID            string                   `xml:"requestId"`
	ClientToken          string                   `xml:"clientToken,omitempty"`
	InterfaceAssociation interfaceAssociationItem `xml:"interfaceAssociation"`
}

type disassociateTrunkInterfaceResponse struct {
	XMLName     xml.Name `xml:"DisassociateTrunkInterfaceResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	ClientToken string   `xml:"clientToken,omitempty"`
	Return      bool     `xml:"return"`
}

type describeTrunkInterfaceAssociationsResponse struct {
	XMLName                 xml.Name `xml:"DescribeTrunkInterfaceAssociationsResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	InterfaceAssociationSet struct {
		Items []interfaceAssociationItem `xml:"item"`
	} `xml:"interfaceAssociationSet"`
}

// ---- Response shapes: Enclave Certificate IAM Role associations ----

type associateEnclaveCertificateIamRoleResponse struct {
	XMLName                 xml.Name `xml:"AssociateEnclaveCertificateIamRoleResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	CertificateS3BucketName string   `xml:"certificateS3BucketName,omitempty"`
	CertificateS3ObjectKey  string   `xml:"certificateS3ObjectKey,omitempty"`
	EncryptionKmsKeyID      string   `xml:"encryptionKmsKeyId,omitempty"`
}

type disassociateEnclaveCertificateIamRoleResponse struct {
	XMLName   xml.Name `xml:"DisassociateEnclaveCertificateIamRoleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type associatedRoleItem struct {
	AssociatedRoleArn       string `xml:"associatedRoleArn,omitempty"`
	CertificateS3BucketName string `xml:"certificateS3BucketName,omitempty"`
	CertificateS3ObjectKey  string `xml:"certificateS3ObjectKey,omitempty"`
	EncryptionKmsKeyID      string `xml:"encryptionKmsKeyId,omitempty"`
}

type getAssociatedEnclaveCertificateIamRolesResponse struct {
	XMLName           xml.Name `xml:"GetAssociatedEnclaveCertificateIamRolesResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	AssociatedRoleSet struct {
		Items []associatedRoleItem `xml:"item"`
	} `xml:"associatedRoleSet"`
}

// ---- Handlers: Trunk Interface associations ----

func (h *Handler) handleAssociateTrunkInterface(vals url.Values, reqID string) (any, error) {
	vlanID, _ := strconv.ParseInt(vals.Get("VlanId"), 10, 32)
	greKey, _ := strconv.ParseInt(vals.Get("GreKey"), 10, 32)

	assoc, err := h.Backend.AssociateTrunkInterface(
		vals.Get("BranchInterfaceId"), vals.Get("TrunkInterfaceId"), int32(vlanID), int32(greKey),
		parseTagSpecification(vals, resourceTypeENI),
	)
	if err != nil {
		return nil, err
	}

	return &associateTrunkInterfaceResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		InterfaceAssociation: toInterfaceAssociationItem(assoc),
		ClientToken:          vals.Get("ClientToken"),
	}, nil
}

func (h *Handler) handleDisassociateTrunkInterface(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DisassociateTrunkInterface(vals.Get("AssociationId")); err != nil {
		return nil, err
	}

	return &disassociateTrunkInterfaceResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, Return: true, ClientToken: vals.Get("ClientToken"),
	}, nil
}

func (h *Handler) handleDescribeTrunkInterfaceAssociations(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "AssociationId")
	assocs := h.Backend.DescribeTrunkInterfaceAssociations(ids)

	resp := &describeTrunkInterfaceAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.InterfaceAssociationSet.Items = append(resp.InterfaceAssociationSet.Items, toInterfaceAssociationItem(a))
	}

	return resp, nil
}

// ---- Handlers: Enclave Certificate IAM Role associations ----

func (h *Handler) handleAssociateEnclaveCertificateIamRole(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.AssociateEnclaveCertificateIamRole(vals.Get("CertificateArn"), vals.Get("RoleArn"))
	if err != nil {
		return nil, err
	}

	return &associateEnclaveCertificateIamRoleResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		CertificateS3BucketName: assoc.CertificateS3BucketName,
		CertificateS3ObjectKey:  assoc.CertificateS3ObjectKey,
		EncryptionKmsKeyID:      assoc.EncryptionKmsKeyID,
	}, nil
}

func (h *Handler) handleDisassociateEnclaveCertificateIamRole(vals url.Values, reqID string) (any, error) {
	err := h.Backend.DisassociateEnclaveCertificateIamRole(vals.Get("CertificateArn"), vals.Get("RoleArn"))
	if err != nil {
		return nil, err
	}

	return &disassociateEnclaveCertificateIamRoleResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleGetAssociatedEnclaveCertificateIamRoles(vals url.Values, reqID string) (any, error) {
	assocs, err := h.Backend.GetAssociatedEnclaveCertificateIamRoles(vals.Get("CertificateArn"))
	if err != nil {
		return nil, err
	}

	resp := &getAssociatedEnclaveCertificateIamRolesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.AssociatedRoleSet.Items = append(resp.AssociatedRoleSet.Items, associatedRoleItem{
			AssociatedRoleArn:       a.RoleArn,
			CertificateS3BucketName: a.CertificateS3BucketName,
			CertificateS3ObjectKey:  a.CertificateS3ObjectKey,
			EncryptionKmsKeyID:      a.EncryptionKmsKeyID,
		})
	}

	return resp, nil
}
