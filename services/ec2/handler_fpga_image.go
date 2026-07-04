package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// ---- Registration ----

func registerFpgaImageOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateFpgaImage"] = h.handleCreateFpgaImage
	ops["CopyFpgaImage"] = h.handleCopyFpgaImage
	ops["DeleteFpgaImage"] = h.handleDeleteFpgaImage
	ops["DescribeFpgaImages"] = h.handleDescribeFpgaImages
	ops["DescribeFpgaImageAttribute"] = h.handleDescribeFpgaImageAttribute
	ops["ModifyFpgaImageAttribute"] = h.handleModifyFpgaImageAttribute
	ops["ResetFpgaImageAttribute"] = h.handleResetFpgaImageAttribute
}

// fpgaImageSupportedOperations lists the operation names registered by
// registerFpgaImageOps, for GetSupportedOperations().
func fpgaImageSupportedOperations() []string {
	return []string{
		"CreateFpgaImage",
		"CopyFpgaImage",
		"DeleteFpgaImage",
		"DescribeFpgaImages",
		"DescribeFpgaImageAttribute",
		"ModifyFpgaImageAttribute",
		"ResetFpgaImageAttribute",
	}
}

// ---- Response shapes ----

type fpgaImageLoadPermissionXML struct {
	Group  string `xml:"group,omitempty"`
	UserID string `xml:"userId,omitempty"`
}

type fpgaImageProductCodeXML struct {
	ProductCode     string `xml:"productCode,omitempty"`
	ProductCodeType string `xml:"type,omitempty"`
}

type fpgaImageStateXML struct {
	Code    string `xml:"code,omitempty"`
	Message string `xml:"message,omitempty"`
}

type fpgaImageItemXML struct {
	State             fpgaImageStateXML `xml:"state"`
	OwnerID           string            `xml:"ownerId,omitempty"`
	Name              string            `xml:"name,omitempty"`
	Description       string            `xml:"description,omitempty"`
	ShellVersion      string            `xml:"shellVersion,omitempty"`
	FpgaImageGlobalID string            `xml:"fpgaImageGlobalId,omitempty"`
	FpgaImageID       string            `xml:"fpgaImageId"`
	OwnerAlias        string            `xml:"ownerAlias,omitempty"`
	CreateTime        string            `xml:"createTime,omitempty"`
	UpdateTime        string            `xml:"updateTime,omitempty"`
	ProductCodeSet    struct {
		Items []fpgaImageProductCodeXML `xml:"item"`
	} `xml:"productCodes"`
	InstanceTypeSet struct {
		Items []string `xml:"item"`
	} `xml:"instanceTypes"`
	Public bool `xml:"public"`
}

func toFpgaImageItemXML(img *FpgaImage) fpgaImageItemXML {
	item := fpgaImageItemXML{
		FpgaImageID:       img.FpgaImageID,
		FpgaImageGlobalID: img.FpgaImageGlobalID,
		Name:              img.Name,
		Description:       img.Description,
		ShellVersion:      img.ShellVersion,
		State:             fpgaImageStateXML{Code: img.State, Message: img.StateMessage},
		OwnerID:           img.OwnerID,
		OwnerAlias:        img.OwnerAlias,
		Public:            img.Public,
	}

	if !img.CreateTime.IsZero() {
		item.CreateTime = img.CreateTime.Format(time.RFC3339)
	}

	if !img.UpdateTime.IsZero() {
		item.UpdateTime = img.UpdateTime.Format(time.RFC3339)
	}

	for _, code := range img.ProductCodes {
		item.ProductCodeSet.Items = append(item.ProductCodeSet.Items, fpgaImageProductCodeXML{
			ProductCode:     code,
			ProductCodeType: "marketplace",
		})
	}

	item.InstanceTypeSet.Items = append(item.InstanceTypeSet.Items, img.InstanceTypes...)

	return item
}

type createFpgaImageResponse struct {
	XMLName           xml.Name `xml:"CreateFpgaImageResponse"`
	RequestID         string   `xml:"requestId"`
	FpgaImageID       string   `xml:"fpgaImageId,omitempty"`
	FpgaImageGlobalID string   `xml:"fpgaImageGlobalId,omitempty"`
}

type copyFpgaImageResponse struct {
	XMLName     xml.Name `xml:"CopyFpgaImageResponse"`
	RequestID   string   `xml:"requestId"`
	FpgaImageID string   `xml:"fpgaImageId,omitempty"`
}

type describeFpgaImagesResponse struct {
	XMLName      xml.Name `xml:"DescribeFpgaImagesResponse"`
	RequestID    string   `xml:"requestId"`
	FpgaImageSet struct {
		Items []fpgaImageItemXML `xml:"item"`
	} `xml:"fpgaImageSet"`
}

type fpgaImageAttributeXML struct {
	FpgaImageID     string `xml:"fpgaImageId,omitempty"`
	Description     string `xml:"description,omitempty"`
	Name            string `xml:"name,omitempty"`
	LoadPermissions struct {
		Items []fpgaImageLoadPermissionXML `xml:"item"`
	} `xml:"loadPermissions"`
	ProductCodes struct {
		Items []fpgaImageProductCodeXML `xml:"item"`
	} `xml:"productCodes"`
}

func toFpgaImageAttributeXML(
	id, description, name string,
	perms []FpgaImageLoadPermission,
	codes []string,
) fpgaImageAttributeXML {
	attr := fpgaImageAttributeXML{FpgaImageID: id, Description: description, Name: name}

	for _, p := range perms {
		attr.LoadPermissions.Items = append(
			attr.LoadPermissions.Items,
			fpgaImageLoadPermissionXML(p),
		)
	}

	for _, code := range codes {
		attr.ProductCodes.Items = append(
			attr.ProductCodes.Items,
			fpgaImageProductCodeXML{ProductCode: code, ProductCodeType: "marketplace"},
		)
	}

	return attr
}

type describeFpgaImageAttributeResponse struct {
	XMLName            xml.Name              `xml:"DescribeFpgaImageAttributeResponse"`
	RequestID          string                `xml:"requestId"`
	FpgaImageAttribute fpgaImageAttributeXML `xml:"fpgaImageAttribute"`
}

type modifyFpgaImageAttributeResponse struct {
	XMLName            xml.Name              `xml:"ModifyFpgaImageAttributeResponse"`
	RequestID          string                `xml:"requestId"`
	FpgaImageAttribute fpgaImageAttributeXML `xml:"fpgaImageAttribute"`
}

// ---- Handlers ----

func (h *Handler) handleCreateFpgaImage(vals url.Values, reqID string) (any, error) {
	name := vals.Get("Name")
	description := vals.Get("Description")

	img, err := h.Backend.CreateFpgaImage(name, description)
	if err != nil {
		return nil, err
	}

	return &createFpgaImageResponse{
		RequestID:         reqID,
		FpgaImageID:       img.FpgaImageID,
		FpgaImageGlobalID: img.FpgaImageGlobalID,
	}, nil
}

func (h *Handler) handleCopyFpgaImage(vals url.Values, reqID string) (any, error) {
	sourceID := vals.Get("SourceFpgaImageId")
	sourceRegion := vals.Get("SourceRegion")
	name := vals.Get("Name")
	description := vals.Get("Description")

	img, err := h.Backend.CopyFpgaImage(sourceID, sourceRegion, name, description)
	if err != nil {
		return nil, err
	}

	return &copyFpgaImageResponse{
		RequestID:   reqID,
		FpgaImageID: img.FpgaImageID,
	}, nil
}

func (h *Handler) handleDeleteFpgaImage(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FpgaImageId")
	if err := h.Backend.DeleteFpgaImage(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteFpgaImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFpgaImages(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FpgaImageId")
	images := h.Backend.DescribeFpgaImages(ids)

	resp := &describeFpgaImagesResponse{RequestID: reqID}
	for _, img := range images {
		resp.FpgaImageSet.Items = append(resp.FpgaImageSet.Items, toFpgaImageItemXML(img))
	}

	return resp, nil
}

func (h *Handler) handleDescribeFpgaImageAttribute(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FpgaImageId")
	attribute := vals.Get("Attribute")

	attr, err := h.Backend.DescribeFpgaImageAttribute(id, attribute)
	if err != nil {
		return nil, err
	}

	return &describeFpgaImageAttributeResponse{
		RequestID: reqID,
		FpgaImageAttribute: toFpgaImageAttributeXML(
			attr.FpgaImageID, attr.Description, attr.Name, attr.LoadPermissions, attr.ProductCodes,
		),
	}, nil
}

// parseFpgaImageLoadPermissionList parses "<prefix>.<N>.Group" /
// "<prefix>.<N>.UserId" indexed entries, e.g. LoadPermission.Add.1.Group.
func parseFpgaImageLoadPermissionList(vals url.Values, prefix string) []FpgaImageLoadPermission {
	var out []FpgaImageLoadPermission

	for i := 1; ; i++ {
		idx := strconv.Itoa(i)
		group := vals.Get(prefix + "." + idx + ".Group")
		userID := vals.Get(prefix + "." + idx + ".UserId")

		if group == "" && userID == "" {
			return out
		}

		out = append(out, FpgaImageLoadPermission{Group: group, UserID: userID})
	}
}

func (h *Handler) handleModifyFpgaImageAttribute(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FpgaImageId")
	attribute := vals.Get("Attribute")

	mod := FpgaImageAttributeModification{
		Attribute:         attribute,
		OperationType:     vals.Get("OperationType"),
		LoadPermissionAdd: parseFpgaImageLoadPermissionList(vals, "LoadPermission.Add"),
		LoadPermissionDel: parseFpgaImageLoadPermissionList(vals, "LoadPermission.Remove"),
		ProductCodes:      parseMemberList(vals, "ProductCode"),
	}

	switch attribute {
	case fpgaAttrDescription:
		if v := vals.Get("Description"); v != "" {
			mod.Description = &v
		}
	case fpgaAttrName:
		if v := vals.Get("Name"); v != "" {
			mod.Name = &v
		}
	}

	// Legacy top-level Description/Name (without an explicit Attribute) is
	// also accepted by AWS.
	if attribute == "" {
		if v := vals.Get("Description"); v != "" {
			mod.Description = &v
		}
		if v := vals.Get("Name"); v != "" {
			mod.Name = &v
		}
	}

	img, err := h.Backend.ModifyFpgaImageAttribute(id, mod)
	if err != nil {
		return nil, err
	}

	return &modifyFpgaImageAttributeResponse{
		RequestID: reqID,
		FpgaImageAttribute: toFpgaImageAttributeXML(
			img.FpgaImageID, img.Description, img.Name, img.LoadPermissions, img.ProductCodes,
		),
	}, nil
}

func (h *Handler) handleResetFpgaImageAttribute(vals url.Values, reqID string) (any, error) {
	id := vals.Get("FpgaImageId")
	attribute := vals.Get("Attribute")

	if err := h.Backend.ResetFpgaImageAttribute(id, attribute); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetFpgaImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}
