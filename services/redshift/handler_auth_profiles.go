package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CreateAuthenticationProfile ----

type xmlAuthenticationProfile struct {
	AuthenticationProfileName    string `xml:"AuthenticationProfileName"`
	AuthenticationProfileContent string `xml:"AuthenticationProfileContent,omitempty"`
}

type createAuthenticationProfileResponse struct {
	XMLName xml.Name                 `xml:"CreateAuthenticationProfileResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Profile xmlAuthenticationProfile `xml:"CreateAuthenticationProfileResult"`
}

func (h *Handler) handleCreateAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")
	content := vals.Get("AuthenticationProfileContent")

	ap, err := h.Backend.CreateAuthenticationProfile(name, content)
	if err != nil {
		return nil, err
	}

	return &createAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Profile: xmlAuthenticationProfile{
			AuthenticationProfileName:    ap.AuthenticationProfileName,
			AuthenticationProfileContent: ap.AuthenticationProfileContent,
		},
	}, nil
}

// ---- DeleteAuthenticationProfile ----

type deleteAuthenticationProfileResponse struct {
	XMLName xml.Name `xml:"DeleteAuthenticationProfileResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Name    string   `xml:"DeleteAuthenticationProfileResult>AuthenticationProfileName"`
}

func (h *Handler) handleDeleteAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")

	if err := h.Backend.DeleteAuthenticationProfile(name); err != nil {
		return nil, err
	}

	return &deleteAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Name:  name,
	}, nil
}

// ---- DescribeAuthenticationProfiles ----

type xmlAuthenticationProfileList struct {
	Profiles []xmlAuthenticationProfile `xml:"AuthenticationProfile"`
}

type describeAuthenticationProfilesResponse struct {
	XMLName  xml.Name                     `xml:"DescribeAuthenticationProfilesResponse"`
	Xmlns    string                       `xml:"xmlns,attr"`
	Profiles xmlAuthenticationProfileList `xml:"DescribeAuthenticationProfilesResult>AuthenticationProfiles"`
}

func (h *Handler) handleDescribeAuthenticationProfiles(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")

	profiles, err := h.Backend.DescribeAuthenticationProfiles(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlAuthenticationProfile, 0, len(profiles))

	for _, ap := range profiles {
		members = append(members, xmlAuthenticationProfile(ap))
	}

	return &describeAuthenticationProfilesResponse{
		Xmlns:    redshiftXMLNS,
		Profiles: xmlAuthenticationProfileList{Profiles: members},
	}, nil
}

// ---- ModifyAuthenticationProfile ----

type modifyAuthenticationProfileResponse struct {
	XMLName xml.Name                 `xml:"ModifyAuthenticationProfileResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Profile xmlAuthenticationProfile `xml:"ModifyAuthenticationProfileResult"`
}

func (h *Handler) handleModifyAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")
	content := vals.Get("AuthenticationProfileContent")

	ap, err := h.Backend.ModifyAuthenticationProfile(name, content)
	if err != nil {
		return nil, err
	}

	return &modifyAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Profile: xmlAuthenticationProfile{
			AuthenticationProfileName:    ap.AuthenticationProfileName,
			AuthenticationProfileContent: ap.AuthenticationProfileContent,
		},
	}, nil
}
