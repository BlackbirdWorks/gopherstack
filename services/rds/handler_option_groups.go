package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCreateOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	engine := vals.Get("EngineName")
	majorVersion := vals.Get("MajorEngineVersion")
	description := vals.Get("OptionGroupDescription")
	og, err := h.Backend.CreateOptionGroup(name, engine, majorVersion, description)
	if err != nil {
		return nil, err
	}

	return &createOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroups(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	groups, err := h.Backend.DescribeOptionGroups(name)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, groups, func(a, b OptionGroup) bool {
		return a.OptionGroupName < b.OptionGroupName
	}, func(item OptionGroup) xmlOptionGroup {
		cp := item

		return toXMLOptionGroup(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeOptionGroupsResponse{
		Xmlns:            rdsXMLNS,
		OptionGroupsList: xmlOptionGroupList{Members: members},
		Marker:           marker,
	}, nil
}

func (h *Handler) handleDeleteOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	if err := h.Backend.DeleteOptionGroup(name); err != nil {
		return nil, err
	}

	return &deleteOptionGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	var optionsToAdd []OptionGroupOption
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionName", i))
		if optName == "" {
			break
		}
		optionsToAdd = append(optionsToAdd, OptionGroupOption{
			OptionName:    optName,
			OptionVersion: vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionVersion", i)),
		})
	}
	var optionsToRemove []string
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToRemove.member.%d", i))
		if optName == "" {
			break
		}
		optionsToRemove = append(optionsToRemove, optName)
	}
	og, err := h.Backend.ModifyOptionGroup(name, optionsToAdd, optionsToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroupOptions(_ url.Values) (any, error) {
	return &describeOptionGroupOptionsResponse{Xmlns: rdsXMLNS}, nil
}

func toXMLOptionGroup(og *OptionGroup) xmlOptionGroup {
	opts := make([]xmlOptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		opts = append(opts, xmlOptionGroupOption(o))
	}

	return xmlOptionGroup{
		OptionGroupName:        og.OptionGroupName,
		OptionGroupDescription: og.OptionGroupDescription,
		EngineName:             og.EngineName,
		MajorEngineVersion:     og.MajorEngineVersion,
		Options:                xmlOptionGroupOptionList{Members: opts},
	}
}

type xmlOptionGroupOption struct {
	OptionName    string `xml:"OptionName"`
	OptionVersion string `xml:"OptionVersion,omitempty"`
}

type xmlOptionGroupOptionList struct {
	Members []xmlOptionGroupOption `xml:"Option"`
}

type xmlOptionGroup struct {
	OptionGroupName        string                   `xml:"OptionGroupName"`
	OptionGroupDescription string                   `xml:"OptionGroupDescription"`
	EngineName             string                   `xml:"EngineName"`
	MajorEngineVersion     string                   `xml:"MajorEngineVersion"`
	Options                xmlOptionGroupOptionList `xml:"Options"`
}

type xmlOptionGroupList struct {
	Members []xmlOptionGroup `xml:"OptionGroup"`
}

type createOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"CreateOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"CreateOptionGroupResult>OptionGroup"`
}

type describeOptionGroupsResponse struct {
	XMLName          xml.Name           `xml:"DescribeOptionGroupsResponse"`
	Xmlns            string             `xml:"xmlns,attr"`
	Marker           string             `xml:"DescribeOptionGroupsResult>Marker,omitempty"`
	OptionGroupsList xmlOptionGroupList `xml:"DescribeOptionGroupsResult>OptionGroupsList"`
}

type deleteOptionGroupResponse struct {
	XMLName xml.Name `xml:"DeleteOptionGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"ModifyOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"ModifyOptionGroupResult>OptionGroup"`
}

type describeOptionGroupOptionsResponse struct {
	XMLName xml.Name `xml:"DescribeOptionGroupOptionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleCopyOptionGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceOptionGroupIdentifier")
	targetGroupName := vals.Get("TargetOptionGroupIdentifier")
	targetDescription := vals.Get("TargetOptionGroupDescription")

	og, err := h.Backend.CopyOptionGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

type copyOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"CopyOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"CopyOptionGroupResult>OptionGroup"`
}
