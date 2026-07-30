package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

// handler_mac_hosts.go implements the HTTP handlers for the EC2 Mac
// Dedicated Host and Mac modification task family, backed by
// mac_hosts.go.

func registerMacHostOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeMacHosts"] = h.handleDescribeMacHosts
	ops["CreateMacSystemIntegrityProtectionModificationTask"] = h.handleCreateMacSIPModificationTask
	ops["CreateDelegateMacVolumeOwnershipTask"] = h.handleCreateDelegateMacVolumeOwnershipTask
	ops["DescribeMacModificationTasks"] = h.handleDescribeMacModificationTasks
}

// macHostSupportedOperations lists the operation names registered by
// registerMacHostOps, for GetSupportedOperations().
func macHostSupportedOperations() []string {
	return []string{
		"DescribeMacHosts",
		"CreateMacSystemIntegrityProtectionModificationTask",
		"CreateDelegateMacVolumeOwnershipTask",
		"DescribeMacModificationTasks",
	}
}

// ---- Response shapes ----

type macHostItem struct {
	HostID                       string   `xml:"hostId,omitempty"`
	MacOSLatestSupportedVersions []string `xml:"macOSLatestSupportedVersionSet>item,omitempty"`
}

func toMacHostItem(h *MacHost) macHostItem {
	return macHostItem{HostID: h.HostID, MacOSLatestSupportedVersions: h.MacOSLatestSupportedVersions}
}

type describeMacHostsResponse struct {
	XMLName    xml.Name `xml:"DescribeMacHostsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	MacHostSet struct {
		Items []macHostItem `xml:"item"`
	} `xml:"macHostSet"`
}

type macSIPConfigItem struct {
	AppleInternal         string `xml:"appleInternal,omitempty"`
	BaseSystem            string `xml:"baseSystem,omitempty"`
	DTraceRestrictions    string `xml:"dTraceRestrictions,omitempty"`
	DebuggingRestrictions string `xml:"debuggingRestrictions,omitempty"`
	FilesystemProtections string `xml:"filesystemProtections,omitempty"`
	KextSigning           string `xml:"kextSigning,omitempty"`
	NvramProtections      string `xml:"nvramProtections,omitempty"`
	Status                string `xml:"status,omitempty"`
}

type macModificationTaskItem struct {
	MacSystemIntegrityProtectionConfig *macSIPConfigItem `xml:"macSystemIntegrityProtectionConfig,omitempty"`
	MacModificationTaskID              string            `xml:"macModificationTaskId,omitempty"`
	InstanceID                         string            `xml:"instanceId,omitempty"`
	TaskType                           string            `xml:"taskType,omitempty"`
	TaskState                          string            `xml:"taskState,omitempty"`
	StartTime                          string            `xml:"startTime,omitempty"`
	TagSet                             []simpleTagItem   `xml:"tagSet>item"`
}

func toMacModificationTaskItem(t *MacModificationTask, tags map[string]string) macModificationTaskItem {
	item := macModificationTaskItem{
		MacModificationTaskID: t.MacModificationTaskID,
		InstanceID:            t.InstanceID,
		TaskType:              t.TaskType,
		TaskState:             t.TaskState,
		StartTime:             t.StartTime.UTC().Format(time.RFC3339),
		TagSet:                tagItemsFromMap(tags),
	}

	if t.SIPConfig != nil {
		item.MacSystemIntegrityProtectionConfig = &macSIPConfigItem{
			AppleInternal:         t.SIPConfig.AppleInternal,
			BaseSystem:            t.SIPConfig.BaseSystem,
			DTraceRestrictions:    t.SIPConfig.DTraceRestrictions,
			DebuggingRestrictions: t.SIPConfig.DebuggingRestrictions,
			FilesystemProtections: t.SIPConfig.FilesystemProtections,
			KextSigning:           t.SIPConfig.KextSigning,
			NvramProtections:      t.SIPConfig.NvramProtections,
			Status:                t.SIPConfig.Status,
		}
	}

	return item
}

type createMacSIPModificationTaskResponse struct {
	XMLName             xml.Name                `xml:"CreateMacSystemIntegrityProtectionModificationTaskResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	MacModificationTask macModificationTaskItem `xml:"macModificationTask"`
}

type createDelegateMacVolumeOwnershipTaskResponse struct {
	XMLName             xml.Name                `xml:"CreateDelegateMacVolumeOwnershipTaskResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	MacModificationTask macModificationTaskItem `xml:"macModificationTask"`
}

type describeMacModificationTasksResponse struct {
	XMLName                xml.Name `xml:"DescribeMacModificationTasksResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	MacModificationTaskSet struct {
		Items []macModificationTaskItem `xml:"item"`
	} `xml:"macModificationTaskSet"`
}

// ---- Handlers ----

func (h *Handler) handleDescribeMacHosts(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "HostId")
	hosts := h.Backend.DescribeMacHosts(ids)

	resp := &describeMacHostsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, hst := range hosts {
		resp.MacHostSet.Items = append(resp.MacHostSet.Items, toMacHostItem(hst))
	}

	return resp, nil
}

func parseMacSIPConfig(vals url.Values) *MacSIPConfig {
	prefix := "MacSystemIntegrityProtectionConfiguration."
	cfg := MacSIPConfig{
		AppleInternal:         vals.Get(prefix + "AppleInternal"),
		BaseSystem:            vals.Get(prefix + "BaseSystem"),
		DTraceRestrictions:    vals.Get(prefix + "DTraceRestrictions"),
		DebuggingRestrictions: vals.Get(prefix + "DebuggingRestrictions"),
		FilesystemProtections: vals.Get(prefix + "FilesystemProtections"),
		KextSigning:           vals.Get(prefix + "KextSigning"),
		NvramProtections:      vals.Get(prefix + "NvramProtections"),
	}

	if cfg == (MacSIPConfig{}) {
		return nil
	}

	return &cfg
}

func (h *Handler) handleCreateMacSIPModificationTask(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.CreateMacSystemIntegrityProtectionModificationTask(
		vals.Get("InstanceId"),
		vals.Get("MacSystemIntegrityProtectionStatus"),
		parseMacSIPConfig(vals),
		parseTagSpecification(vals, resourceTypeMacModificationTask),
	)
	if err != nil {
		return nil, err
	}

	return &createMacSIPModificationTaskResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		MacModificationTask: toMacModificationTaskItem(
			task, h.Backend.TagsForResource(task.MacModificationTaskID),
		),
	}, nil
}

func (h *Handler) handleCreateDelegateMacVolumeOwnershipTask(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.CreateDelegateMacVolumeOwnershipTask(
		vals.Get("InstanceId"),
		vals.Get("MacCredentials"),
		parseTagSpecification(vals, resourceTypeMacModificationTask),
	)
	if err != nil {
		return nil, err
	}

	return &createDelegateMacVolumeOwnershipTaskResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		MacModificationTask: toMacModificationTaskItem(
			task, h.Backend.TagsForResource(task.MacModificationTaskID),
		),
	}, nil
}

func (h *Handler) handleDescribeMacModificationTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "MacModificationTaskId")
	tasks := h.Backend.DescribeMacModificationTasks(ids)

	resp := &describeMacModificationTasksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tasks {
		resp.MacModificationTaskSet.Items = append(
			resp.MacModificationTaskSet.Items,
			toMacModificationTaskItem(t, h.Backend.TagsForResource(t.MacModificationTaskID)),
		)
	}

	return resp, nil
}
