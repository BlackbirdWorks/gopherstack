package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultIRMaxRecords and maxIRMaxRecords are DescribeInstanceRefreshes's documented
// default/max page size (api_op_DescribeInstanceRefreshes.go: "The default value is 50 and the
// maximum value is 100").
const (
	defaultIRMaxRecords = 50
	maxIRMaxRecords     = 100
)

func (h *Handler) handleCancelInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	refreshID, err := h.Backend.CancelInstanceRefresh(groupName)
	if err != nil {
		return nil, err
	}

	return &cancelInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: cancelInstanceRefreshResult{
			InstanceRefreshID: refreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-cancel-refresh"},
	}, nil
}

type cancelInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type cancelInstanceRefreshResponse struct {
	XMLName          xml.Name                    `xml:"CancelInstanceRefreshResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           cancelInstanceRefreshResult `xml:"CancelInstanceRefreshResult"`
}

func (h *Handler) handleDescribeInstanceRefreshes(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	refreshIDs := parseMembers(vals, "InstanceRefreshIds.member")

	refreshes, err := h.Backend.DescribeInstanceRefreshes(groupName, refreshIDs)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultIRMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), maxIRMaxRecords)
		}
	}

	p := page.New(refreshes, vals.Get("NextToken"), maxRecords, defaultIRMaxRecords)

	members := make([]xmlInstanceRefresh, 0, len(p.Data))
	for _, r := range p.Data {
		endTime := ""
		if !r.EndTime.IsZero() {
			endTime = r.EndTime.UTC().Format(time.RFC3339)
		}

		members = append(members, xmlInstanceRefresh{
			InstanceRefreshID:         r.InstanceRefreshID,
			AutoScalingGroupName:      r.AutoScalingGroupName,
			Status:                    r.Status,
			StatusReason:              r.StatusReason,
			StartTime:                 r.StartTime.UTC().Format(time.RFC3339),
			EndTime:                   endTime,
			Strategy:                  r.Strategy,
			PercentageComplete:        r.PercentageComplete,
			InstancesToUpdate:         r.InstancesToUpdate,
			MinHealthyPercentage:      r.Preferences.MinHealthyPercentage,
			MaxHealthyPercentage:      r.Preferences.MaxHealthyPercentage,
			InstanceWarmup:            r.Preferences.InstanceWarmup,
			SkipMatching:              r.Preferences.SkipMatching,
			AutoRollback:              r.Preferences.AutoRollback,
			ScaleInProtectedInstances: r.Preferences.ScaleInProtectedInstances,
			StandbyInstances:          r.Preferences.StandbyInstances,
		})
	}

	return &describeInstanceRefreshesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeInstanceRefreshesResult{
			NextToken:         p.Next,
			InstanceRefreshes: xmlInstanceRefreshList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-instance-refreshes"},
	}, nil
}

func (h *Handler) handleStartInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	strategy := vals.Get("Strategy")

	minHealthy, err := parseIntVal(vals.Get("Preferences.MinHealthyPercentage"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.MinHealthyPercentage", ErrInvalidParameter)
	}

	maxHealthy, err := parseIntVal(vals.Get("Preferences.MaxHealthyPercentage"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.MaxHealthyPercentage", ErrInvalidParameter)
	}

	instanceWarmup, err := parseIntVal(vals.Get("Preferences.InstanceWarmup"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.InstanceWarmup", ErrInvalidParameter)
	}

	checkpointDelay, err := parseIntVal(vals.Get("Preferences.CheckpointDelay"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Preferences.CheckpointDelay", ErrInvalidParameter)
	}

	prefs := InstanceRefreshPreferences{
		MinHealthyPercentage:      minHealthy,
		MaxHealthyPercentage:      maxHealthy,
		InstanceWarmup:            instanceWarmup,
		CheckpointDelay:           checkpointDelay,
		SkipMatching:              vals.Get("Preferences.SkipMatching") == formValueTrue,
		AutoRollback:              vals.Get("Preferences.AutoRollback") == formValueTrue,
		ScaleInProtectedInstances: vals.Get("Preferences.ScaleInProtectedInstances"),
		StandbyInstances:          vals.Get("Preferences.StandbyInstances"),
	}

	refresh, err := h.Backend.StartInstanceRefreshWithInput(StartInstanceRefreshInput{
		AutoScalingGroupName: groupName,
		Strategy:             strategy,
		Preferences:          prefs,
	})
	if err != nil {
		return nil, err
	}

	return &startInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: startInstanceRefreshResult{
			InstanceRefreshID: refresh.InstanceRefreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-start-instance-refresh"},
	}, nil
}

func (h *Handler) handleRollbackInstanceRefresh(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	refreshID, err := h.Backend.RollbackInstanceRefresh(groupName)
	if err != nil {
		return nil, err
	}

	return &rollbackInstanceRefreshResponse{
		Xmlns: autoscalingXMLNS,
		Result: rollbackInstanceRefreshResult{
			InstanceRefreshID: refreshID,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-rollback-instance-refresh"},
	}, nil
}

type xmlInstanceRefresh struct {
	InstanceRefreshID         string `xml:"InstanceRefreshId"`
	AutoScalingGroupName      string `xml:"AutoScalingGroupName"`
	Status                    string `xml:"Status"`
	StatusReason              string `xml:"StatusReason,omitempty"`
	StartTime                 string `xml:"StartTime"`
	EndTime                   string `xml:"EndTime,omitempty"`
	Strategy                  string `xml:"Strategy,omitempty"`
	ScaleInProtectedInstances string `xml:"Preferences>ScaleInProtectedInstances,omitempty"`
	StandbyInstances          string `xml:"Preferences>StandbyInstances,omitempty"`
	MinHealthyPercentage      int32  `xml:"Preferences>MinHealthyPercentage,omitempty"`
	MaxHealthyPercentage      int32  `xml:"Preferences>MaxHealthyPercentage,omitempty"`
	InstanceWarmup            int32  `xml:"Preferences>InstanceWarmup,omitempty"`
	PercentageComplete        int32  `xml:"PercentageComplete,omitempty"`
	InstancesToUpdate         int32  `xml:"InstancesToUpdate,omitempty"`
	SkipMatching              bool   `xml:"Preferences>SkipMatching,omitempty"`
	AutoRollback              bool   `xml:"Preferences>AutoRollback,omitempty"`
}

type xmlInstanceRefreshList struct {
	Members []xmlInstanceRefresh `xml:"member"`
}

type describeInstanceRefreshesResult struct {
	NextToken         string                 `xml:"NextToken,omitempty"`
	InstanceRefreshes xmlInstanceRefreshList `xml:"InstanceRefreshes"`
}

type describeInstanceRefreshesResponse struct {
	XMLName          xml.Name                        `xml:"DescribeInstanceRefreshesResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeInstanceRefreshesResult `xml:"DescribeInstanceRefreshesResult"`
}

type startInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type startInstanceRefreshResponse struct {
	XMLName          xml.Name                   `xml:"StartInstanceRefreshResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           startInstanceRefreshResult `xml:"StartInstanceRefreshResult"`
}

type rollbackInstanceRefreshResult struct {
	InstanceRefreshID string `xml:"InstanceRefreshId"`
}

type rollbackInstanceRefreshResponse struct {
	XMLName          xml.Name                      `xml:"RollbackInstanceRefreshResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           rollbackInstanceRefreshResult `xml:"RollbackInstanceRefreshResult"`
}
