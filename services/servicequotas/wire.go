package servicequotas

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Wire shapes follow the pinned aws-sdk-go-v2/service/servicequotas v1.43.4 awsjson1.1 schemas;
// timestamps are epoch-seconds numbers.

type (
	emptyResp struct{}

	pageReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	listServicesReq struct{ pageReq }

	listQuotasReq struct {
		ServiceCode         string `json:"ServiceCode"`
		QuotaCode           string `json:"QuotaCode"`
		QuotaAppliedAtLevel string `json:"QuotaAppliedAtLevel"`
		pageReq
	}

	quotaCodesReq struct {
		ServiceCode string `json:"ServiceCode"`
		QuotaCode   string `json:"QuotaCode"`
		ContextID   string `json:"ContextId"`
	}

	requestIncreaseReq struct {
		DesiredValue       *float64 `json:"DesiredValue"`
		SupportCaseAllowed *bool    `json:"SupportCaseAllowed"`
		ServiceCode        string   `json:"ServiceCode"`
		QuotaCode          string   `json:"QuotaCode"`
		ContextID          string   `json:"ContextId"`
	}

	requestIDReq struct {
		RequestID string `json:"RequestId"`
	}

	listHistoryReq struct {
		ServiceCode           string `json:"ServiceCode"`
		Status                string `json:"Status"`
		QuotaRequestedAtLevel string `json:"QuotaRequestedAtLevel"`
		pageReq
	}

	listHistoryByQuotaReq struct {
		QuotaCode             string `json:"QuotaCode"`
		ServiceCode           string `json:"ServiceCode"`
		Status                string `json:"Status"`
		QuotaRequestedAtLevel string `json:"QuotaRequestedAtLevel"`
		pageReq
	}

	templateKeyReq struct {
		AwsRegion   string `json:"AwsRegion"`
		QuotaCode   string `json:"QuotaCode"`
		ServiceCode string `json:"ServiceCode"`
	}

	putTemplateReq struct {
		DesiredValue *float64 `json:"DesiredValue"`
		templateKeyReq
	}

	listTemplateReq struct {
		AwsRegion   string `json:"AwsRegion"`
		ServiceCode string `json:"ServiceCode"`
		pageReq
	}

	associationResp struct {
		ServiceQuotaTemplateAssociationStatus string `json:"ServiceQuotaTemplateAssociationStatus"`
	}

	autoStartReq struct {
		ExclusionList   map[string][]string `json:"ExclusionList"`
		NotificationArn *string             `json:"NotificationArn"`
		OptInLevel      string              `json:"OptInLevel"`
		OptInType       string              `json:"OptInType"`
	}

	autoUpdateReq struct {
		ExclusionList   map[string][]string `json:"ExclusionList"`
		NotificationArn *string             `json:"NotificationArn"`
		OptInType       string              `json:"OptInType"`
	}

	quotaInfoWire struct {
		QuotaCode string `json:"QuotaCode"`
		QuotaName string `json:"QuotaName"`
	}

	autoGetResp struct {
		ExclusionList   map[string][]quotaInfoWire `json:"ExclusionList,omitempty"`
		NotificationArn string                     `json:"NotificationArn,omitempty"`
		OptInLevel      string                     `json:"OptInLevel,omitempty"`
		OptInStatus     string                     `json:"OptInStatus"`
		OptInType       string                     `json:"OptInType,omitempty"`
	}

	startReportResp struct {
		Message  string `json:"Message"`
		ReportID string `json:"ReportId"`
		Status   string `json:"Status"`
	}

	getReportReq struct {
		ReportID string `json:"ReportId"`
		pageReq
	}

	utilizationWire struct {
		Namespace    string  `json:"Namespace,omitempty"`
		QuotaCode    string  `json:"QuotaCode"`
		QuotaName    string  `json:"QuotaName"`
		ServiceCode  string  `json:"ServiceCode"`
		ServiceName  string  `json:"ServiceName"`
		AppliedValue float64 `json:"AppliedValue"`
		DefaultValue float64 `json:"DefaultValue"`
		Utilization  float64 `json:"Utilization"`
		Adjustable   bool    `json:"Adjustable"`
	}

	getReportResp struct {
		GeneratedAt *float64          `json:"GeneratedAt,omitempty"`
		TotalCount  *int32            `json:"TotalCount,omitempty"`
		NextToken   string            `json:"NextToken,omitempty"`
		ReportID    string            `json:"ReportId"`
		Status      string            `json:"Status"`
		Quotas      []utilizationWire `json:"Quotas,omitempty"`
	}

	tagReq struct {
		ResourceARN string    `json:"ResourceARN"`
		Tags        []tags.KV `json:"Tags"`
		TagKeys     []string  `json:"TagKeys"`
	}

	listTagsResp struct {
		Tags []tags.KV `json:"Tags"`
	}
)

type serviceInfoWire struct {
	ServiceCode string `json:"ServiceCode"`
	ServiceName string `json:"ServiceName"`
}

type listServicesResp struct {
	NextToken string            `json:"NextToken,omitempty"`
	Services  []serviceInfoWire `json:"Services"`
}

type periodWire struct {
	PeriodUnit  string `json:"PeriodUnit"`
	PeriodValue int32  `json:"PeriodValue"`
}

type metricWire struct {
	MetricDimensions              map[string]string `json:"MetricDimensions,omitempty"`
	MetricName                    string            `json:"MetricName"`
	MetricNamespace               string            `json:"MetricNamespace"`
	MetricStatisticRecommendation string            `json:"MetricStatisticRecommendation"`
}

type quotaWire struct {
	Period              *periodWire `json:"Period,omitempty"`
	UsageMetric         *metricWire `json:"UsageMetric,omitempty"`
	Description         string      `json:"Description,omitempty"`
	QuotaAppliedAtLevel string      `json:"QuotaAppliedAtLevel,omitempty"`
	QuotaArn            string      `json:"QuotaArn"`
	QuotaCode           string      `json:"QuotaCode"`
	QuotaName           string      `json:"QuotaName"`
	ServiceCode         string      `json:"ServiceCode"`
	ServiceName         string      `json:"ServiceName"`
	Unit                string      `json:"Unit"`
	Value               float64     `json:"Value"`
	Adjustable          bool        `json:"Adjustable"`
	GlobalQuota         bool        `json:"GlobalQuota"`
}

func quotaToWire(v QuotaView, applied bool) quotaWire {
	w := quotaWire{
		Adjustable: v.Def.Adjustable, Description: v.Def.Description, GlobalQuota: v.Def.Global,
		QuotaArn: v.ARN, QuotaCode: v.Def.Code, QuotaName: v.Def.Name, ServiceCode: v.Service.Code,
		ServiceName: v.Service.Name, Unit: v.Def.Unit, Value: v.Value,
	}

	if applied {
		w.QuotaAppliedAtLevel = levelAccount
	}

	if u := v.Def.Usage; u != nil {
		w.UsageMetric = &metricWire{
			MetricDimensions: u.Dimensions, MetricName: u.Name, MetricNamespace: u.Namespace,
			MetricStatisticRecommendation: u.Statistic,
		}
	}

	return w
}

type quotaResp struct {
	Quota quotaWire `json:"Quota"`
}

type listQuotasResp struct {
	NextToken string      `json:"NextToken,omitempty"`
	Quotas    []quotaWire `json:"Quotas"`
}

type requestWire struct {
	ServiceCode           string  `json:"ServiceCode"`
	ServiceName           string  `json:"ServiceName"`
	ID                    string  `json:"Id"`
	Unit                  string  `json:"Unit"`
	QuotaArn              string  `json:"QuotaArn"`
	QuotaCode             string  `json:"QuotaCode"`
	Status                string  `json:"Status"`
	Requester             string  `json:"Requester"`
	QuotaName             string  `json:"QuotaName"`
	CaseID                string  `json:"CaseId,omitempty"`
	QuotaRequestedAtLevel string  `json:"QuotaRequestedAtLevel"`
	Created               float64 `json:"Created"`
	LastUpdated           float64 `json:"LastUpdated"`
	DesiredValue          float64 `json:"DesiredValue"`
	GlobalQuota           bool    `json:"GlobalQuota"`
}

func requestToWire(v RequestView) requestWire {
	r := v.Request

	return requestWire{
		CaseID: r.CaseID, Created: awstime.Epoch(r.Created), ID: r.ID, LastUpdated: awstime.Epoch(r.LastUpdated),
		QuotaArn: v.ARN, QuotaCode: r.QuotaCode, QuotaName: v.Def.Name, QuotaRequestedAtLevel: levelAccount,
		Requester: r.Requester, ServiceCode: r.ServiceCode, ServiceName: v.Service.Name, Status: r.Status,
		Unit: v.Def.Unit, DesiredValue: r.DesiredValue, GlobalQuota: v.Def.Global,
	}
}

type requestResp struct {
	RequestedQuota requestWire `json:"RequestedQuota"`
}

type listRequestsResp struct {
	NextToken       string        `json:"NextToken,omitempty"`
	RequestedQuotas []requestWire `json:"RequestedQuotas"`
}

type templateItemWire struct {
	AwsRegion    string  `json:"AwsRegion"`
	QuotaCode    string  `json:"QuotaCode"`
	QuotaName    string  `json:"QuotaName"`
	ServiceCode  string  `json:"ServiceCode"`
	ServiceName  string  `json:"ServiceName"`
	Unit         string  `json:"Unit"`
	DesiredValue float64 `json:"DesiredValue"`
	GlobalQuota  bool    `json:"GlobalQuota"`
}

func templateToWire(v TemplateView) templateItemWire {
	return templateItemWire{
		AwsRegion: v.Item.AwsRegion, QuotaCode: v.Item.QuotaCode, QuotaName: v.Def.Name,
		ServiceCode: v.Item.ServiceCode, ServiceName: v.Service.Name, Unit: v.Def.Unit,
		DesiredValue: v.Item.DesiredValue, GlobalQuota: v.Def.Global,
	}
}

type templateItemResp struct {
	Item templateItemWire `json:"ServiceQuotaIncreaseRequestInTemplate"`
}

type listTemplateResp struct {
	NextToken string             `json:"NextToken,omitempty"`
	Items     []templateItemWire `json:"ServiceQuotaIncreaseRequestInTemplateList"`
}
