package ec2

import (
	"encoding/xml"
	"net/url"
)

// ---- Registration ----

func registerVerifiedAccessPolicyOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["GetVerifiedAccessEndpointPolicy"] = h.handleGetVerifiedAccessEndpointPolicy
	ops["ModifyVerifiedAccessEndpointPolicy"] = h.handleModifyVerifiedAccessEndpointPolicy
	ops["GetVerifiedAccessGroupPolicy"] = h.handleGetVerifiedAccessGroupPolicy
	ops["ModifyVerifiedAccessGroupPolicy"] = h.handleModifyVerifiedAccessGroupPolicy
	ops["DescribeVerifiedAccessInstanceLoggingConfigurations"] = h.
		handleDescribeVerifiedAccessInstanceLoggingConfigurations
	ops["ModifyVerifiedAccessInstanceLoggingConfiguration"] = h.handleModifyVerifiedAccessInstanceLoggingConfiguration
	ops["GetVerifiedAccessEndpointTargets"] = h.handleGetVerifiedAccessEndpointTargets
	ops["ExportVerifiedAccessInstanceClientConfiguration"] = h.handleExportVerifiedAccessInstanceClientConfiguration
}

// verifiedAccessPolicySupportedOperations lists the operation names registered
// by registerVerifiedAccessPolicyOps, for GetSupportedOperations().
func verifiedAccessPolicySupportedOperations() []string {
	return []string{
		"GetVerifiedAccessEndpointPolicy",
		"ModifyVerifiedAccessEndpointPolicy",
		"GetVerifiedAccessGroupPolicy",
		"ModifyVerifiedAccessGroupPolicy",
		"DescribeVerifiedAccessInstanceLoggingConfigurations",
		"ModifyVerifiedAccessInstanceLoggingConfiguration",
		"GetVerifiedAccessEndpointTargets",
		"ExportVerifiedAccessInstanceClientConfiguration",
	}
}

// ---- Response shapes ----

type getVerifiedAccessEndpointPolicyResponse struct {
	XMLName        xml.Name `xml:"GetVerifiedAccessEndpointPolicyResponse"`
	RequestID      string   `xml:"requestId"`
	PolicyDocument string   `xml:"policyDocument,omitempty"`
	PolicyEnabled  bool     `xml:"policyEnabled"`
}

type modifyVerifiedAccessEndpointPolicyResponse struct {
	XMLName        xml.Name `xml:"ModifyVerifiedAccessEndpointPolicyResponse"`
	RequestID      string   `xml:"requestId"`
	PolicyDocument string   `xml:"policyDocument,omitempty"`
	PolicyEnabled  bool     `xml:"policyEnabled"`
}

type getVerifiedAccessGroupPolicyResponse struct {
	XMLName        xml.Name `xml:"GetVerifiedAccessGroupPolicyResponse"`
	RequestID      string   `xml:"requestId"`
	PolicyDocument string   `xml:"policyDocument,omitempty"`
	PolicyEnabled  bool     `xml:"policyEnabled"`
}

type modifyVerifiedAccessGroupPolicyResponse struct {
	XMLName        xml.Name `xml:"ModifyVerifiedAccessGroupPolicyResponse"`
	RequestID      string   `xml:"requestId"`
	PolicyDocument string   `xml:"policyDocument,omitempty"`
	PolicyEnabled  bool     `xml:"policyEnabled"`
}

type verifiedAccessLogCloudWatchLogsXML struct {
	LogGroup string `xml:"logGroup,omitempty"`
	Enabled  bool   `xml:"enabled"`
}

type verifiedAccessLogKinesisDataFirehoseXML struct {
	DeliveryStream string `xml:"deliveryStream,omitempty"`
	Enabled        bool   `xml:"enabled"`
}

type verifiedAccessLogS3XML struct {
	BucketName  string `xml:"bucketName,omitempty"`
	BucketOwner string `xml:"bucketOwner,omitempty"`
	Prefix      string `xml:"prefix,omitempty"`
	Enabled     bool   `xml:"enabled"`
}

type verifiedAccessLogsXML struct {
	CloudWatchLogs      *verifiedAccessLogCloudWatchLogsXML      `xml:"cloudWatchLogs,omitempty"`
	KinesisDataFirehose *verifiedAccessLogKinesisDataFirehoseXML `xml:"kinesisDataFirehose,omitempty"`
	S3                  *verifiedAccessLogS3XML                  `xml:"s3,omitempty"`
	LogVersion          string                                   `xml:"logVersion,omitempty"`
	IncludeTrustContext bool                                     `xml:"includeTrustContext"`
}

type vaLoggingConfigXML struct {
	VerifiedAccessInstanceID string                `xml:"verifiedAccessInstanceId"`
	AccessLogs               verifiedAccessLogsXML `xml:"accessLogs"`
}

type describeVerifiedAccessInstanceLoggingConfigurationsResponse struct {
	XMLName                 xml.Name `xml:"DescribeVerifiedAccessInstanceLoggingConfigurationsResponse"`
	RequestID               string   `xml:"requestId"`
	LoggingConfigurationSet struct {
		Items []vaLoggingConfigXML `xml:"item"`
	} `xml:"loggingConfigurationSet"`
}

type modifyVerifiedAccessInstanceLoggingConfigurationResponse struct {
	XMLName              xml.Name           `xml:"ModifyVerifiedAccessInstanceLoggingConfigurationResponse"`
	RequestID            string             `xml:"requestId"`
	LoggingConfiguration vaLoggingConfigXML `xml:"loggingConfiguration"`
}

type verifiedAccessEndpointTargetXML struct {
	VerifiedAccessEndpointID              string `xml:"verifiedAccessEndpointId,omitempty"`
	VerifiedAccessEndpointTargetIPAddress string `xml:"verifiedAccessEndpointTargetIpAddress,omitempty"`
	VerifiedAccessEndpointTargetDNS       string `xml:"verifiedAccessEndpointTargetDns,omitempty"`
}

type getVerifiedAccessEndpointTargetsResponse struct {
	XMLName                         xml.Name `xml:"GetVerifiedAccessEndpointTargetsResponse"`
	RequestID                       string   `xml:"requestId"`
	VerifiedAccessEndpointTargetSet struct {
		Items []verifiedAccessEndpointTargetXML `xml:"item"`
	} `xml:"verifiedAccessEndpointTargetSet"`
}

type exportVerifiedAccessInstanceClientConfigurationResponse struct {
	XMLName                  xml.Name `xml:"ExportVerifiedAccessInstanceClientConfigurationResponse"`
	RequestID                string   `xml:"requestId"`
	VerifiedAccessInstanceID string   `xml:"verifiedAccessInstanceId,omitempty"`
	Region                   string   `xml:"region,omitempty"`
	Version                  string   `xml:"version,omitempty"`
	DeviceTrustProviderSet   struct {
		Items []string `xml:"item"`
	} `xml:"deviceTrustProviderSet"`
	OpenVpnConfigurationSet struct {
		Items []string `xml:"item"`
	} `xml:"openVpnConfigurationSet"`
}

// ---- Handlers ----

func (h *Handler) handleGetVerifiedAccessEndpointPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")

	pol, err := h.Backend.GetVerifiedAccessEndpointPolicy(id)
	if err != nil {
		return nil, err
	}

	return &getVerifiedAccessEndpointPolicyResponse{
		RequestID:      reqID,
		PolicyEnabled:  pol.PolicyEnabled,
		PolicyDocument: pol.PolicyDocument,
	}, nil
}

func (h *Handler) handleModifyVerifiedAccessEndpointPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")
	policyEnabled := vals.Get("PolicyEnabled") == ec2BooleanTrue
	policyDocument := vals.Get("PolicyDocument")

	pol, err := h.Backend.ModifyVerifiedAccessEndpointPolicy(id, policyEnabled, policyDocument)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessEndpointPolicyResponse{
		RequestID:      reqID,
		PolicyEnabled:  pol.PolicyEnabled,
		PolicyDocument: pol.PolicyDocument,
	}, nil
}

func (h *Handler) handleGetVerifiedAccessGroupPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")

	pol, err := h.Backend.GetVerifiedAccessGroupPolicy(id)
	if err != nil {
		return nil, err
	}

	return &getVerifiedAccessGroupPolicyResponse{
		RequestID:      reqID,
		PolicyEnabled:  pol.PolicyEnabled,
		PolicyDocument: pol.PolicyDocument,
	}, nil
}

func (h *Handler) handleModifyVerifiedAccessGroupPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")
	policyEnabled := vals.Get("PolicyEnabled") == ec2BooleanTrue
	policyDocument := vals.Get("PolicyDocument")

	pol, err := h.Backend.ModifyVerifiedAccessGroupPolicy(id, policyEnabled, policyDocument)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessGroupPolicyResponse{
		RequestID:      reqID,
		PolicyEnabled:  pol.PolicyEnabled,
		PolicyDocument: pol.PolicyDocument,
	}, nil
}

// toVerifiedAccessLogsXML converts stored logging-configuration state into
// its EC2 query-XML representation.
func toVerifiedAccessLogsXML(opts VerifiedAccessLogOptions) verifiedAccessLogsXML {
	out := verifiedAccessLogsXML{
		LogVersion:          opts.LogVersion,
		IncludeTrustContext: opts.IncludeTrustContext,
	}

	if opts.CloudWatchLogs != nil {
		out.CloudWatchLogs = &verifiedAccessLogCloudWatchLogsXML{
			Enabled:  opts.CloudWatchLogs.Enabled,
			LogGroup: opts.CloudWatchLogs.LogGroup,
		}
	}

	if opts.KinesisDataFirehose != nil {
		out.KinesisDataFirehose = &verifiedAccessLogKinesisDataFirehoseXML{
			Enabled:        opts.KinesisDataFirehose.Enabled,
			DeliveryStream: opts.KinesisDataFirehose.DeliveryStream,
		}
	}

	if opts.S3 != nil {
		out.S3 = &verifiedAccessLogS3XML{
			Enabled:     opts.S3.Enabled,
			BucketName:  opts.S3.BucketName,
			BucketOwner: opts.S3.BucketOwner,
			Prefix:      opts.S3.Prefix,
		}
	}

	return out
}

func (h *Handler) handleDescribeVerifiedAccessInstanceLoggingConfigurations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessInstanceId")
	cfgs := h.Backend.DescribeVerifiedAccessInstanceLoggingConfigurations(ids)

	resp := &describeVerifiedAccessInstanceLoggingConfigurationsResponse{RequestID: reqID}
	for _, cfg := range cfgs {
		resp.LoggingConfigurationSet.Items = append(
			resp.LoggingConfigurationSet.Items,
			vaLoggingConfigXML{
				VerifiedAccessInstanceID: cfg.VerifiedAccessInstanceID,
				AccessLogs:               toVerifiedAccessLogsXML(cfg.AccessLogs),
			},
		)
	}

	return resp, nil
}

// parseVerifiedAccessLogOptions parses the AccessLogs.* form fields sent by
// ModifyVerifiedAccessInstanceLoggingConfiguration.
func parseVerifiedAccessLogOptions(vals url.Values) VerifiedAccessLogOptions {
	opts := VerifiedAccessLogOptions{
		LogVersion:          vals.Get("AccessLogs.LogVersion"),
		IncludeTrustContext: vals.Get("AccessLogs.IncludeTrustContext") == ec2BooleanTrue,
	}

	if vals.Get("AccessLogs.CloudWatchLogs.Enabled") != "" {
		opts.CloudWatchLogs = &VerifiedAccessLogCloudWatchLogs{
			Enabled:  vals.Get("AccessLogs.CloudWatchLogs.Enabled") == ec2BooleanTrue,
			LogGroup: vals.Get("AccessLogs.CloudWatchLogs.LogGroup"),
		}
	}

	if vals.Get("AccessLogs.KinesisDataFirehose.Enabled") != "" {
		opts.KinesisDataFirehose = &VerifiedAccessLogKinesisDataFirehose{
			Enabled:        vals.Get("AccessLogs.KinesisDataFirehose.Enabled") == ec2BooleanTrue,
			DeliveryStream: vals.Get("AccessLogs.KinesisDataFirehose.DeliveryStream"),
		}
	}

	if vals.Get("AccessLogs.S3.Enabled") != "" {
		opts.S3 = &VerifiedAccessLogS3{
			Enabled:     vals.Get("AccessLogs.S3.Enabled") == ec2BooleanTrue,
			BucketName:  vals.Get("AccessLogs.S3.BucketName"),
			BucketOwner: vals.Get("AccessLogs.S3.BucketOwner"),
			Prefix:      vals.Get("AccessLogs.S3.Prefix"),
		}
	}

	return opts
}

func (h *Handler) handleModifyVerifiedAccessInstanceLoggingConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	opts := parseVerifiedAccessLogOptions(vals)

	cfg, err := h.Backend.ModifyVerifiedAccessInstanceLoggingConfiguration(instanceID, opts)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessInstanceLoggingConfigurationResponse{
		RequestID: reqID,
		LoggingConfiguration: vaLoggingConfigXML{
			VerifiedAccessInstanceID: cfg.VerifiedAccessInstanceID,
			AccessLogs:               toVerifiedAccessLogsXML(cfg.AccessLogs),
		},
	}, nil
}

func (h *Handler) handleGetVerifiedAccessEndpointTargets(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")

	targets, err := h.Backend.GetVerifiedAccessEndpointTargets(id)
	if err != nil {
		return nil, err
	}

	resp := &getVerifiedAccessEndpointTargetsResponse{RequestID: reqID}
	for _, t := range targets {
		resp.VerifiedAccessEndpointTargetSet.Items = append(
			resp.VerifiedAccessEndpointTargetSet.Items,
			verifiedAccessEndpointTargetXML{
				VerifiedAccessEndpointID:              t.VerifiedAccessEndpointID,
				VerifiedAccessEndpointTargetIPAddress: t.VerifiedAccessEndpointTargetIPAddress,
				VerifiedAccessEndpointTargetDNS:       t.VerifiedAccessEndpointTargetDNS,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleExportVerifiedAccessInstanceClientConfiguration(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")

	cfg, err := h.Backend.ExportVerifiedAccessInstanceClientConfiguration(instanceID)
	if err != nil {
		return nil, err
	}

	return &exportVerifiedAccessInstanceClientConfigurationResponse{
		RequestID:                reqID,
		VerifiedAccessInstanceID: cfg.VerifiedAccessInstanceID,
		Region:                   cfg.Region,
		Version:                  cfg.Version,
	}, nil
}
