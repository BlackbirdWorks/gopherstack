package oam

// Wire shapes follow the pinned aws-sdk-go-v2/service/oam v1.31.4 restjson1 serializers; every
// operation is POST /<Operation> with a JSON body except the three tag operations on /tags/{ResourceArn}.

type (
	pageReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	createSinkReq struct {
		Tags map[string]string `json:"Tags"`
		Name string            `json:"Name"`
	}

	identifierReq struct {
		IncludeTags *bool  `json:"IncludeTags"`
		Identifier  string `json:"Identifier"`
	}

	sinkPolicyReq struct {
		Policy         string `json:"Policy"`
		SinkIdentifier string `json:"SinkIdentifier"`
	}

	attachedLinksReq struct {
		SinkIdentifier string `json:"SinkIdentifier"`
		pageReq
	}

	linkConfigWire struct {
		LogGroupConfiguration *filterWire `json:"LogGroupConfiguration,omitempty"`
		MetricConfiguration   *filterWire `json:"MetricConfiguration,omitempty"`
	}

	filterWire struct {
		Filter *string `json:"Filter"`
	}

	createLinkReq struct {
		LinkConfiguration *linkConfigWire   `json:"LinkConfiguration"`
		Tags              map[string]string `json:"Tags"`
		LabelTemplate     string            `json:"LabelTemplate"`
		SinkIdentifier    string            `json:"SinkIdentifier"`
		ResourceTypes     []string          `json:"ResourceTypes"`
	}

	updateLinkReq struct {
		LinkConfiguration *linkConfigWire `json:"LinkConfiguration"`
		IncludeTags       *bool           `json:"IncludeTags"`
		Identifier        string          `json:"Identifier"`
		ResourceTypes     []string        `json:"ResourceTypes"`
	}

	tagReq struct {
		Tags map[string]string `json:"Tags"`
	}

	sinkResp struct {
		Tags map[string]string `json:"Tags,omitempty"`
		Arn  string            `json:"Arn"`
		ID   string            `json:"Id"`
		Name string            `json:"Name"`
	}

	linkResp struct {
		LinkConfiguration *linkConfigWire   `json:"LinkConfiguration,omitempty"`
		Tags              map[string]string `json:"Tags,omitempty"`
		Arn               string            `json:"Arn"`
		ID                string            `json:"Id"`
		Label             string            `json:"Label"`
		LabelTemplate     string            `json:"LabelTemplate"`
		SinkArn           string            `json:"SinkArn"`
		ResourceTypes     []string          `json:"ResourceTypes"`
	}

	sinkPolicyResp struct {
		Policy  string `json:"Policy"`
		SinkArn string `json:"SinkArn"`
		SinkID  string `json:"SinkId"`
	}

	listSinksItem struct {
		Arn  string `json:"Arn"`
		ID   string `json:"Id"`
		Name string `json:"Name"`
	}

	listSinksResp struct {
		NextToken string          `json:"NextToken,omitempty"`
		Items     []listSinksItem `json:"Items"`
	}

	listLinksItem struct {
		Arn           string   `json:"Arn"`
		ID            string   `json:"Id"`
		Label         string   `json:"Label"`
		SinkArn       string   `json:"SinkArn"`
		ResourceTypes []string `json:"ResourceTypes"`
	}

	listLinksResp struct {
		NextToken string          `json:"NextToken,omitempty"`
		Items     []listLinksItem `json:"Items"`
	}

	attachedLinkItem struct {
		Label         string   `json:"Label"`
		LinkArn       string   `json:"LinkArn"`
		ResourceTypes []string `json:"ResourceTypes"`
	}

	listAttachedLinksResp struct {
		NextToken string             `json:"NextToken,omitempty"`
		Items     []attachedLinkItem `json:"Items"`
	}

	listTagsResp struct {
		Tags map[string]string `json:"Tags"`
	}
)

func configToWire(l *Link) *linkConfigWire {
	if l.LogGroupFilter == nil && l.MetricFilter == nil {
		return nil
	}

	w := &linkConfigWire{}
	if l.LogGroupFilter != nil {
		w.LogGroupConfiguration = &filterWire{Filter: l.LogGroupFilter}
	}

	if l.MetricFilter != nil {
		w.MetricConfiguration = &filterWire{Filter: l.MetricFilter}
	}

	return w
}

func configFromWire(w *linkConfigWire) *LinkConfig {
	if w == nil {
		return nil
	}

	cfg := &LinkConfig{}
	if w.LogGroupConfiguration != nil {
		cfg.LogGroupFilter = filterOrEmpty(w.LogGroupConfiguration.Filter)
	}

	if w.MetricConfiguration != nil {
		cfg.MetricFilter = filterOrEmpty(w.MetricConfiguration.Filter)
	}

	return cfg
}

func filterOrEmpty(f *string) *string {
	if f == nil {
		empty := ""

		return &empty
	}

	return f
}

func linkToResp(l *Link, withTags bool) *linkResp {
	r := &linkResp{
		Arn: l.ARN, ID: l.ID, Label: l.Label, LabelTemplate: l.LabelTemplate, SinkArn: l.SinkARN,
		ResourceTypes: l.ResourceTypes, LinkConfiguration: configToWire(l),
	}

	if withTags && len(l.Tags) > 0 {
		r.Tags = l.Tags
	}

	return r
}

func sinkToResp(s *Sink, withTags bool) *sinkResp {
	r := &sinkResp{Arn: s.ARN, ID: s.ID, Name: s.Name}
	if withTags && len(s.Tags) > 0 {
		r.Tags = s.Tags
	}

	return r
}
