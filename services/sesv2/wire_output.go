package sesv2

// This file holds AWS-shaped ("wire") response DTOs and conversion helpers
// for backend entities whose internal Go structs (backend.go, backend_ops.go)
// intentionally keep lowerCamelCase JSON tags for the on-disk snapshot format
// (see persistence.go). Marshalling those internal structs directly as HTTP
// responses produced the wrong field casing/shape for a real aws-sdk-go-v2
// client (e.g. "poolName" instead of "PoolName", or a missing
// "DedicatedIpPool" wrapper) -- these types fix that without touching the
// persisted shape. Field names and nesting were verified against
// aws-sdk-go-v2/service/sesv2 v1.60.1's types package; see PARITY.md.

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// ---- contact list ----

// contactListOutput mirrors GetContactListOutput's top-level fields.
type contactListOutput struct {
	ContactListName      string     `json:"ContactListName"`
	Description          string     `json:"Description,omitempty"`
	Tags                 []tagEntry `json:"Tags,omitempty"`
	CreatedTimestamp     float64    `json:"CreatedTimestamp,omitempty"`
	LastUpdatedTimestamp float64    `json:"LastUpdatedTimestamp,omitempty"`
}

func toContactListOutput(cl *ContactList) *contactListOutput {
	return &contactListOutput{
		ContactListName:      cl.Name,
		Description:          cl.Description,
		CreatedTimestamp:     awstime.Epoch(cl.CreatedAt),
		LastUpdatedTimestamp: awstime.Epoch(cl.LastUpdatedAt),
		Tags:                 tagsToEntries(cl.Tags),
	}
}

// contactListSummaryOutput mirrors types.ContactList, the ListContactLists
// item shape -- notably it has neither Description nor CreatedTimestamp.
type contactListSummaryOutput struct {
	ContactListName      string  `json:"ContactListName"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp,omitempty"`
}

func toContactListSummaryOutput(cl *ContactList) contactListSummaryOutput {
	return contactListSummaryOutput{
		ContactListName:      cl.Name,
		LastUpdatedTimestamp: awstime.Epoch(cl.LastUpdatedAt),
	}
}

// ---- contact ----

// topicPreferenceOutput mirrors types.TopicPreference.
type topicPreferenceOutput struct {
	TopicName          string `json:"TopicName"`
	SubscriptionStatus string `json:"SubscriptionStatus"`
}

func toTopicPreferenceOutputs(prefs []TopicPreference) []topicPreferenceOutput {
	if len(prefs) == 0 {
		return nil
	}

	out := make([]topicPreferenceOutput, len(prefs))
	for i, p := range prefs {
		out[i] = topicPreferenceOutput(p)
	}

	return out
}

// contactOutput mirrors GetContactOutput's top-level fields.
type contactOutput struct {
	ContactListName      string                  `json:"ContactListName"`
	EmailAddress         string                  `json:"EmailAddress"`
	TopicPreferences     []topicPreferenceOutput `json:"TopicPreferences,omitempty"`
	CreatedTimestamp     float64                 `json:"CreatedTimestamp,omitempty"`
	LastUpdatedTimestamp float64                 `json:"LastUpdatedTimestamp,omitempty"`
	UnsubscribeAll       bool                    `json:"UnsubscribeAll"`
}

func toContactOutput(c *Contact) *contactOutput {
	return &contactOutput{
		ContactListName:      c.ContactListName,
		EmailAddress:         c.EmailAddress,
		CreatedTimestamp:     awstime.Epoch(c.CreatedAt),
		LastUpdatedTimestamp: awstime.Epoch(c.LastUpdatedAt),
		TopicPreferences:     toTopicPreferenceOutputs(c.TopicPreferences),
		UnsubscribeAll:       c.UnsubscribeAll,
	}
}

// contactSummaryOutput mirrors types.Contact, the ListContacts item shape --
// notably it has neither ContactListName nor CreatedTimestamp.
type contactSummaryOutput struct {
	EmailAddress         string                  `json:"EmailAddress"`
	TopicPreferences     []topicPreferenceOutput `json:"TopicPreferences,omitempty"`
	LastUpdatedTimestamp float64                 `json:"LastUpdatedTimestamp,omitempty"`
	UnsubscribeAll       bool                    `json:"UnsubscribeAll"`
}

func toContactSummaryOutput(c *Contact) contactSummaryOutput {
	return contactSummaryOutput{
		EmailAddress:         c.EmailAddress,
		LastUpdatedTimestamp: awstime.Epoch(c.LastUpdatedAt),
		TopicPreferences:     toTopicPreferenceOutputs(c.TopicPreferences),
		UnsubscribeAll:       c.UnsubscribeAll,
	}
}

// ---- suppressed destination ----

// suppressedDestinationOutput mirrors types.SuppressedDestination, used both
// for the GetSuppressedDestination wrapper payload and (since
// SuppressedDestinationSummary has the same three fields) for
// ListSuppressedDestinations items.
type suppressedDestinationOutput struct {
	EmailAddress   string  `json:"EmailAddress"`
	Reason         string  `json:"Reason"`
	LastUpdateTime float64 `json:"LastUpdateTime,omitempty"`
}

func toSuppressedDestinationOutput(d *SuppressedDestination) suppressedDestinationOutput {
	return suppressedDestinationOutput{
		EmailAddress:   d.EmailAddress,
		Reason:         d.Reason,
		LastUpdateTime: awstime.Epoch(d.LastUpdateTime),
	}
}

// ---- dedicated IP pool ----

// dedicatedIPPoolOutput mirrors types.DedicatedIpPool.
type dedicatedIPPoolOutput struct {
	PoolName    string `json:"PoolName"`
	ScalingMode string `json:"ScalingMode"`
}

func toDedicatedIPPoolOutput(p *DedicatedIPPool) dedicatedIPPoolOutput {
	return dedicatedIPPoolOutput{PoolName: p.PoolName, ScalingMode: p.ScalingMode}
}

// ---- configuration set event destination ----

// eventDestinationOutput mirrors types.EventDestination's modelled subset
// (Name, Enabled, MatchingEventTypes). Note the real type has no
// ConfigurationSetName or creation-time field.
type eventDestinationOutput struct {
	Name               string   `json:"Name"`
	MatchingEventTypes []string `json:"MatchingEventTypes"`
	Enabled            bool     `json:"Enabled"`
}

func toEventDestinationOutput(d *EventDestination) eventDestinationOutput {
	return eventDestinationOutput{Name: d.Name, Enabled: d.Enabled, MatchingEventTypes: d.MatchingEventTypes}
}

func toEventDestinationOutputs(dests []*EventDestination) []eventDestinationOutput {
	out := make([]eventDestinationOutput, 0, len(dests))
	for _, d := range dests {
		out = append(out, toEventDestinationOutput(d))
	}

	return out
}

// ---- email template ----

// emailTemplateContentOutput mirrors types.EmailTemplateContent -- note the
// HTML field name is "Html", not "HTML".
type emailTemplateContentOutput struct {
	Subject string `json:"Subject,omitempty"`
	HTML    string `json:"Html,omitempty"`
	Text    string `json:"Text,omitempty"`
}

func toEmailTemplateContentOutput(c *EmailTemplateContent) *emailTemplateContentOutput {
	if c == nil {
		return nil
	}

	return &emailTemplateContentOutput{Subject: c.Subject, HTML: c.HTML, Text: c.Text}
}

// emailTemplateOutput mirrors GetEmailTemplateOutput's top-level fields
// (flat -- no wrapper, and no creation timestamp).
type emailTemplateOutput struct {
	TemplateContent *emailTemplateContentOutput `json:"TemplateContent,omitempty"`
	TemplateName    string                      `json:"TemplateName"`
	Tags            []tagEntry                  `json:"Tags,omitempty"`
}

func toEmailTemplateOutput(t *EmailTemplate) *emailTemplateOutput {
	return &emailTemplateOutput{
		TemplateName:    t.TemplateName,
		TemplateContent: toEmailTemplateContentOutput(t.TemplateContent),
		Tags:            tagsToEntries(t.Tags),
	}
}

// emailTemplateMetadataOutput mirrors types.EmailTemplateMetadata, the
// ListEmailTemplates item shape (name + creation time only, no content).
type emailTemplateMetadataOutput struct {
	TemplateName     string  `json:"TemplateName"`
	CreatedTimestamp float64 `json:"CreatedTimestamp,omitempty"`
}

func toEmailTemplateMetadataOutput(t *EmailTemplate) emailTemplateMetadataOutput {
	return emailTemplateMetadataOutput{
		TemplateName:     t.TemplateName,
		CreatedTimestamp: awstime.Epoch(t.CreatedAt),
	}
}

// ---- custom verification email template ----

// customVerificationEmailTemplateOutput mirrors
// GetCustomVerificationEmailTemplateOutput's top-level fields.
type customVerificationEmailTemplateOutput struct {
	TemplateName          string `json:"TemplateName"`
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent,omitempty"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

func toCustomVerificationEmailTemplateOutput(
	t *CustomVerificationEmailTemplate,
) *customVerificationEmailTemplateOutput {
	return &customVerificationEmailTemplateOutput{
		TemplateName:          t.TemplateName,
		FromEmailAddress:      t.FromEmailAddress,
		TemplateSubject:       t.TemplateSubject,
		TemplateContent:       t.TemplateContent,
		SuccessRedirectionURL: t.SuccessRedirectionURL,
		FailureRedirectionURL: t.FailureRedirectionURL,
	}
}

// customVerificationEmailTemplateMetadataOutput mirrors
// types.CustomVerificationEmailTemplateMetadata, the
// ListCustomVerificationEmailTemplates item shape (no TemplateContent).
type customVerificationEmailTemplateMetadataOutput struct {
	TemplateName          string `json:"TemplateName"`
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

func toCustomVerificationEmailTemplateMetadataOutput(
	t *CustomVerificationEmailTemplate,
) customVerificationEmailTemplateMetadataOutput {
	return customVerificationEmailTemplateMetadataOutput{
		TemplateName:          t.TemplateName,
		FromEmailAddress:      t.FromEmailAddress,
		TemplateSubject:       t.TemplateSubject,
		SuccessRedirectionURL: t.SuccessRedirectionURL,
		FailureRedirectionURL: t.FailureRedirectionURL,
	}
}

// ---- deliverability test report ----

// deliverabilityTestReportItemOutput mirrors types.DeliverabilityTestReport,
// used both nested inside the GetDeliverabilityTestReport wrapper and as the
// ListDeliverabilityTestReports item shape.
type deliverabilityTestReportItemOutput struct {
	ReportID                 string  `json:"ReportId"`
	ReportName               string  `json:"ReportName,omitempty"`
	FromEmailAddress         string  `json:"FromEmailAddress"`
	DeliverabilityTestStatus string  `json:"DeliverabilityTestStatus"`
	CreateDate               float64 `json:"CreateDate,omitempty"`
}

func toDeliverabilityTestReportItemOutput(r *DeliverabilityTestReport) deliverabilityTestReportItemOutput {
	return deliverabilityTestReportItemOutput{
		ReportID:                 r.ReportID,
		ReportName:               r.ReportName,
		FromEmailAddress:         r.FromEmailAddress,
		DeliverabilityTestStatus: r.DeliverabilityTestStatus,
		CreateDate:               awstime.Epoch(r.CreateDate),
	}
}

// ---- export / import jobs ----

// exportJobOutput mirrors both GetExportJobOutput's top-level fields and
// types.ExportJobSummary, the ListExportJobs item shape (both reduce to
// JobId/JobStatus/CreatedTimestamp for the fields gopherstack models).
type exportJobOutput struct {
	JobID            string  `json:"JobId"`
	JobStatus        string  `json:"JobStatus"`
	CreatedTimestamp float64 `json:"CreatedTimestamp,omitempty"`
}

func toExportJobOutput(j *ExportJob) *exportJobOutput {
	return &exportJobOutput{JobID: j.JobID, JobStatus: j.JobStatus, CreatedTimestamp: awstime.Epoch(j.CreatedAt)}
}

// importJobOutput mirrors both GetImportJobOutput's top-level fields and
// types.ImportJobSummary, the ListImportJobs item shape.
type importJobOutput struct {
	JobID            string  `json:"JobId"`
	JobStatus        string  `json:"JobStatus"`
	CreatedTimestamp float64 `json:"CreatedTimestamp,omitempty"`
}

func toImportJobOutput(j *ImportJob) *importJobOutput {
	return &importJobOutput{JobID: j.JobID, JobStatus: j.JobStatus, CreatedTimestamp: awstime.Epoch(j.CreatedAt)}
}

// ---- account ----
//
// GetAccount previously marshalled the internal *AccountDetails struct
// directly, which (like the other families documented above) uses
// lowerCamelCase JSON tags for the on-disk snapshot format -- not the real
// AWS response shape, which is a top-level object with a nested "Details"
// sub-object (types.AccountDetails), a nested "SuppressionAttributes"
// sub-object (types.SuppressionAttributes, not a bare array), and a nested
// "PricingAttributes" sub-object (types.PricingAttributes). Field-diffed
// against GetAccountOutput/types.AccountDetails/types.SuppressionAttributes/
// types.PricingAttributes (aws-sdk-go-v2/service/sesv2 v1.66.0). Fields real
// SES v2 documents that gopherstack has no data source for --
// EnforcementStatus, ProductionAccessEnabled, SendQuota -- are omitted
// (they're all pointer/optional in the real shape) rather than fabricated;
// see PARITY.md.

// accountDetailsOutput mirrors types.AccountDetails (the nested "Details"
// object). AdditionalContactEmailAddresses and ReviewDetails are always
// omitted: gopherstack doesn't model the account-review workflow.
type accountDetailsOutput struct {
	MailType           string `json:"MailType,omitempty"`
	WebsiteURL         string `json:"WebsiteURL,omitempty"`
	ContactLanguage    string `json:"ContactLanguage,omitempty"`
	UseCaseDescription string `json:"UseCaseDescription,omitempty"`
}

// accountSuppressionAttributesOutput mirrors types.SuppressionAttributes.
// ValidationAttributes is always omitted: gopherstack has no destination
// suppression-validation feature to report on.
type accountSuppressionAttributesOutput struct {
	SuppressedReasons []string `json:"SuppressedReasons,omitempty"`
}

// accountPricingAttributesOutput mirrors types.PricingAttributes. NextPlan is
// always empty: gopherstack has no billing-cycle concept, so
// PutAccountPricingAttributes takes effect immediately as CurrentPlan and
// there is never a "scheduled" next plan to report.
type accountPricingAttributesOutput struct {
	CurrentPlan string `json:"CurrentPlan,omitempty"`
	NextPlan    string `json:"NextPlan,omitempty"`
}

// accountOutput mirrors GetAccountOutput's top-level fields.
type accountOutput struct {
	Details                      *accountDetailsOutput               `json:"Details,omitempty"`
	SuppressionAttributes        *accountSuppressionAttributesOutput `json:"SuppressionAttributes,omitempty"`
	PricingAttributes            *accountPricingAttributesOutput     `json:"PricingAttributes,omitempty"`
	VdmAttributes                map[string]any                      `json:"VdmAttributes,omitempty"`
	SendingEnabled               bool                                `json:"SendingEnabled"`
	DedicatedIPAutoWarmupEnabled bool                                `json:"DedicatedIpAutoWarmupEnabled"`
}

// toAccountOutput builds the AWS-shaped GetAccount response from the internal
// (lowerCamelCase, snapshot-format) AccountDetails struct.
func toAccountOutput(d *AccountDetails) *accountOutput {
	out := &accountOutput{
		SendingEnabled:               d.SendingEnabled,
		DedicatedIPAutoWarmupEnabled: d.AutoWarmupEnabled,
		VdmAttributes:                d.VdmAttributes,
	}

	if d.MailType != "" || d.WebsiteURL != "" || d.ContactLanguage != "" || d.UseCaseName != "" {
		out.Details = &accountDetailsOutput{
			MailType:           d.MailType,
			WebsiteURL:         d.WebsiteURL,
			ContactLanguage:    d.ContactLanguage,
			UseCaseDescription: d.UseCaseName,
		}
	}

	if d.SuppressionAttributes != nil {
		out.SuppressionAttributes = &accountSuppressionAttributesOutput{
			SuppressedReasons: d.SuppressionAttributes,
		}
	}

	if d.PricingPlan != "" {
		out.PricingAttributes = &accountPricingAttributesOutput{CurrentPlan: d.PricingPlan}
	}

	return out
}

// ---- tenant ----
//
// tenants.go's backend maps (b.tenants: map[string]map[string]any) are
// intentionally both the persisted snapshot format AND the wire-shaped
// response (see the "Traps for the next auditor" note in PARITY.md) -- they
// predate this file's typed-DTO convention and changing their storage shape
// would require bumping sesv2SnapshotVersion for no wire-format benefit. The
// types below add a typed conversion step at the response boundary only
// (mapString/mapFloat64/mapTagEntries read the already-correct map values),
// so a future field-name typo in tenants.go fails to compile instead of
// silently producing a wrong JSON key, without touching persistence.

// mapString/mapFloat64/mapTagEntries safely extract a typed value from one
// of the ad-hoc PascalCase-keyed response maps built by tenants.go/
// multi_region_endpoints.go, defaulting to the zero value if absent or of
// an unexpected type.
func mapString(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func mapFloat64(m map[string]any, key string) float64 {
	f, _ := m[key].(float64)

	return f
}

func mapTagEntries(m map[string]any, key string) []tagEntry {
	entries, _ := m[key].([]tagEntry)

	return entries
}

// tenantSuppressionAttributesOutput mirrors types.TenantSuppressionAttributes.
type tenantSuppressionAttributesOutput struct {
	SuppressionScope  string   `json:"SuppressionScope,omitempty"`
	SuppressedReasons []string `json:"SuppressedReasons,omitempty"`
}

// toTenantSuppressionAttributesOutput reads the SuppressedReasons/
// SuppressionScope keys PutTenantSuppressionAttributes (tenants.go) writes
// onto the tenant map, returning nil (so the field is omitted, matching the
// real API's pointer-optional shape) if PutTenantSuppressionAttributes was
// never called for this tenant.
func toTenantSuppressionAttributesOutput(t map[string]any) *tenantSuppressionAttributesOutput {
	reasons, hasReasons := t[keySuppressedReasons].([]string)
	scope, hasScope := t[keySuppressionScope].(string)

	if !hasReasons && !hasScope {
		return nil
	}

	return &tenantSuppressionAttributesOutput{SuppressedReasons: reasons, SuppressionScope: scope}
}

// tenantOutput mirrors types.Tenant (CreateTenantOutput/GetTenantOutput.Tenant).
type tenantOutput struct {
	TenantName            string                             `json:"TenantName"`
	TenantID              string                             `json:"TenantId,omitempty"`
	TenantARN             string                             `json:"TenantArn,omitempty"`
	SendingStatus         string                             `json:"SendingStatus,omitempty"`
	SuppressionAttributes *tenantSuppressionAttributesOutput `json:"SuppressionAttributes,omitempty"`
	Tags                  []tagEntry                         `json:"Tags,omitempty"`
	CreatedTimestamp      float64                            `json:"CreatedTimestamp,omitempty"`
}

func toTenantOutput(t map[string]any) *tenantOutput {
	return &tenantOutput{
		TenantName:            mapString(t, keyTenantName),
		TenantID:              mapString(t, keyTenantID),
		TenantARN:             mapString(t, keyTenantARN),
		SendingStatus:         mapString(t, keySendingStatus),
		CreatedTimestamp:      mapFloat64(t, keyCreatedTimestamp),
		Tags:                  mapTagEntries(t, keyTags),
		SuppressionAttributes: toTenantSuppressionAttributesOutput(t),
	}
}

// tenantInfoOutput mirrors types.TenantInfo, the ListTenants item shape (no
// SendingStatus/Tags).
type tenantInfoOutput struct {
	TenantName       string  `json:"TenantName"`
	TenantID         string  `json:"TenantId,omitempty"`
	TenantARN        string  `json:"TenantArn,omitempty"`
	CreatedTimestamp float64 `json:"CreatedTimestamp,omitempty"`
}

func toTenantInfoOutput(t map[string]any) tenantInfoOutput {
	return tenantInfoOutput{
		TenantName:       mapString(t, keyTenantName),
		TenantID:         mapString(t, keyTenantID),
		TenantARN:        mapString(t, keyTenantARN),
		CreatedTimestamp: mapFloat64(t, keyCreatedTimestamp),
	}
}

// resourceTenantOutput mirrors types.ResourceTenantMetadata, the
// ListResourceTenants item shape.
type resourceTenantOutput struct {
	TenantName          string  `json:"TenantName"`
	TenantID            string  `json:"TenantId,omitempty"`
	ResourceARN         string  `json:"ResourceArn"`
	AssociatedTimestamp float64 `json:"AssociatedTimestamp,omitempty"`
}

func toResourceTenantOutput(t map[string]any) resourceTenantOutput {
	return resourceTenantOutput{
		TenantName:          mapString(t, keyTenantName),
		TenantID:            mapString(t, keyTenantID),
		ResourceARN:         mapString(t, keyResourceArn),
		AssociatedTimestamp: mapFloat64(t, keyAssociatedTimestamp),
	}
}

// tenantResourceOutput mirrors types.TenantResource, the ListTenantResources
// item shape.
type tenantResourceOutput struct {
	ResourceARN  string `json:"ResourceArn"`
	ResourceType string `json:"ResourceType,omitempty"`
}

func toTenantResourceOutput(t map[string]any) tenantResourceOutput {
	return tenantResourceOutput{
		ResourceARN:  mapString(t, keyResourceArn),
		ResourceType: mapString(t, keyResourceType),
	}
}

// ---- multi-region endpoint ----

// multiRegionEndpointRouteOutput mirrors types.Route.
type multiRegionEndpointRouteOutput struct {
	Region string `json:"Region"`
}

// createMultiRegionEndpointOutput mirrors CreateMultiRegionEndpointOutput
// (EndpointId/Status only).
type createMultiRegionEndpointOutput struct {
	EndpointID string `json:"EndpointId,omitempty"`
	Status     string `json:"Status,omitempty"`
}

// multiRegionEndpointOutput mirrors GetMultiRegionEndpointOutput's top-level fields.
type multiRegionEndpointOutput struct {
	EndpointID           string                           `json:"EndpointId,omitempty"`
	EndpointName         string                           `json:"EndpointName,omitempty"`
	Status               string                           `json:"Status,omitempty"`
	Routes               []multiRegionEndpointRouteOutput `json:"Routes,omitempty"`
	CreatedTimestamp     float64                          `json:"CreatedTimestamp,omitempty"`
	LastUpdatedTimestamp float64                          `json:"LastUpdatedTimestamp,omitempty"`
}

func toMultiRegionEndpointOutput(ep map[string]any) *multiRegionEndpointOutput {
	regions, _ := ep["Regions"].([]string)
	routes := make([]multiRegionEndpointRouteOutput, 0, len(regions))

	for _, r := range regions {
		routes = append(routes, multiRegionEndpointRouteOutput{Region: r})
	}

	return &multiRegionEndpointOutput{
		EndpointID:           mapString(ep, keyEndpointID),
		EndpointName:         mapString(ep, "EndpointName"),
		Status:               mapString(ep, keyStatus),
		Routes:               routes,
		CreatedTimestamp:     mapFloat64(ep, keyCreatedTimestamp),
		LastUpdatedTimestamp: mapFloat64(ep, "LastUpdatedTimestamp"),
	}
}

// multiRegionEndpointSummaryOutput mirrors types.MultiRegionEndpoint, the
// ListMultiRegionEndpoints item shape (Regions instead of Routes).
type multiRegionEndpointSummaryOutput struct {
	EndpointID           string   `json:"EndpointId,omitempty"`
	EndpointName         string   `json:"EndpointName,omitempty"`
	Status               string   `json:"Status,omitempty"`
	Regions              []string `json:"Regions,omitempty"`
	CreatedTimestamp     float64  `json:"CreatedTimestamp,omitempty"`
	LastUpdatedTimestamp float64  `json:"LastUpdatedTimestamp,omitempty"`
}

func toMultiRegionEndpointSummaryOutput(ep map[string]any) multiRegionEndpointSummaryOutput {
	regions, _ := ep["Regions"].([]string)

	return multiRegionEndpointSummaryOutput{
		EndpointID:           mapString(ep, keyEndpointID),
		EndpointName:         mapString(ep, "EndpointName"),
		Status:               mapString(ep, keyStatus),
		Regions:              regions,
		CreatedTimestamp:     mapFloat64(ep, keyCreatedTimestamp),
		LastUpdatedTimestamp: mapFloat64(ep, "LastUpdatedTimestamp"),
	}
}

// ---- reputation entity ----

// statusRecordOutput mirrors types.StatusRecord. gopherstack only tracks
// Status (the only field UpdateReputationEntityCustomerManagedStatus lets a
// caller set) -- Cause/LastUpdatedTimestamp are always omitted.
type statusRecordOutput struct {
	Status               string  `json:"Status,omitempty"`
	Cause                string  `json:"Cause,omitempty"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp,omitempty"`
}

// reputationEntityOutput mirrors types.ReputationEntity. AwsSesManagedStatus
// is omitted: it's SES's own computed reputation-findings status, which
// gopherstack has no findings engine to derive (see ListRecommendations).
type reputationEntityOutput struct {
	ReputationEntityReference  string              `json:"ReputationEntityReference,omitempty"`
	ReputationEntityType       string              `json:"ReputationEntityType,omitempty"`
	CustomerManagedStatus      *statusRecordOutput `json:"CustomerManagedStatus,omitempty"`
	ReputationManagementPolicy string              `json:"ReputationManagementPolicy,omitempty"`
	SendingStatusAggregate     string              `json:"SendingStatusAggregate,omitempty"`
}

// toReputationEntityOutput renders a reputation entity as the AWS-shaped
// response, field-diffed against types.ReputationEntity.
// SendingStatusAggregate is derived from CustomerManagedStatus (gopherstack
// has no separate AWS-SES-managed status to combine it with).
func toReputationEntityOutput(e *ReputationEntity) reputationEntityOutput {
	out := reputationEntityOutput{
		ReputationEntityReference: e.EntityRef,
		ReputationEntityType:      e.EntityType,
	}

	if e.CustomerManagedStatus != "" {
		out.CustomerManagedStatus = &statusRecordOutput{Status: e.CustomerManagedStatus}
	}

	out.ReputationManagementPolicy = e.ReputationPolicy

	aggregate := sendingStatusEnabled
	if e.CustomerManagedStatus != "" {
		aggregate = e.CustomerManagedStatus
	}

	out.SendingStatusAggregate = aggregate

	return out
}

// ---- bulk email result ----

// bulkEmailEntryResultOutput mirrors types.BulkEmailEntryResult, the
// SendBulkEmail per-entry result shape.
type bulkEmailEntryResultOutput struct {
	MessageID string `json:"MessageId,omitempty"`
	Status    string `json:"Status,omitempty"`
	Error     string `json:"Error,omitempty"`
}

// ---- recommendation ----

// recommendationOutput mirrors types.Recommendation; see ListRecommendations
// (deliverability.go) for which Type values gopherstack can derive for real.
type recommendationOutput struct {
	ResourceArn          string  `json:"ResourceArn,omitempty"`
	Type                 string  `json:"Type,omitempty"`
	Status               string  `json:"Status,omitempty"`
	Impact               string  `json:"Impact,omitempty"`
	Description          string  `json:"Description,omitempty"`
	CreatedTimestamp     float64 `json:"CreatedTimestamp,omitempty"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp,omitempty"`
}
