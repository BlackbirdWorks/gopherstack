package connect

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

type (
	empty struct{}

	// instPath binds the InstanceId path label.
	instPath struct {
		InstanceID string `json:"-" path:"InstanceId"`
	}

	idArnOut struct {
		Arn string `json:"HoursOfOperationArn"`
		ID  string `json:"HoursOfOperationId"`
	}

	queueOut struct {
		QueueArn string `json:"QueueArn"`
		QueueID  string `json:"QueueId"`
	}

	quickConnectOut struct {
		QuickConnectARN string `json:"QuickConnectARN"`
		QuickConnectID  string `json:"QuickConnectId"`
	}

	routingProfileOut struct {
		RoutingProfileArn string `json:"RoutingProfileArn"`
		RoutingProfileID  string `json:"RoutingProfileId"`
	}

	securityProfileOut struct {
		SecurityProfileArn string `json:"SecurityProfileArn"`
		SecurityProfileID  string `json:"SecurityProfileId"`
	}

	hierarchyGroupOut struct {
		HierarchyGroupArn string `json:"HierarchyGroupArn"`
		HierarchyGroupID  string `json:"HierarchyGroupId"`
	}

	userOut struct {
		UserArn string `json:"UserArn"`
		UserID  string `json:"UserId"`
	}

	agentStatusOut struct {
		AgentStatusARN string `json:"AgentStatusARN"`
		AgentStatusID  string `json:"AgentStatusId"`
	}

	promptOut struct {
		PromptARN string `json:"PromptARN"`
		PromptID  string `json:"PromptId"`
	}

	contactFlowOut struct {
		ContactFlowArn    string `json:"ContactFlowArn"`
		ContactFlowID     string `json:"ContactFlowId"`
		FlowContentSha256 string `json:"FlowContentSha256"`
	}

	moduleOut struct {
		Arn string `json:"Arn"`
		ID  string `json:"Id"`
	}

	phoneOut struct {
		PhoneNumberArn string `json:"PhoneNumberArn"`
		PhoneNumberID  string `json:"PhoneNumberId"`
	}

	searchNumbersOut struct {
		NextToken            string                   `json:"NextToken,omitempty"`
		AvailableNumbersList []AvailableNumberSummary `json:"AvailableNumbersList"`
	}

	listPhonesOut struct {
		NextToken                   string                    `json:"NextToken,omitempty"`
		ListPhoneNumbersSummaryList []ListPhoneNumbersSummary `json:"ListPhoneNumbersSummaryList"`
	}

	integrationOut struct {
		IntegrationAssociationArn string `json:"IntegrationAssociationArn"`
		IntegrationAssociationID  string `json:"IntegrationAssociationId"`
	}

	useCaseOut struct {
		UseCaseArn string `json:"UseCaseArn"`
		UseCaseID  string `json:"UseCaseId"`
	}

	contactIDOut struct {
		ContactID string `json:"ContactId"`
	}

	chatOut struct {
		ContactID              string `json:"ContactId"`
		ParticipantID          string `json:"ParticipantId"`
		ParticipantToken       string `json:"ParticipantToken"`
		ContinuedFromContactID string `json:"ContinuedFromContactId,omitempty"`
	}

	attributesOut struct {
		Attributes map[string]string `json:"Attributes"`
	}

	searchContactsOut struct {
		NextToken  string                 `json:"NextToken,omitempty"`
		Contacts   []ContactSearchSummary `json:"Contacts"`
		TotalCount int64                  `json:"TotalCount"`
	}

	currentMetricsOut struct {
		DataSnapshotTime      awstime.EpochTime     `json:"DataSnapshotTime"`
		NextToken             string                `json:"NextToken,omitempty"`
		MetricResults         []CurrentMetricResult `json:"MetricResults"`
		ApproximateTotalCount int64                 `json:"ApproximateTotalCount"`
	}

	historicalMetricsOut struct {
		NextToken     string                   `json:"NextToken,omitempty"`
		MetricResults []HistoricalMetricResult `json:"MetricResults"`
	}

	userDataOut struct {
		NextToken             string     `json:"NextToken,omitempty"`
		UserDataList          []UserData `json:"UserDataList"`
		ApproximateTotalCount int64      `json:"ApproximateTotalCount"`
	}
)
