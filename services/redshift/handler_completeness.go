package redshift

import (
	"encoding/xml"
	"net/url"
)

const (
	// endpointStatusActive is the status for an active Redshift endpoint access.
	endpointStatusActive = "active"
	// redshiftDefaultPort is the default Redshift cluster port.
	redshiftDefaultPort = 5439
	// minClusterNodes is the minimum number of nodes for a node config option stub.
	minClusterNodes = 2
)

// buildOpsCompleteness returns stub dispatch entries for all previously
// notImplemented Redshift operations.
func (h *Handler) buildOpsCompleteness() map[string]redshiftActionFn {
	return map[string]redshiftActionFn{
		"CreateCustomDomainAssociation":    h.handleCreateCustomDomainAssociation,
		"CreateEndpointAccess":             h.handleCreateEndpointAccess,
		"CreateHsmClientCertificate":       h.handleCreateHsmClientCertificate,
		"CreateHsmConfiguration":           h.handleCreateHsmConfiguration,
		"CreateIntegration":                h.handleCreateIntegration,
		"CreateRedshiftIdcApplication":     h.handleCreateRedshiftIdcApplication,
		opCreateScheduledAction:            h.handleCreateScheduledAction,
		"DeleteCustomDomainAssociation":    h.handleDeleteCustomDomainAssociation,
		"DeleteEndpointAccess":             h.handleDeleteEndpointAccess,
		"DeleteHsmClientCertificate":       h.handleDeleteHsmClientCertificate,
		"DeleteHsmConfiguration":           h.handleDeleteHsmConfiguration,
		"DeleteIntegration":                h.handleDeleteIntegration,
		"DeleteRedshiftIdcApplication":     h.handleDeleteRedshiftIdcApplication,
		opDeleteScheduledAction:            h.handleDeleteScheduledAction,
		"DeregisterNamespace":              h.handleDeregisterNamespace,
		"DescribeClusterDbRevisions":       h.handleDescribeClusterDBRevisions,
		"DescribeCustomDomainAssociations": h.handleDescribeCustomDomainAssociations,
		"DescribeEndpointAccess":           h.handleDescribeEndpointAccess,
		"DescribeHsmClientCertificates":    h.handleDescribeHsmClientCertificates,
		"DescribeHsmConfigurations":        h.handleDescribeHsmConfigurations,
		"DescribeInboundIntegrations":      h.handleDescribeInboundIntegrations,
		"DescribeIntegrations":             h.handleDescribeIntegrations,
		"DescribeNodeConfigurationOptions": h.handleDescribeNodeConfigurationOptions,
		"DescribeRedshiftIdcApplications":  h.handleDescribeRedshiftIdcApplications,
		"DescribeScheduledActions":         h.handleDescribeScheduledActions,
		"GetIdentityCenterAuthToken":       h.handleGetIdentityCenterAuthToken,
		"ListRecommendations":              h.handleListRecommendations,
		"ModifyAquaConfiguration":          h.handleModifyAquaConfiguration,
		"ModifyClusterDbRevision":          h.handleModifyClusterDBRevision,
		"ModifyCustomDomainAssociation":    h.handleModifyCustomDomainAssociation,
		"ModifyEndpointAccess":             h.handleModifyEndpointAccess,
		"ModifyIntegration":                h.handleModifyIntegration,
		"ModifyLakehouseConfiguration":     h.handleModifyLakehouseConfiguration,
		"ModifyRedshiftIdcApplication":     h.handleModifyRedshiftIdcApplication,
		"ModifyScheduledAction":            h.handleModifyScheduledAction,
		"RegisterNamespace":                h.handleRegisterNamespace,
		"RestoreTableFromClusterSnapshot":  h.handleRestoreTableFromClusterSnapshot,
	}
}

// ----- Custom Domain Association -----

type createCustomDomainAssociationResult struct {
	CustomDomainName           string `xml:"CustomDomainName"`
	CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
	ClusterIdentifier          string `xml:"ClusterIdentifier"`
}

type createCustomDomainAssociationResponse struct {
	XMLName xml.Name                            `xml:"CreateCustomDomainAssociationResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  createCustomDomainAssociationResult `xml:"CreateCustomDomainAssociationResult"`
}

func (h *Handler) handleCreateCustomDomainAssociation(vals url.Values) (any, error) {
	assoc, err := h.Backend.CreateCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
		vals.Get("CustomDomainCertificateArn"),
	)
	if err != nil {
		return nil, err
	}

	return &createCustomDomainAssociationResponse{
		Xmlns: redshiftXMLNS,
		Result: createCustomDomainAssociationResult{
			ClusterIdentifier:          assoc.ClusterIdentifier,
			CustomDomainName:           assoc.CustomDomainName,
			CustomDomainCertificateArn: assoc.CustomDomainCertificateArn,
		},
	}, nil
}

type deleteCustomDomainAssociationResponse struct {
	XMLName xml.Name `xml:"DeleteCustomDomainAssociationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteCustomDomainAssociation(vals url.Values) (any, error) {
	if err := h.Backend.DeleteCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
	); err != nil {
		return nil, err
	}

	return &deleteCustomDomainAssociationResponse{Xmlns: redshiftXMLNS}, nil
}

type customDomainAssociation struct {
	ClusterIdentifier          string `xml:"ClusterIdentifier"`
	CustomDomainName           string `xml:"CustomDomainName"`
	CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
}

type describeCustomDomainAssociationsResponse struct {
	XMLName xml.Name `xml:"DescribeCustomDomainAssociationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		CustomDomainAssociations []customDomainAssociation `xml:"CustomDomainAssociations>CustomDomainAssociation"`
	} `xml:"DescribeCustomDomainAssociationsResult"`
}

func (h *Handler) handleDescribeCustomDomainAssociations(vals url.Values) (any, error) {
	assocs, err := h.Backend.DescribeCustomDomainAssociations(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]customDomainAssociation, 0, len(assocs))

	for _, a := range assocs {
		members = append(members, customDomainAssociation{
			ClusterIdentifier:          a.ClusterIdentifier,
			CustomDomainName:           a.CustomDomainName,
			CustomDomainCertificateArn: a.CustomDomainCertificateArn,
		})
	}

	resp := &describeCustomDomainAssociationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.CustomDomainAssociations = members

	return resp, nil
}

type modifyCustomDomainAssociationResponse struct {
	XMLName xml.Name `xml:"ModifyCustomDomainAssociationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ClusterIdentifier          string `xml:"ClusterIdentifier"`
		CustomDomainName           string `xml:"CustomDomainName"`
		CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
	} `xml:"ModifyCustomDomainAssociationResult"`
}

func (h *Handler) handleModifyCustomDomainAssociation(vals url.Values) (any, error) {
	assoc, err := h.Backend.ModifyCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
		vals.Get("CustomDomainCertificateArn"),
	)
	if err != nil {
		return nil, err
	}

	resp := &modifyCustomDomainAssociationResponse{Xmlns: redshiftXMLNS}
	resp.Result.ClusterIdentifier = assoc.ClusterIdentifier
	resp.Result.CustomDomainName = assoc.CustomDomainName
	resp.Result.CustomDomainCertificateArn = assoc.CustomDomainCertificateArn

	return resp, nil
}

// ----- Endpoint Access -----

type endpointAccessXML struct {
	ClusterIdentifier  string `xml:"ClusterIdentifier"`
	EndpointName       string `xml:"EndpointName"`
	EndpointStatus     string `xml:"EndpointStatus"`
	EndpointCreateTime string `xml:"EndpointCreateTime,omitempty"`
	Port               int    `xml:"Port,omitempty"`
}

type createEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"CreateEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"CreateEndpointAccessResult"`
}

func endpointAccessToXML(ep *EndpointAccess) endpointAccessXML {
	return endpointAccessXML{
		ClusterIdentifier:  ep.ClusterIdentifier,
		EndpointName:       ep.EndpointName,
		EndpointStatus:     ep.EndpointStatus,
		EndpointCreateTime: ep.EndpointCreateTime,
		Port:               ep.Port,
	}
}

func (h *Handler) handleCreateEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.CreateEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
		vals.Get("VpcId"),
	)
	if err != nil {
		return nil, err
	}

	return &createEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}

type deleteEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"DeleteEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"DeleteEndpointAccessResult"`
}

func (h *Handler) handleDeleteEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.DeleteEndpointAccess(vals.Get("EndpointName"))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}

type describeEndpointAccessResponse struct {
	XMLName xml.Name `xml:"DescribeEndpointAccessResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		EndpointAccessList []endpointAccessXML `xml:"EndpointAccessList>member"`
	} `xml:"DescribeEndpointAccessResult"`
}

func (h *Handler) handleDescribeEndpointAccess(vals url.Values) (any, error) {
	eps, err := h.Backend.DescribeEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]endpointAccessXML, 0, len(eps))

	for i := range eps {
		members = append(members, endpointAccessToXML(&eps[i]))
	}

	resp := &describeEndpointAccessResponse{Xmlns: redshiftXMLNS}
	resp.Result.EndpointAccessList = members

	return resp, nil
}

type modifyEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"ModifyEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"ModifyEndpointAccessResult"`
}

func (h *Handler) handleModifyEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.ModifyEndpointAccess(
		vals.Get("EndpointName"),
		vals.Get("VpcId"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}

// ----- HSM -----

type hsmClientCertificateXML struct {
	HsmClientCertificateIdentifier string `xml:"HsmClientCertificateIdentifier"`
	HsmClientCertificatePublicKey  string `xml:"HsmClientCertificatePublicKey"`
}

type createHsmClientCertificateResponse struct {
	XMLName xml.Name                `xml:"CreateHsmClientCertificateResponse"`
	Xmlns   string                  `xml:"xmlns,attr"`
	Result  hsmClientCertificateXML `xml:"CreateHsmClientCertificateResult"`
}

func (h *Handler) handleCreateHsmClientCertificate(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	cert, err := h.Backend.CreateHsmClientCertificate(id, nil)
	if err != nil {
		return nil, err
	}

	return &createHsmClientCertificateResponse{
		Xmlns: redshiftXMLNS,
		Result: hsmClientCertificateXML{
			HsmClientCertificateIdentifier: cert.HsmClientCertificateIdentifier,
			HsmClientCertificatePublicKey:  cert.HsmClientCertificatePublicKey,
		},
	}, nil
}

type deleteHsmClientCertificateResponse struct {
	XMLName xml.Name `xml:"DeleteHsmClientCertificateResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteHsmClientCertificate(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	if err := h.Backend.DeleteHsmClientCertificate(id); err != nil {
		return nil, err
	}

	return &deleteHsmClientCertificateResponse{Xmlns: redshiftXMLNS}, nil
}

type describeHsmClientCertificatesResponse struct {
	XMLName xml.Name `xml:"DescribeHsmClientCertificatesResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		HsmClientCertificates []hsmClientCertificateXML `xml:"HsmClientCertificates>HsmClientCertificate"`
	} `xml:"DescribeHsmClientCertificatesResult"`
}

func (h *Handler) handleDescribeHsmClientCertificates(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	certs, err := h.Backend.DescribeHsmClientCertificates(id)
	if err != nil {
		return nil, err
	}

	members := make([]hsmClientCertificateXML, 0, len(certs))

	for _, c := range certs {
		members = append(members, hsmClientCertificateXML{
			HsmClientCertificateIdentifier: c.HsmClientCertificateIdentifier,
			HsmClientCertificatePublicKey:  c.HsmClientCertificatePublicKey,
		})
	}

	resp := &describeHsmClientCertificatesResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmClientCertificates = members

	return resp, nil
}

type hsmConfigurationXML struct {
	HsmConfigurationIdentifier string `xml:"HsmConfigurationIdentifier"`
	Description                string `xml:"Description"`
	HsmIPAddress               string `xml:"HsmIPAddress"`
	HsmPartitionName           string `xml:"HsmPartitionName"`
}

type createHsmConfigurationResponse struct {
	XMLName xml.Name            `xml:"CreateHsmConfigurationResponse"`
	Xmlns   string              `xml:"xmlns,attr"`
	Result  hsmConfigurationXML `xml:"CreateHsmConfigurationResult"`
}

func (h *Handler) handleCreateHsmConfiguration(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	cfg, err := h.Backend.CreateHsmConfiguration(
		id,
		vals.Get("Description"),
		vals.Get("HsmIPAddress"),
		vals.Get("HsmPartitionName"),
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &createHsmConfigurationResponse{
		Xmlns: redshiftXMLNS,
		Result: hsmConfigurationXML{
			HsmConfigurationIdentifier: cfg.HsmConfigurationIdentifier,
			Description:                cfg.Description,
			HsmIPAddress:               cfg.HsmIPAddress,
			HsmPartitionName:           cfg.HsmPartitionName,
		},
	}, nil
}

type deleteHsmConfigurationResponse struct {
	XMLName xml.Name `xml:"DeleteHsmConfigurationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteHsmConfiguration(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	if err := h.Backend.DeleteHsmConfiguration(id); err != nil {
		return nil, err
	}

	return &deleteHsmConfigurationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeHsmConfigurationsResponse struct {
	XMLName xml.Name `xml:"DescribeHsmConfigurationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		HsmConfigurations []hsmConfigurationXML `xml:"HsmConfigurations>HsmConfiguration"`
	} `xml:"DescribeHsmConfigurationsResult"`
}

func (h *Handler) handleDescribeHsmConfigurations(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	cfgs, err := h.Backend.DescribeHsmConfigurations(id)
	if err != nil {
		return nil, err
	}

	members := make([]hsmConfigurationXML, 0, len(cfgs))

	for _, c := range cfgs {
		members = append(members, hsmConfigurationXML{
			HsmConfigurationIdentifier: c.HsmConfigurationIdentifier,
			Description:                c.Description,
			HsmIPAddress:               c.HsmIPAddress,
			HsmPartitionName:           c.HsmPartitionName,
		})
	}

	resp := &describeHsmConfigurationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmConfigurations = members

	return resp, nil
}

// ----- Integrations -----

type integrationXML struct {
	IntegrationArn  string `xml:"IntegrationArn"`
	IntegrationName string `xml:"IntegrationName"`
	SourceArn       string `xml:"SourceArn,omitempty"`
	TargetArn       string `xml:"TargetArn,omitempty"`
	Status          string `xml:"Status"`
	Description     string `xml:"Description,omitempty"`
	KmsKeyID        string `xml:"KmsKeyId,omitempty"`
}

func integrationToXML(ig *Integration) integrationXML {
	return integrationXML{
		IntegrationArn:  ig.IntegrationArn,
		IntegrationName: ig.IntegrationName,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          ig.Status,
		Description:     ig.Description,
		KmsKeyID:        ig.KmsKeyID,
	}
}

type createIntegrationResponse struct {
	XMLName xml.Name       `xml:"CreateIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"CreateIntegrationResult"`
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.CreateIntegration(
		vals.Get("IntegrationName"),
		vals.Get("SourceArn"),
		vals.Get("TargetArn"),
		vals.Get("KmsKeyId"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type deleteIntegrationResponse struct {
	XMLName xml.Name       `xml:"DeleteIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"DeleteIntegrationResult"`
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.DeleteIntegration(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type describeIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Integrations []integrationXML `xml:"Integrations>Integration"`
	} `xml:"DescribeIntegrationsResult"`
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	igs, err := h.Backend.DescribeIntegrations(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	members := make([]integrationXML, 0, len(igs))

	for i := range igs {
		members = append(members, integrationToXML(&igs[i]))
	}

	resp := &describeIntegrationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.Integrations = members

	return resp, nil
}

type describeInboundIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeInboundIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		InboundIntegrations []integrationXML `xml:"InboundIntegrations>InboundIntegration"`
	} `xml:"DescribeInboundIntegrationsResult"`
}

func (h *Handler) handleDescribeInboundIntegrations(_ url.Values) (any, error) {
	return &describeInboundIntegrationsResponse{Xmlns: redshiftXMLNS}, nil
}

type modifyIntegrationResponse struct {
	XMLName xml.Name       `xml:"ModifyIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"ModifyIntegrationResult"`
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.ModifyIntegration(
		vals.Get("IntegrationArn"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

// ----- Redshift IDC Application -----

type redshiftIdcAppXML struct {
	RedshiftIdcApplicationArn  string `xml:"RedshiftIdcApplicationArn"`
	RedshiftIdcApplicationName string `xml:"RedshiftIdcApplicationName"`
	IdcInstanceArn             string `xml:"IamRoleArn,omitempty"`
	IdcDisplayName             string `xml:"IdcDisplayName,omitempty"`
	IamRoleArn                 string `xml:"IdcInstanceArn,omitempty"`
}

func idcAppToXML(app *RedshiftIdcApplication) redshiftIdcAppXML {
	return redshiftIdcAppXML{
		RedshiftIdcApplicationArn:  app.RedshiftIdcApplicationArn,
		RedshiftIdcApplicationName: app.RedshiftIdcApplicationName,
		IdcInstanceArn:             app.IdcInstanceArn,
		IdcDisplayName:             app.IdcDisplayName,
		IamRoleArn:                 app.IamRoleArn,
	}
}

type createRedshiftIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"CreateRedshiftIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"CreateRedshiftIdcApplicationResult"`
}

func (h *Handler) handleCreateRedshiftIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.CreateRedshiftIdcApplication(
		vals.Get("RedshiftIdcApplicationName"),
		vals.Get("IdcInstanceArn"),
		vals.Get("IdcDisplayName"),
		vals.Get("IamRoleArn"),
	)
	if err != nil {
		return nil, err
	}

	return &createRedshiftIdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: idcAppToXML(app),
	}, nil
}

type deleteRedshiftIdcApplicationResponse struct {
	XMLName xml.Name `xml:"DeleteRedshiftIdcApplicationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteRedshiftIdcApplication(vals url.Values) (any, error) {
	if err := h.Backend.DeleteRedshiftIdcApplication(vals.Get("RedshiftIdcApplicationArn")); err != nil {
		return nil, err
	}

	return &deleteRedshiftIdcApplicationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeRedshiftIdcApplicationsResponse struct {
	XMLName xml.Name `xml:"DescribeRedshiftIdcApplicationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		RedshiftIdcApplications []redshiftIdcAppXML `xml:"RedshiftIdcApplications>RedshiftIdcApplication"`
	} `xml:"DescribeRedshiftIdcApplicationsResult"`
}

func (h *Handler) handleDescribeRedshiftIdcApplications(vals url.Values) (any, error) {
	apps, err := h.Backend.DescribeRedshiftIdcApplications(vals.Get("RedshiftIdcApplicationArn"))
	if err != nil {
		return nil, err
	}

	members := make([]redshiftIdcAppXML, 0, len(apps))

	for i := range apps {
		members = append(members, idcAppToXML(&apps[i]))
	}

	resp := &describeRedshiftIdcApplicationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.RedshiftIdcApplications = members

	return resp, nil
}

type modifyRedshiftIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"ModifyRedshiftIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"ModifyRedshiftIdcApplicationResult"`
}

func (h *Handler) handleModifyRedshiftIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.ModifyRedshiftIdcApplication(
		vals.Get("RedshiftIdcApplicationArn"),
		vals.Get("IdcDisplayName"),
		vals.Get("IamRoleArn"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyRedshiftIdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: idcAppToXML(app),
	}, nil
}

// ----- Scheduled Actions -----

type scheduledActionXML struct {
	ScheduledActionName        string `xml:"ScheduledActionName"`
	Schedule                   string `xml:"Schedule"`
	IamRole                    string `xml:"IamRole,omitempty"`
	ScheduledActionDescription string `xml:"ScheduledActionDescription,omitempty"`
	State                      string `xml:"State"`
}

type createScheduledActionResponse struct {
	XMLName xml.Name           `xml:"CreateScheduledActionResponse"`
	Xmlns   string             `xml:"xmlns,attr"`
	Result  scheduledActionXML `xml:"CreateScheduledActionResult"`
}

func scheduledActionToXML(a *ScheduledAction) scheduledActionXML {
	return scheduledActionXML{
		ScheduledActionName:        a.ScheduledActionName,
		Schedule:                   a.Schedule,
		IamRole:                    a.IamRole,
		ScheduledActionDescription: a.ScheduledActionDescription,
		State:                      a.State,
	}
}

func (h *Handler) handleCreateScheduledAction(vals url.Values) (any, error) {
	action, err := h.Backend.CreateScheduledAction(
		vals.Get("ScheduledActionName"),
		vals.Get("Schedule"),
		vals.Get("IamRole"),
		vals.Get("ScheduledActionDescription"),
		vals.Get("TargetAction"),
	)
	if err != nil {
		return nil, err
	}

	return &createScheduledActionResponse{
		Xmlns:  redshiftXMLNS,
		Result: scheduledActionToXML(action),
	}, nil
}

type deleteScheduledActionResponse struct {
	XMLName xml.Name `xml:"DeleteScheduledActionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteScheduledAction(vals url.Values) (any, error) {
	name := vals.Get("ScheduledActionName")
	if err := h.Backend.DeleteScheduledAction(name); err != nil {
		return nil, err
	}

	return &deleteScheduledActionResponse{Xmlns: redshiftXMLNS}, nil
}

type describeScheduledActionsResponse struct {
	XMLName xml.Name `xml:"DescribeScheduledActionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ScheduledActions []scheduledActionXML `xml:"ScheduledActions>ScheduledAction"`
	} `xml:"DescribeScheduledActionsResult"`
}

func (h *Handler) handleDescribeScheduledActions(vals url.Values) (any, error) {
	name := vals.Get("ScheduledActionName")
	actions, err := h.Backend.DescribeScheduledActions(name)
	if err != nil {
		return nil, err
	}

	members := make([]scheduledActionXML, 0, len(actions))

	for i := range actions {
		members = append(members, scheduledActionToXML(&actions[i]))
	}

	resp := &describeScheduledActionsResponse{Xmlns: redshiftXMLNS}
	resp.Result.ScheduledActions = members

	return resp, nil
}

type modifyScheduledActionResponse struct {
	XMLName xml.Name           `xml:"ModifyScheduledActionResponse"`
	Xmlns   string             `xml:"xmlns,attr"`
	Result  scheduledActionXML `xml:"ModifyScheduledActionResult"`
}

func (h *Handler) handleModifyScheduledAction(vals url.Values) (any, error) {
	action, err := h.Backend.ModifyScheduledAction(
		vals.Get("ScheduledActionName"),
		vals.Get("Schedule"),
		vals.Get("IamRole"),
		vals.Get("ScheduledActionDescription"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyScheduledActionResponse{
		Xmlns:  redshiftXMLNS,
		Result: scheduledActionToXML(action),
	}, nil
}

// ----- Misc stubs -----

type deregisterNamespaceResponse struct {
	XMLName xml.Name `xml:"DeregisterNamespaceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeregisterNamespace(_ url.Values) (any, error) {
	return &deregisterNamespaceResponse{Xmlns: redshiftXMLNS}, nil
}

type registerNamespaceResponse struct {
	XMLName xml.Name `xml:"RegisterNamespaceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleRegisterNamespace(_ url.Values) (any, error) {
	return &registerNamespaceResponse{Xmlns: redshiftXMLNS}, nil
}

type clusterDBRevisionXML struct {
	ClusterIdentifier       string `xml:"ClusterIdentifier"`
	CurrentDatabaseRevision string `xml:"CurrentDatabaseRevision"`
}

type describeClusterDBRevisionsResponse struct {
	XMLName xml.Name `xml:"DescribeClusterDBRevisionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ClusterDBRevisions []clusterDBRevisionXML `xml:"ClusterDBRevisions>ClusterDbRevision"`
	} `xml:"DescribeClusterDBRevisionsResult"`
}

func (h *Handler) handleDescribeClusterDBRevisions(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	resp := &describeClusterDBRevisionsResponse{Xmlns: redshiftXMLNS}

	if id != "" {
		resp.Result.ClusterDBRevisions = []clusterDBRevisionXML{
			{ClusterIdentifier: id, CurrentDatabaseRevision: "1"},
		}
	}

	return resp, nil
}

type modifyClusterDBRevisionResponse struct {
	XMLName xml.Name   `xml:"ModifyClusterDbRevisionResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Result  xmlCluster `xml:"ModifyClusterDbRevisionResult"`
}

func (h *Handler) handleModifyClusterDBRevision(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	cluster, err := h.Backend.DescribeClusters(id)
	if err != nil {
		return nil, err
	}

	if len(cluster) == 0 {
		return &modifyClusterDBRevisionResponse{Xmlns: redshiftXMLNS}, nil
	}

	return &modifyClusterDBRevisionResponse{
		Xmlns:  redshiftXMLNS,
		Result: toXMLCluster(&cluster[0]),
	}, nil
}

type nodeConfigOptionXML struct {
	NodeType                        string  `xml:"NodeType"`
	NumberOfNodes                   int     `xml:"NumberOfNodes"`
	EstimatedDiskUtilizationPercent float64 `xml:"EstimatedDiskUtilizationPercent"`
}

type describeNodeConfigurationOptionsResponse struct {
	XMLName xml.Name `xml:"DescribeNodeConfigurationOptionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		NodeConfigurationOptionList []nodeConfigOptionXML `xml:"NodeConfigurationOptionList>NodeConfigurationOption"`
	} `xml:"DescribeNodeConfigurationOptionsResult"`
}

func (h *Handler) handleDescribeNodeConfigurationOptions(_ url.Values) (any, error) {
	return &describeNodeConfigurationOptionsResponse{
		Xmlns: redshiftXMLNS,
		Result: struct {
			NodeConfigurationOptionList []nodeConfigOptionXML `xml:"NodeConfigurationOptionList>NodeConfigurationOption"`
		}{
			NodeConfigurationOptionList: []nodeConfigOptionXML{
				{NodeType: "ra3.xlplus", NumberOfNodes: minClusterNodes, EstimatedDiskUtilizationPercent: 0},
			},
		},
	}, nil
}

type identityCenterAuthTokenResponse struct {
	XMLName xml.Name `xml:"GetIdentityCenterAuthTokenResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		AuthToken           string `xml:"AuthToken"`
		AuthTokenExpiration string `xml:"AuthTokenExpiration"`
	} `xml:"GetIdentityCenterAuthTokenResult"`
}

func (h *Handler) handleGetIdentityCenterAuthToken(_ url.Values) (any, error) {
	resp := &identityCenterAuthTokenResponse{Xmlns: redshiftXMLNS}
	resp.Result.AuthToken = "stub-auth-token"
	resp.Result.AuthTokenExpiration = "2099-01-01T00:00:00Z"

	return resp, nil
}

type recommendationXML struct {
	RecommendationID  string `xml:"RecommendationId"`
	ClusterIdentifier string `xml:"ClusterIdentifier"`
	Title             string `xml:"Title"`
}

type listRecommendationsResponse struct {
	XMLName xml.Name `xml:"ListRecommendationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Recommendations []recommendationXML `xml:"Recommendations>Recommendation"`
	} `xml:"ListRecommendationsResult"`
}

func (h *Handler) handleListRecommendations(_ url.Values) (any, error) {
	return &listRecommendationsResponse{Xmlns: redshiftXMLNS}, nil
}

type aquaConfigurationResponse struct {
	XMLName xml.Name `xml:"ModifyAquaConfigurationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		AquaConfiguration struct {
			AquaConfigurationStatus string `xml:"AquaConfigurationStatus"`
			AquaStatus              string `xml:"AquaStatus"`
		} `xml:"AquaConfiguration"`
	} `xml:"ModifyAquaConfigurationResult"`
}

func (h *Handler) handleModifyAquaConfiguration(_ url.Values) (any, error) {
	resp := &aquaConfigurationResponse{Xmlns: redshiftXMLNS}
	resp.Result.AquaConfiguration.AquaConfigurationStatus = "auto"
	resp.Result.AquaConfiguration.AquaStatus = "disabled"

	return resp, nil
}

type modifyLakehouseConfigurationResponse struct {
	XMLName xml.Name `xml:"ModifyLakehouseConfigurationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleModifyLakehouseConfiguration(_ url.Values) (any, error) {
	return &modifyLakehouseConfigurationResponse{Xmlns: redshiftXMLNS}, nil
}

type restoreTableFromClusterSnapshotResponse struct {
	XMLName xml.Name `xml:"RestoreTableFromClusterSnapshotResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		TableRestoreStatus struct {
			TableRestoreRequestID string `xml:"TableRestoreRequestId"`
			Status                string `xml:"Status"`
		} `xml:"TableRestoreStatus"`
	} `xml:"RestoreTableFromClusterSnapshotResult"`
}

func (h *Handler) handleRestoreTableFromClusterSnapshot(vals url.Values) (any, error) {
	tr, err := h.Backend.CreateTableRestoreStatus(
		vals.Get("ClusterIdentifier"),
		vals.Get("SnapshotIdentifier"),
		vals.Get("SourceDatabaseName"),
		vals.Get("SourceTableName"),
		vals.Get("TargetDatabaseName"),
		vals.Get("NewTableName"),
	)
	if err != nil {
		return nil, err
	}

	resp := &restoreTableFromClusterSnapshotResponse{Xmlns: redshiftXMLNS}
	resp.Result.TableRestoreStatus.TableRestoreRequestID = tr.TableRestoreRequestID
	resp.Result.TableRestoreStatus.Status = tr.Status

	return resp, nil
}
