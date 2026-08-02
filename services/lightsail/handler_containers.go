package lightsail

import "context"

// containerOps returns the dispatch table for family Q+R (12 ops).
func (h *Handler) containerOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateContainerService":              h.handleCreateContainerService,
		"UpdateContainerService":              h.handleUpdateContainerService,
		"DeleteContainerService":              h.handleDeleteContainerService,
		"GetContainerServices":                h.handleGetContainerServices,
		"GetContainerServiceMetricData":       h.handleGetContainerServiceMetricData,
		"CreateContainerServiceDeployment":    h.handleCreateContainerServiceDeployment,
		"GetContainerServiceDeployments":      h.handleGetContainerServiceDeployments,
		"CreateContainerServiceRegistryLogin": h.handleCreateContainerServiceRegistryLogin,
		"RegisterContainerImage":              h.handleRegisterContainerImage,
		"GetContainerImages":                  h.handleGetContainerImages,
		"DeleteContainerImage":                h.handleDeleteContainerImage,
		"GetContainerLog":                     h.handleGetContainerLog,
	}
}

type containerWire struct {
	Environment map[string]string `json:"environment,omitempty"`
	Ports       map[string]string `json:"ports,omitempty"`
	Image       string            `json:"image,omitempty"`
	Command     []string          `json:"command,omitempty"`
}

type containerServiceEndpointWire struct {
	ContainerName string `json:"containerName,omitempty"`
	ContainerPort int32  `json:"containerPort,omitempty"`
}

type containerServiceDeploymentWire struct {
	Containers     map[string]containerWire      `json:"containers,omitempty"`
	CreatedAt      *float64                      `json:"createdAt,omitempty"`
	PublicEndpoint *containerServiceEndpointWire `json:"publicEndpoint,omitempty"`
	State          string                        `json:"state,omitempty"`
	Version        int32                         `json:"version,omitempty"`
}

func containerDeploymentToWire(d *ContainerServiceDeployment) *containerServiceDeploymentWire {
	if d == nil {
		return nil
	}

	containers := make(map[string]containerWire, len(d.Containers))
	for k, c := range d.Containers {
		containers[k] = containerWire(c)
	}

	w := &containerServiceDeploymentWire{
		Containers: containers,
		CreatedAt:  epochPtr(d.CreatedAt),
		State:      d.State,
		Version:    d.Version,
	}

	if d.PublicEndpoint != nil {
		w.PublicEndpoint = &containerServiceEndpointWire{
			ContainerName: d.PublicEndpoint.ContainerName,
			ContainerPort: d.PublicEndpoint.ContainerPort,
		}
	}

	return w
}

type containerServiceWire struct {
	NextDeployment       *containerServiceDeploymentWire  `json:"nextDeployment,omitempty"`
	StateDetail          *containerServiceStateDetailWire `json:"stateDetail,omitempty"`
	CreatedAt            *float64                         `json:"createdAt,omitempty"`
	CurrentDeployment    *containerServiceDeploymentWire  `json:"currentDeployment,omitempty"`
	PublicDomainNames    map[string][]string              `json:"publicDomainNames,omitempty"`
	Location             *resourceLocationWire            `json:"location,omitempty"`
	ResourceType         string                           `json:"resourceType,omitempty"`
	Power                string                           `json:"power,omitempty"`
	PowerID              string                           `json:"powerId,omitempty"`
	PrincipalArn         string                           `json:"principalArn,omitempty"`
	PrivateDomainName    string                           `json:"privateDomainName,omitempty"`
	Arn                  string                           `json:"arn,omitempty"`
	State                string                           `json:"state,omitempty"`
	ContainerServiceName string                           `json:"containerServiceName,omitempty"`
	URL                  string                           `json:"url,omitempty"`
	Tags                 []tagWire                        `json:"tags,omitempty"`
	Scale                int32                            `json:"scale,omitempty"`
	IsDisabled           bool                             `json:"isDisabled,omitempty"`
}

type containerServiceStateDetailWire struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func containerServiceToWire(cs *ContainerService) containerServiceWire {
	w := containerServiceWire{
		Arn: cs.Arn, ContainerServiceName: cs.Name, CreatedAt: epochPtr(cs.CreatedAt),
		CurrentDeployment: containerDeploymentToWire(cs.CurrentDeployment), IsDisabled: cs.IsDisabled,
		Location: locationToWire(cs.Location), NextDeployment: containerDeploymentToWire(cs.NextDeployment),
		Power: cs.Power, PowerID: cs.PowerID, PrincipalArn: cs.PrincipalArn, PrivateDomainName: cs.PrivateDomainName,
		PublicDomainNames: cs.PublicDomainNames, ResourceType: ResourceTypeContainerService, Scale: cs.Scale,
		State: cs.State, Tags: mapFromTags(cs.Tags), URL: cs.URL,
	}

	if cs.StateDetailCode != "" {
		w.StateDetail = &containerServiceStateDetailWire{Code: cs.StateDetailCode}
	}

	return w
}

type endpointRequestWire struct {
	ContainerName string `json:"containerName,omitempty"`
	ContainerPort int32  `json:"containerPort,omitempty"`
}

type containerServiceDeploymentRequestWire struct {
	Containers     map[string]containerWire `json:"containers,omitempty"`
	PublicEndpoint *endpointRequestWire     `json:"publicEndpoint,omitempty"`
}

func containersFromWire(in map[string]containerWire) map[string]ContainerDefinition {
	out := make(map[string]ContainerDefinition, len(in))
	for k, c := range in {
		out[k] = ContainerDefinition(c)
	}

	return out
}

type createContainerServiceRequest struct {
	Deployment            *containerServiceDeploymentRequestWire `json:"deployment,omitempty"`
	PrivateRegistryAccess *struct {
		EcrImagePullerRole *struct {
			IsActive bool `json:"isActive,omitempty"`
		} `json:"ecrImagePullerRole,omitempty"`
	} `json:"privateRegistryAccess,omitempty"`
	PublicDomainNames map[string][]string `json:"publicDomainNames,omitempty"`
	Power             string              `json:"power"`
	ServiceName       string              `json:"serviceName"`
	Tags              []tagWire           `json:"tags,omitempty"`
	Scale             int32               `json:"scale"`
}

func (h *Handler) handleCreateContainerService(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createContainerServiceRequest](body)
	if err != nil {
		return nil, err
	}

	var deployment *ContainerServiceDeployment

	if req.Deployment != nil {
		var ep *ContainerServiceEndpoint
		if req.Deployment.PublicEndpoint != nil {
			ep = &ContainerServiceEndpoint{
				ContainerName: req.Deployment.PublicEndpoint.ContainerName,
				ContainerPort: req.Deployment.PublicEndpoint.ContainerPort,
			}
		}

		deployment = &ContainerServiceDeployment{
			Containers:     containersFromWire(req.Deployment.Containers),
			PublicEndpoint: ep,
		}
	}

	cs, createErr := h.Backend.CreateContainerService(
		req.ServiceName,
		req.Power,
		req.Scale,
		deployment,
		req.PublicDomainNames,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	w := containerServiceToWire(cs)

	return marshalResponse(containerServiceEnvelope{ContainerService: &w})
}

type containerServiceEnvelope struct {
	ContainerService *containerServiceWire `json:"containerService,omitempty"`
}

type serviceNameRequest struct {
	ServiceName string `json:"serviceName"`
}

type updateContainerServiceRequest struct {
	PublicDomainNames map[string][]string `json:"publicDomainNames,omitempty"`
	Power             string              `json:"power,omitempty"`
	ServiceName       string              `json:"serviceName"`
	Scale             int32               `json:"scale,omitempty"`
	IsDisabled        bool                `json:"isDisabled,omitempty"`
}

func (h *Handler) handleUpdateContainerService(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateContainerServiceRequest](body)
	if err != nil {
		return nil, err
	}

	disabled := req.IsDisabled

	cs, updateErr := h.Backend.UpdateContainerService(
		req.ServiceName,
		&disabled,
		req.Power,
		req.Scale,
		req.PublicDomainNames,
	)
	if updateErr != nil {
		return nil, updateErr
	}

	w := containerServiceToWire(cs)

	return marshalResponse(containerServiceEnvelope{ContainerService: &w})
}

func (h *Handler) handleDeleteContainerService(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[serviceNameRequest](body)
	if err != nil {
		return nil, err
	}

	if delErr := h.Backend.DeleteContainerService(req.ServiceName); delErr != nil {
		return nil, delErr
	}

	return marshalResponse(struct{}{})
}

type containerServicesListResponse struct {
	ContainerServices []containerServiceWire `json:"containerServices,omitempty"`
}

func (h *Handler) handleGetContainerServices(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[serviceNameRequest](body)
	if err != nil {
		return nil, err
	}

	svcs, getErr := h.Backend.GetContainerServices(req.ServiceName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]containerServiceWire, len(svcs))
	for i, cs := range svcs {
		out[i] = containerServiceToWire(cs)
	}

	return marshalResponse(containerServicesListResponse{ContainerServices: out})
}

type containerServiceMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

type containerServiceMetricDataRequest struct {
	MetricName  string `json:"metricName,omitempty"`
	ServiceName string `json:"serviceName"`
}

func (h *Handler) handleGetContainerServiceMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[containerServiceMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetContainerServiceMetricData(req.ServiceName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(containerServiceMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName})
}

type createContainerServiceDeploymentRequest struct {
	Containers     map[string]containerWire `json:"containers,omitempty"`
	PublicEndpoint *endpointRequestWire     `json:"publicEndpoint,omitempty"`
	ServiceName    string                   `json:"serviceName"`
}

func (h *Handler) handleCreateContainerServiceDeployment(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createContainerServiceDeploymentRequest](body)
	if err != nil {
		return nil, err
	}

	var ep *ContainerServiceEndpoint
	if req.PublicEndpoint != nil {
		ep = &ContainerServiceEndpoint{
			ContainerName: req.PublicEndpoint.ContainerName,
			ContainerPort: req.PublicEndpoint.ContainerPort,
		}
	}

	cs, createErr := h.Backend.CreateContainerServiceDeployment(req.ServiceName, containersFromWire(req.Containers), ep)
	if createErr != nil {
		return nil, createErr
	}

	w := containerServiceToWire(cs)

	return marshalResponse(containerServiceEnvelope{ContainerService: &w})
}

type containerServiceDeploymentsListResponse struct {
	Deployments []containerServiceDeploymentWire `json:"deployments,omitempty"`
}

func (h *Handler) handleGetContainerServiceDeployments(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[serviceNameRequest](body)
	if err != nil {
		return nil, err
	}

	deployments, getErr := h.Backend.GetContainerServiceDeployments(req.ServiceName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]containerServiceDeploymentWire, len(deployments))
	for i := range deployments {
		out[i] = *containerDeploymentToWire(&deployments[i])
	}

	return marshalResponse(containerServiceDeploymentsListResponse{Deployments: out})
}

type registryLoginResponse struct {
	RegistryLogin *registryLoginWire `json:"registryLogin,omitempty"`
}

type registryLoginWire struct {
	ExpiresAt *float64 `json:"expiresAt,omitempty"`
	Password  string   `json:"password,omitempty"`
	Registry  string   `json:"registry,omitempty"`
	Username  string   `json:"username,omitempty"`
}

func (h *Handler) handleCreateContainerServiceRegistryLogin(_ context.Context, _ []byte) ([]byte, error) {
	username, password, registry, expires := h.Backend.CreateContainerServiceRegistryLogin()
	w := &registryLoginWire{ExpiresAt: epochPtr(expires), Password: password, Registry: registry, Username: username}

	return marshalResponse(registryLoginResponse{RegistryLogin: w})
}

type registerContainerImageRequest struct {
	Digest      string `json:"digest"`
	Label       string `json:"label"`
	ServiceName string `json:"serviceName"`
}

type containerImageWire struct {
	CreatedAt *float64 `json:"createdAt,omitempty"`
	Digest    string   `json:"digest,omitempty"`
	Image     string   `json:"image,omitempty"`
}

type containerImageEnvelope struct {
	ContainerImage *containerImageWire `json:"containerImage,omitempty"`
}

func (h *Handler) handleRegisterContainerImage(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[registerContainerImageRequest](body)
	if err != nil {
		return nil, err
	}

	img, regErr := h.Backend.RegisterContainerImage(req.ServiceName, req.Label, req.Digest)
	if regErr != nil {
		return nil, regErr
	}

	w := &containerImageWire{CreatedAt: epochPtr(img.CreatedAt), Digest: img.Digest, Image: img.Image}

	return marshalResponse(containerImageEnvelope{ContainerImage: w})
}

type containerImagesListResponse struct {
	ContainerImages []containerImageWire `json:"containerImages,omitempty"`
}

func (h *Handler) handleGetContainerImages(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[serviceNameRequest](body)
	if err != nil {
		return nil, err
	}

	imgs, getErr := h.Backend.GetContainerImages(req.ServiceName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]containerImageWire, len(imgs))
	for i, img := range imgs {
		out[i] = containerImageWire{CreatedAt: epochPtr(img.CreatedAt), Digest: img.Digest, Image: img.Image}
	}

	return marshalResponse(containerImagesListResponse{ContainerImages: out})
}

type deleteContainerImageRequest struct {
	Image       string `json:"image"`
	ServiceName string `json:"serviceName"`
}

func (h *Handler) handleDeleteContainerImage(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteContainerImageRequest](body)
	if err != nil {
		return nil, err
	}

	if delErr := h.Backend.DeleteContainerImage(req.ServiceName, req.Image); delErr != nil {
		return nil, delErr
	}

	return marshalResponse(struct{}{})
}

type getContainerLogRequest struct {
	EndTime       *float64 `json:"endTime,omitempty"`
	StartTime     *float64 `json:"startTime,omitempty"`
	ContainerName string   `json:"containerName"`
	FilterPattern string   `json:"filterPattern,omitempty"`
	PageToken     string   `json:"pageToken,omitempty"`
	ServiceName   string   `json:"serviceName"`
}

type getContainerLogResponse struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	LogEvents     []logEventWire `json:"logEvents,omitempty"`
}

func (h *Handler) handleGetContainerLog(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getContainerLogRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetContainerLog(req.ServiceName, req.ContainerName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getContainerLogResponse{LogEvents: []logEventWire{}})
}
