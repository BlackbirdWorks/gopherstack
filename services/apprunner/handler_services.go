package apprunner

import (
	"context"
	"fmt"
)

type imageRepositoryInput struct {
	ImageIdentifier     string `json:"ImageIdentifier"`
	ImageRepositoryType string `json:"ImageRepositoryType"`
}

type sourceConfigurationInput struct {
	ImageRepository *imageRepositoryInput `json:"ImageRepository,omitempty"`
}

type instanceConfigurationInput struct {
	CPU    string `json:"Cpu,omitempty"`
	Memory string `json:"Memory,omitempty"`
}

type createServiceInput struct {
	ServiceName           string                      `json:"ServiceName"`
	SourceConfiguration   *sourceConfigurationInput   `json:"SourceConfiguration,omitempty"`
	InstanceConfiguration *instanceConfigurationInput `json:"InstanceConfiguration,omitempty"`
	Tags                  []tagInput                  `json:"Tags"`
}

type instanceConfigurationOutput struct {
	CPU    string `json:"Cpu,omitempty"`
	Memory string `json:"Memory,omitempty"`
}

type imageRepositoryOutput struct {
	ImageIdentifier     string `json:"ImageIdentifier"`
	ImageRepositoryType string `json:"ImageRepositoryType,omitempty"`
}

type sourceConfigurationOutput struct {
	ImageRepository *imageRepositoryOutput `json:"ImageRepository,omitempty"`
}

type serviceOutput struct {
	InstanceConfiguration *instanceConfigurationOutput `json:"InstanceConfiguration,omitempty"`
	SourceConfiguration   *sourceConfigurationOutput   `json:"SourceConfiguration,omitempty"`
	ServiceArn            string                       `json:"ServiceArn"`
	ServiceID             string                       `json:"ServiceId"`
	ServiceName           string                       `json:"ServiceName"`
	ServiceURL            string                       `json:"ServiceUrl"`
	Status                string                       `json:"Status"`
	CreatedAt             int64                        `json:"CreatedAt"`
	UpdatedAt             int64                        `json:"UpdatedAt"`
}

type createServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func toServiceOutput(svc *Service) serviceOutput {
	out := serviceOutput{
		ServiceArn:  svc.ServiceArn,
		ServiceID:   svc.ServiceID,
		ServiceName: svc.ServiceName,
		ServiceURL:  svc.ServiceURL,
		Status:      svc.Status,
		CreatedAt:   svc.CreatedAt.Unix(),
		UpdatedAt:   svc.UpdatedAt.Unix(),
	}

	if svc.CPU != "" || svc.Memory != "" {
		out.InstanceConfiguration = &instanceConfigurationOutput{
			CPU:    svc.CPU,
			Memory: svc.Memory,
		}
	}

	if svc.ImageURI != "" {
		out.SourceConfiguration = &sourceConfigurationOutput{
			ImageRepository: &imageRepositoryOutput{
				ImageIdentifier: svc.ImageURI,
			},
		}
	}

	return out
}

func (h *Handler) handleCreateService(
	_ context.Context,
	in *createServiceInput,
) (*createServiceOutput, error) {
	if in.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	var cpu, memory, imageURI string

	if in.InstanceConfiguration != nil {
		cpu = in.InstanceConfiguration.CPU
		memory = in.InstanceConfiguration.Memory
	}

	if in.SourceConfiguration != nil && in.SourceConfiguration.ImageRepository != nil {
		imageURI = in.SourceConfiguration.ImageRepository.ImageIdentifier
	}

	tags := tagsFromInput(in.Tags)

	svc, err := h.Backend.CreateService(in.ServiceName, cpu, memory, imageURI, tags)
	if err != nil {
		return nil, err
	}

	return &createServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type describeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type describeServiceOutput struct {
	Service serviceOutput `json:"Service"`
}

func (h *Handler) handleDescribeService(
	_ context.Context,
	in *describeServiceInput,
) (*describeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DescribeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &describeServiceOutput{Service: toServiceOutput(svc)}, nil
}

type updateServiceInput struct {
	SourceConfiguration   *sourceConfigurationInput   `json:"SourceConfiguration,omitempty"`
	InstanceConfiguration *instanceConfigurationInput `json:"InstanceConfiguration,omitempty"`
	ServiceArn            string                      `json:"ServiceArn"`
}

type updateServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleUpdateService(
	_ context.Context,
	in *updateServiceInput,
) (*updateServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	var cpu, memory, imageURI string

	if in.InstanceConfiguration != nil {
		cpu = in.InstanceConfiguration.CPU
		memory = in.InstanceConfiguration.Memory
	}

	if in.SourceConfiguration != nil && in.SourceConfiguration.ImageRepository != nil {
		imageURI = in.SourceConfiguration.ImageRepository.ImageIdentifier
	}

	svc, err := h.Backend.UpdateService(in.ServiceArn, cpu, memory, imageURI)
	if err != nil {
		return nil, err
	}

	return &updateServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type deleteServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type deleteServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleDeleteService(
	_ context.Context,
	in *deleteServiceInput,
) (*deleteServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.DeleteService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &deleteServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type listServicesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type serviceSummaryOutput struct {
	ServiceArn  string `json:"ServiceArn"`
	ServiceID   string `json:"ServiceId"`
	ServiceName string `json:"ServiceName"`
	ServiceURL  string `json:"ServiceUrl"`
	Status      string `json:"Status"`
	CreatedAt   int64  `json:"CreatedAt"`
}

type listServicesOutput struct {
	NextToken          string                 `json:"NextToken,omitempty"`
	ServiceSummaryList []serviceSummaryOutput `json:"ServiceSummaryList"`
}

func (h *Handler) handleListServices(
	_ context.Context,
	in *listServicesInput,
) (*listServicesOutput, error) {
	services, nextToken, err := h.Backend.ListServices(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]serviceSummaryOutput, 0, len(services))
	for _, s := range services {
		out = append(out, serviceSummaryOutput{
			ServiceArn:  s.ServiceArn,
			ServiceID:   s.ServiceID,
			ServiceName: s.ServiceName,
			ServiceURL:  s.ServiceURL,
			Status:      s.Status,
			CreatedAt:   s.CreatedAt.Unix(),
		})
	}

	return &listServicesOutput{ServiceSummaryList: out, NextToken: nextToken}, nil
}

type pauseServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type pauseServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handlePauseService(
	_ context.Context,
	in *pauseServiceInput,
) (*pauseServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.PauseService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &pauseServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type resumeServiceInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type resumeServiceOutput struct {
	OperationID string        `json:"OperationId"`
	Service     serviceOutput `json:"Service"`
}

func (h *Handler) handleResumeService(
	_ context.Context,
	in *resumeServiceInput,
) (*resumeServiceOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	svc, err := h.Backend.ResumeService(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &resumeServiceOutput{
		Service:     toServiceOutput(svc),
		OperationID: newOpID(),
	}, nil
}

type startDeploymentInput struct {
	ServiceArn string `json:"ServiceArn"`
}

type startDeploymentOutput struct {
	OperationID string `json:"OperationId"`
}

func (h *Handler) handleStartDeployment(
	_ context.Context,
	in *startDeploymentInput,
) (*startDeploymentOutput, error) {
	if in.ServiceArn == "" {
		return nil, fmt.Errorf("%w: ServiceArn is required", errInvalidRequest)
	}

	opID, err := h.Backend.StartDeployment(in.ServiceArn)
	if err != nil {
		return nil, err
	}

	return &startDeploymentOutput{OperationID: opID}, nil
}
