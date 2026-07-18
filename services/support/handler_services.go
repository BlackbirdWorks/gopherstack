package support

import (
	"context"
	"fmt"
)

type describeServicesInput struct {
	Language        string   `json:"language"`
	ServiceCodeList []string `json:"serviceCodeList"`
}

type serviceCategoryView struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type serviceView struct {
	Code       string                `json:"code"`
	Name       string                `json:"name"`
	Categories []serviceCategoryView `json:"categories"`
}

type describeServicesOutput struct {
	Services []serviceView `json:"services"`
}

func (h *Handler) handleDescribeServices(
	_ context.Context,
	in *describeServicesInput,
) (*describeServicesOutput, error) {
	if in.Language != "" && !validLanguage(in.Language) {
		return nil, fmt.Errorf("%w: invalid language", ErrValidation)
	}
	services := h.Backend.DescribeServices(in.ServiceCodeList, in.Language)

	views := make([]serviceView, 0, len(services))
	for _, svc := range services {
		cats := make([]serviceCategoryView, 0, len(svc.Categories))
		for _, c := range svc.Categories {
			cats = append(cats, serviceCategoryView(c))
		}

		views = append(views, serviceView{
			Code:       svc.Code,
			Name:       svc.Name,
			Categories: cats,
		})
	}

	return &describeServicesOutput{Services: views}, nil
}
