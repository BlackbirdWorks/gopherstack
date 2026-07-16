package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleAdminForgetDevice(
	_ context.Context,
	in *adminForgetDeviceInput,
) (*adminForgetDeviceOutput, error) {
	if err := h.Backend.AdminForgetDevice(in.UserPoolID, in.Username, in.DeviceKey); err != nil {
		return nil, err
	}

	return &adminForgetDeviceOutput{}, nil
}

// toDeviceType converts a stored Device into its AWS wire representation.
func toDeviceType(d *Device) *deviceType {
	if d == nil {
		return nil
	}

	created := float64(d.CreatedAt.Unix())
	modified := float64(d.LastModifiedAt.Unix())
	lastAuth := float64(d.LastAuthenticatedAt.Unix())

	return &deviceType{
		DeviceKey:                   d.DeviceKey,
		DeviceStatus:                d.Status,
		DeviceCreateDate:            &created,
		DeviceLastModifiedDate:      &modified,
		DeviceLastAuthenticatedDate: &lastAuth,
		DeviceAttributes:            sortedAttributeList(d.Attributes),
	}
}

func (h *Handler) handleAdminGetDevice(_ context.Context, in *adminGetDeviceInput) (*adminGetDeviceOutput, error) {
	dev, err := h.Backend.AdminGetDevice(in.UserPoolID, in.Username, in.DeviceKey)
	if err != nil {
		return nil, err
	}

	return &adminGetDeviceOutput{Device: toDeviceType(dev)}, nil
}

func (h *Handler) handleAdminListDevices(
	_ context.Context,
	in *adminListDevicesInput,
) (*adminListDevicesOutput, error) {
	limit, err := validateCognitoMaxResults(in.Limit)
	if err != nil {
		return nil, err
	}

	devices, token, err := h.Backend.AdminListDevices(in.UserPoolID, in.Username, limit, in.PaginationToken)
	if err != nil {
		return nil, err
	}

	out := make([]deviceType, 0, len(devices))
	for _, d := range devices {
		out = append(out, *toDeviceType(d))
	}

	return &adminListDevicesOutput{Devices: out, PaginationToken: token}, nil
}

func (h *Handler) handleAdminUpdateDeviceStatus(
	_ context.Context,
	in *adminUpdateDeviceStatusInput,
) (*adminUpdateDeviceStatusOutput, error) {
	if err := h.Backend.AdminUpdateDeviceStatus(
		in.UserPoolID, in.Username, in.DeviceKey, in.DeviceRememberedStatus,
	); err != nil {
		return nil, err
	}

	return &adminUpdateDeviceStatusOutput{}, nil
}

func (h *Handler) handleConfirmDevice(_ context.Context, in *confirmDeviceInput) (*confirmDeviceOutput, error) {
	_, necessary, err := h.Backend.ConfirmDevice(in.AccessToken, in.DeviceKey, in.DeviceName)
	if err != nil {
		return nil, err
	}

	return &confirmDeviceOutput{UserConfirmationNecessary: necessary}, nil
}

func (h *Handler) handleListDevices(_ context.Context, in *listDevicesInput) (*listDevicesOutput, error) {
	limit, err := validateCognitoMaxResults(in.Limit)
	if err != nil {
		return nil, err
	}

	devices, token, err := h.Backend.ListDevices(in.AccessToken, limit, in.PaginationToken)
	if err != nil {
		return nil, err
	}

	out := make([]deviceType, 0, len(devices))
	for _, d := range devices {
		out = append(out, *toDeviceType(d))
	}

	return &listDevicesOutput{Devices: out, PaginationToken: token}, nil
}

func (h *Handler) handleForgetDevice(_ context.Context, in *forgetDeviceInput) (*forgetDeviceOutput, error) {
	if err := h.Backend.ForgetDevice(in.AccessToken, in.DeviceKey); err != nil {
		return nil, err
	}

	return &forgetDeviceOutput{}, nil
}

func (h *Handler) handleGetDevice(_ context.Context, in *getDeviceInput) (*getDeviceOutput, error) {
	dev, err := h.Backend.GetDevice(in.AccessToken, in.DeviceKey)
	if err != nil {
		return nil, err
	}

	return &getDeviceOutput{Device: toDeviceType(dev)}, nil
}

func (h *Handler) handleUpdateDeviceStatus(
	_ context.Context,
	in *updateDeviceStatusInput,
) (*updateDeviceStatusOutput, error) {
	if err := h.Backend.UpdateDeviceStatus(in.AccessToken, in.DeviceKey, in.DeviceRememberedStatus); err != nil {
		return nil, err
	}

	return &updateDeviceStatusOutput{}, nil
}

func (h *Handler) devicesOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminForgetDevice": service.WrapOp(h.handleAdminForgetDevice),
	}
}

func (h *Handler) devicesOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminGetDevice":          service.WrapOp(h.handleAdminGetDevice),
		"AdminListDevices":        service.WrapOp(h.handleAdminListDevices),
		"AdminUpdateDeviceStatus": service.WrapOp(h.handleAdminUpdateDeviceStatus),
		"ConfirmDevice":           service.WrapOp(h.handleConfirmDevice),
		"ForgetDevice":            service.WrapOp(h.handleForgetDevice),
		"GetDevice":               service.WrapOp(h.handleGetDevice),
		"ListDevices":             service.WrapOp(h.handleListDevices),
		"UpdateDeviceStatus":      service.WrapOp(h.handleUpdateDeviceStatus),
	}
}
