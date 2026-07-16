package cognitoidp

import "time"

// Device represents a tracked/remembered device for a user, keyed by DeviceKey.
type Device struct {
	CreatedAt           time.Time         `json:"createdAt"`
	LastModifiedAt      time.Time         `json:"lastModifiedAt"`
	LastAuthenticatedAt time.Time         `json:"lastAuthenticatedAt"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	DeviceKey           string            `json:"deviceKey,omitempty"`
	Status              string            `json:"status,omitempty"`
}

type adminForgetDeviceInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	DeviceKey  string `json:"DeviceKey,omitempty"`
}

type adminForgetDeviceOutput struct{}

type adminGetDeviceInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	DeviceKey  string `json:"DeviceKey,omitempty"`
}

type deviceType struct {
	DeviceCreateDate            *float64        `json:"DeviceCreateDate,omitempty"`
	DeviceLastModifiedDate      *float64        `json:"DeviceLastModifiedDate,omitempty"`
	DeviceLastAuthenticatedDate *float64        `json:"DeviceLastAuthenticatedDate,omitempty"`
	DeviceKey                   string          `json:"DeviceKey,omitempty"`
	DeviceStatus                string          `json:"DeviceStatus,omitempty"`
	DeviceAttributes            []attributeType `json:"DeviceAttributes,omitempty"`
}

type adminGetDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

type adminListDevicesInput struct {
	UserPoolID      string `json:"UserPoolId,omitempty"`
	Username        string `json:"Username,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	Limit           int    `json:"Limit,omitempty"`
}

type adminListDevicesOutput struct {
	PaginationToken string       `json:"PaginationToken,omitempty"`
	Devices         []deviceType `json:"Devices,omitempty"`
}

type adminUpdateDeviceStatusInput struct {
	UserPoolID             string `json:"UserPoolId,omitempty"`
	Username               string `json:"Username,omitempty"`
	DeviceKey              string `json:"DeviceKey,omitempty"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus,omitempty"`
}

type adminUpdateDeviceStatusOutput struct{}

type confirmDeviceInput struct {
	AccessToken                string            `json:"AccessToken,omitempty"`
	DeviceKey                  string            `json:"DeviceKey,omitempty"`
	DeviceSecretVerifierConfig map[string]string `json:"DeviceSecretVerifierConfig,omitempty"`
	DeviceName                 string            `json:"DeviceName,omitempty"`
}

type confirmDeviceOutput struct {
	UserConfirmationNecessary bool `json:"UserConfirmationNecessary,omitempty"`
}

type listDevicesInput struct {
	AccessToken     string `json:"AccessToken,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	Limit           int    `json:"Limit,omitempty"`
}

type listDevicesOutput struct {
	PaginationToken string       `json:"PaginationToken,omitempty"`
	Devices         []deviceType `json:"Devices,omitempty"`
}

type forgetDeviceInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	DeviceKey   string `json:"DeviceKey,omitempty"`
}

type forgetDeviceOutput struct{}

type getDeviceInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	DeviceKey   string `json:"DeviceKey,omitempty"`
}

type getDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

type updateDeviceStatusInput struct {
	AccessToken            string `json:"AccessToken,omitempty"`
	DeviceKey              string `json:"DeviceKey,omitempty"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus,omitempty"`
}

type updateDeviceStatusOutput struct{}
