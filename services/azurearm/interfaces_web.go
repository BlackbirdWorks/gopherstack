package azurearm

import (
	"context"
	"time"
)

// Host key collections ARM addresses as host/default/{keyType}.
const (
	FunctionHostKeyTypeFunction = "functionKeys"
	FunctionHostKeyTypeSystem   = "systemKeys"
)

// FunctionAppConfig is what ARM tells the data plane about a function app's stack.
type FunctionAppConfig struct {
	Kind            string
	LinuxFx         string
	AppCommandLine  string
	HealthCheckPath string
}

// FunctionAppInfo is the data plane's view of a function app.
type FunctionAppInfo struct {
	Created    time.Time
	LastDeploy time.Time
	HostName   string
	SCMHost    string
	Image      string
	Worker     string
	Failure    string
	Stopped    bool
	Deployed   bool
}

// FunctionAppFunction is one function of an app.
type FunctionAppFunction struct {
	Config     map[string]any
	Name       string
	Language   string
	InvokeURL  string
	IsDisabled bool
}

// FunctionHostKeys are an app's host-level keys.
type FunctionHostKeys struct {
	FunctionKeys map[string]string
	SystemKeys   map[string]string
	MasterKey    string
}

// FunctionApps is the seam the Microsoft.Web resource provider uses to keep function apps and Linux web apps in
// step with the Azure Functions data plane. Primitive types only, so this package never imports the data-plane service.
// Slots are addressed as "<app>/<slot>".
// Implementations return ErrWebSiteNotFound, ErrWebFunctionNotFound, ErrWebKeyNotFound or ErrWebInvalidKeyType.
type FunctionApps interface {
	PutApp(name string, cfg FunctionAppConfig) (FunctionAppInfo, error)
	DeleteApp(name string) error
	AppInfo(name string) (FunctionAppInfo, bool)
	Settings(name string) (map[string]string, error)
	SetSettings(name string, settings map[string]string) error
	SetStopped(name string, stopped bool) error
	Restart(name string) error
	Functions(ctx context.Context, name string) ([]FunctionAppFunction, error)
	HostKeys(name string) (FunctionHostKeys, error)
	SetHostKey(name, keyType, keyName, value string) (string, error)
	DeleteHostKey(name, keyType, keyName string) error
	FunctionKeys(name, fn string) (map[string]string, error)
	SetFunctionKey(name, fn, keyName, value string) (string, error)
	DeleteFunctionKey(name, fn, keyName string) error
	PublishingCredentials(name string) (user, password, scmURI string, err error)
	SwapContent(first, second string) error
	WarmUp(ctx context.Context, name string) error
}

type noopFunctionApps struct{}

func (noopFunctionApps) PutApp(string, FunctionAppConfig) (FunctionAppInfo, error) {
	return FunctionAppInfo{}, ErrWebFunctionsUnavailable
}

func (noopFunctionApps) DeleteApp(string) error                 { return nil }
func (noopFunctionApps) AppInfo(string) (FunctionAppInfo, bool) { return FunctionAppInfo{}, false }

func (noopFunctionApps) Settings(string) (map[string]string, error) {
	return nil, ErrWebFunctionsUnavailable
}

func (noopFunctionApps) SetSettings(string, map[string]string) error {
	return ErrWebFunctionsUnavailable
}

func (noopFunctionApps) SetStopped(string, bool) error { return ErrWebFunctionsUnavailable }

func (noopFunctionApps) Restart(string) error { return ErrWebFunctionsUnavailable }

func (noopFunctionApps) Functions(context.Context, string) ([]FunctionAppFunction, error) {
	return nil, ErrWebFunctionsUnavailable
}

func (noopFunctionApps) HostKeys(string) (FunctionHostKeys, error) {
	return FunctionHostKeys{}, ErrWebFunctionsUnavailable
}

func (noopFunctionApps) SetHostKey(string, string, string, string) (string, error) {
	return "", ErrWebFunctionsUnavailable
}

func (noopFunctionApps) DeleteHostKey(string, string, string) error {
	return ErrWebFunctionsUnavailable
}

func (noopFunctionApps) FunctionKeys(string, string) (map[string]string, error) {
	return nil, ErrWebFunctionsUnavailable
}

func (noopFunctionApps) SetFunctionKey(string, string, string, string) (string, error) {
	return "", ErrWebFunctionsUnavailable
}

func (noopFunctionApps) DeleteFunctionKey(string, string, string) error {
	return ErrWebFunctionsUnavailable
}

func (noopFunctionApps) PublishingCredentials(string) (string, string, string, error) {
	return "", "", "", ErrWebFunctionsUnavailable
}

func (noopFunctionApps) SwapContent(string, string) error { return ErrWebFunctionsUnavailable }

func (noopFunctionApps) WarmUp(context.Context, string) error { return ErrWebFunctionsUnavailable }

var _ FunctionApps = noopFunctionApps{}
