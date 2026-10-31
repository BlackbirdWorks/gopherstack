package dashboard

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	functionsbackend "github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const (
	azureFunctionsProxyPrefix = "/dashboard/api/v1/azure/functions"
	azureFunctionsMask        = "••••••••"
	azureFunctionsDefaultAuth = "function"
)

type azureFunctionAppJSON struct {
	Created    time.Time  `json:"created"`
	LastDeploy *time.Time `json:"lastDeploy,omitempty"`
	Name       string     `json:"name"`
	HostName   string     `json:"hostName"`
	SCMHost    string     `json:"scmHost"`
	Image      string     `json:"image"`
	Worker     string     `json:"worker"`
	State      string     `json:"state"`
	Stopped    bool       `json:"stopped"`
	Deployed   bool       `json:"deployed"`
}

type azureFunctionSettingJSON struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type azureFunctionJSON struct {
	Name        string `json:"name"`
	Language    string `json:"language"`
	TriggerType string `json:"triggerType"`
	AuthLevel   string `json:"authLevel"`
	InvokeURL   string `json:"invokeUrl"`
	Disabled    bool   `json:"disabled"`
}

type azureFunctionDeploymentJSON struct {
	Received  time.Time `json:"received"`
	Finished  time.Time `json:"finished"`
	ID        string    `json:"id"`
	Deployer  string    `json:"deployer"`
	Message   string    `json:"message"`
	Succeeded bool      `json:"succeeded"`
}

// registerAzureFunctionsRoutes serves the Functions page from the backend in-process.
// Application settings are masked in every listing; a value leaves the server only through reveal.
func (h *DashboardHandler) registerAzureFunctionsRoutes() {
	api := &azureFunctionsAPI{}
	if h.config.AzureFunctionsOps != nil {
		api.b = h.config.AzureFunctionsOps.Backend
	}

	r := h.SubRouter
	r.Add(http.MethodGet, azureFunctionsProxyPrefix+"/apps", api.guard(api.apps))
	r.Add(http.MethodGet, azureFunctionsProxyPrefix+"/apps/:name/settings", api.guard(api.settings))
	r.Add(http.MethodPost, azureFunctionsProxyPrefix+"/apps/:name/settings/reveal", api.guard(api.reveal))
	r.Add(http.MethodGet, azureFunctionsProxyPrefix+"/apps/:name/functions", api.guard(api.functions))
	r.Add(http.MethodGet, azureFunctionsProxyPrefix+"/apps/:name/deployments", api.guard(api.deployments))
	r.Add(http.MethodPost, azureFunctionsProxyPrefix+"/apps/:name/:action", api.guard(api.lifecycle))
}

type azureFunctionsAPI struct {
	b *functionsbackend.InMemoryBackend
}

func (a *azureFunctionsAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure Functions")
		}

		return next(c)
	}
}

func (a *azureFunctionsAPI) fail(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, functionsbackend.ErrAppNotFound), errors.Is(err, functionsbackend.ErrFunctionNotFound):
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
	default:
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
	}
}

func azureFunctionApp(name string, i functionsbackend.AppInfo) azureFunctionAppJSON {
	out := azureFunctionAppJSON{
		Name: name, Created: i.Created, HostName: i.HostName, SCMHost: i.SCMHost,
		Image: i.Image, Worker: i.Worker, Stopped: i.Stopped, Deployed: i.Deployed, State: "Running",
	}

	if i.Stopped {
		out.State = "Stopped"
	}

	if !i.LastDeploy.IsZero() {
		out.LastDeploy = &i.LastDeploy
	}

	return out
}

func (a *azureFunctionsAPI) apps(c *echo.Context) error {
	names := a.b.Names()
	items := make([]azureFunctionAppJSON, 0, len(names))

	for _, name := range names {
		if info, ok := a.b.App(name); ok {
			items = append(items, azureFunctionApp(name, info))
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"apps": items})
}

func (a *azureFunctionsAPI) settings(c *echo.Context) error {
	settings, err := a.b.Settings(c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	names := make([]string, 0, len(settings))
	for k := range settings {
		names = append(names, k)
	}

	slices.Sort(names)

	items := make([]azureFunctionSettingJSON, 0, len(names))
	for _, k := range names {
		items = append(items, azureFunctionSettingJSON{Name: k, Value: azureFunctionsMask})
	}

	return c.JSON(http.StatusOK, map[string]any{"settings": items})
}

func (a *azureFunctionsAPI) reveal(c *echo.Context) error {
	var in struct {
		Name string `json:"name"`
	}

	if !cloudJSONDecode(c, &in) {
		return nil
	}

	settings, err := a.b.Settings(c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	value, ok := settings[in.Name]
	if !ok {
		return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, "setting not found")
	}

	return c.JSON(http.StatusOK, azureFunctionSettingJSON{Name: in.Name, Value: value})
}

// azureFunctionTrigger returns the trigger binding type and auth level of a function.json config.
func azureFunctionTrigger(cfg map[string]any) (string, string) {
	bindings, _ := cfg["bindings"].([]any)

	for _, raw := range bindings {
		bd, _ := raw.(map[string]any)
		typ, _ := bd["type"].(string)

		if !strings.HasSuffix(strings.ToLower(typ), "trigger") {
			continue
		}

		auth, _ := bd["authLevel"].(string)
		if auth == "" && strings.EqualFold(typ, "httpTrigger") {
			auth = azureFunctionsDefaultAuth
		}

		return typ, auth
	}

	return "", ""
}

func (a *azureFunctionsAPI) functions(c *echo.Context) error {
	fns, err := a.b.Functions(c.Request().Context(), c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	items := make([]azureFunctionJSON, 0, len(fns))

	for _, f := range fns {
		trigger, auth := azureFunctionTrigger(f.Config)
		items = append(items, azureFunctionJSON{
			Name: f.Name, Language: f.Language, TriggerType: trigger, AuthLevel: auth,
			InvokeURL: f.InvokeURL, Disabled: f.IsDisabled,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"functions": items})
}

func (a *azureFunctionsAPI) deployments(c *echo.Context) error {
	deps, err := a.b.Deployments(c.Param("name"))
	if err != nil {
		return a.fail(c, err)
	}

	items := make([]azureFunctionDeploymentJSON, 0, len(deps))
	for _, d := range slices.Backward(deps) {
		items = append(items, azureFunctionDeploymentJSON{
			ID: d.ID, Deployer: d.Deployer, Message: d.Message, Succeeded: d.Succeeded,
			Received: d.Received, Finished: d.Finished,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"deployments": items})
}

func (a *azureFunctionsAPI) lifecycle(c *echo.Context) error {
	name := c.Param("name")

	var err error

	switch c.Param("action") {
	case aciPxStart:
		err = a.b.SetStopped(name, false)
	case "stop":
		err = a.b.SetStopped(name, true)
	case aciPxRestart:
		err = a.b.Restart(name)
	default:
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "unsupported action")
	}

	if err != nil {
		return a.fail(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
