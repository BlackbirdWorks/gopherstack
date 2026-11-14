package dashboard

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	eventhubsbackend "github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

type eventHubsPolicyJSON struct {
	Hub    string   `json:"hub,omitempty"`
	Name   string   `json:"name"`
	Rights []string `json:"rights"`
}

type eventHubsPoliciesJSON struct {
	Policies []eventHubsPolicyJSON `json:"policies"`
}

type eventHubsRevealJSON struct {
	Hub  string `json:"hub"`
	Name string `json:"name"`
}

// policies lists the shared access policies without their keys.
func (a *azureEventHubsAPI) policies(c *echo.Context) error {
	rules, err := a.b.ListRules(c.Param("ns"))
	if err != nil {
		return a.fail(c, err)
	}

	out := make([]eventHubsPolicyJSON, 0, len(rules))
	for _, r := range rules {
		out = append(out, eventHubsPolicyJSON{Hub: r.Hub, Name: r.Rule.Name, Rights: r.Rule.Rights})
	}

	return c.JSON(http.StatusOK, eventHubsPoliciesJSON{Policies: out})
}

// revealPolicy returns one policy's key pair; it is the only route that sends keys out.
func (a *azureEventHubsAPI) revealPolicy(c *echo.Context) error {
	var in eventHubsRevealJSON
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	rule, err := a.b.GetRule(c.Param("ns"), in.Hub, in.Name)
	if err != nil {
		if errors.Is(err, eventhubsbackend.ErrRuleNotFound) {
			return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
		}

		return a.fail(c, err)
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, map[string]string{"primaryKey": rule.PrimaryKey, "secondaryKey": rule.SecondaryKey})
}
