package app

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

// GetAzureGraphSettings returns Microsoft Graph settings (azuregraph.ConfigProvider).
func (c *CLI) GetAzureGraphSettings() azuregraph.Settings { return c.AzureGraph }

// GetAzureGraphPort returns the Graph listener's configured port (azurearm.GraphPortProvider).
func (c *CLI) GetAzureGraphPort() int { return c.AzureGraph.Port }

// wireAzureGraph shares ARM's token issuer with Graph, lets the ARM token endpoint authenticate the secrets of
// applications registered in Graph, and lets role assignments resolve principals against the Graph directory.
func wireAzureGraph(byName map[string]service.Registerable) {
	graphH, ok := byName["AzureGraph"].(*azuregraph.Handler)
	if !ok {
		return
	}

	armH, ok := byName["AzureARM"].(*azurearm.Handler)
	if !ok {
		return
	}

	if armH.Issuer != nil {
		graphH.Issuer = armH.Issuer
	}

	armH.SetClientAuthenticator(&graphClientAuth{backend: graphH.Backend})
	armH.SetPrincipalDirectory(graphH.Backend)
}

// graphClientAuth adapts the Graph directory to services/azurearm.ClientAuthenticator.
type graphClientAuth struct {
	backend *azuregraph.Backend
}

func (a *graphClientAuth) AuthenticateClient(clientID, secret string) (string, bool, error) {
	oid, known, err := a.backend.AuthenticateClient(clientID, secret)

	var ce *azuregraph.ClientAuthError
	if errors.As(err, &ce) {
		return "", known, &azurearm.ClientAuthError{
			Name: ce.Name, Description: ce.Description, Status: ce.Status, Code: ce.Code,
		}
	}

	return oid, known, err
}
