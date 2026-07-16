package cognitoidp

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// clientToData converts a UserPoolClient to the wire format.
func clientToData(c *UserPoolClient) userPoolClientData {
	return userPoolClientData{
		ClientID:     c.ClientID,
		ClientName:   c.ClientName,
		UserPoolID:   c.UserPoolID,
		ClientSecret: c.ClientSecret,
		CreationDate: float64(c.CreatedAt.Unix()),
	}
}

func (h *Handler) handleCreateUserPoolClient(
	_ context.Context,
	in *createUserPoolClientInput,
) (*createUserPoolClientOutput, error) {
	client, err := h.Backend.CreateUserPoolClient(in.UserPoolID, in.ClientName)
	if err != nil {
		return nil, err
	}

	return &createUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
}

func (h *Handler) handleDescribeUserPoolClient(
	_ context.Context,
	in *describeUserPoolClientInput,
) (*describeUserPoolClientOutput, error) {
	client, err := h.Backend.DescribeUserPoolClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
}

func (h *Handler) handleDeleteUserPoolClient(
	_ context.Context,
	in *deleteUserPoolClientInput,
) (*deleteUserPoolClientOutput, error) {
	if err := h.Backend.DeleteUserPoolClient(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &deleteUserPoolClientOutput{}, nil
}

func (h *Handler) handleListUserPoolClients(
	_ context.Context,
	in *listUserPoolClientsInput,
) (*listUserPoolClientsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	// ListUserPoolClients already returns clients sorted by name, giving a
	// stable ordering for pagination tokens.
	clients, err := h.Backend.ListUserPoolClients(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	start := 0
	if in.NextToken != "" {
		for i, c := range clients {
			if c.ClientID == in.NextToken {
				start = i

				break
			}
		}
	}

	clients = clients[start:]

	nextToken := ""
	if len(clients) > limit {
		nextToken = clients[limit].ClientID
		clients = clients[:limit]
	}

	items := make([]userPoolClientData, 0, len(clients))
	for _, c := range clients {
		items = append(items, clientToData(c))
	}

	return &listUserPoolClientsOutput{UserPoolClients: items, NextToken: nextToken}, nil
}

func (h *Handler) handleAddUserPoolClientSecret(
	_ context.Context,
	in *addUserPoolClientSecretInput,
) (*addUserPoolClientSecretOutput, error) {
	secret, err := h.Backend.AddUserPoolClientSecret(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &addUserPoolClientSecretOutput{ClientSecret: secret}, nil
}

func (h *Handler) handleUpdateUserPoolClient(
	_ context.Context,
	in *updateUserPoolClientInput,
) (*updateUserPoolClientOutput, error) {
	client, err := h.Backend.UpdateUserPoolClient(in.UserPoolID, in.ClientID, in.ClientName)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
}

func clientToAccurateData(c *UserPoolClient) clientDataAccurate {
	flows := make([]string, len(c.AllowedOAuthFlows))
	copy(flows, c.AllowedOAuthFlows)
	scopes := make([]string, len(c.AllowedOAuthScopes))
	copy(scopes, c.AllowedOAuthScopes)
	ef := make([]string, len(c.ExplicitAuthFlows))
	copy(ef, c.ExplicitAuthFlows)
	sort.Strings(flows)
	sort.Strings(scopes)

	lastModified := c.CreatedAt
	if !c.UpdatedAt.IsZero() {
		lastModified = c.UpdatedAt
	}

	return clientDataAccurate{
		ClientID:                        c.ClientID,
		ClientName:                      c.ClientName,
		UserPoolID:                      c.UserPoolID,
		ClientSecret:                    c.ClientSecret,
		PreventUserExistenceErrors:      c.PreventUserExistenceErrors,
		AllowedOAuthFlows:               flows,
		AllowedOAuthScopes:              scopes,
		ExplicitAuthFlows:               ef,
		CallbackURLs:                    c.CallbackURLs,
		LogoutURLs:                      c.LogoutURLs,
		SupportedIdentityProviders:      c.SupportedIdentityProviders,
		CreationDate:                    float64(c.CreatedAt.Unix()),
		LastModifiedDate:                float64(lastModified.Unix()),
		AccessTokenValidity:             c.AccessTokenValidity,
		IDTokenValidity:                 c.IDTokenValidity,
		RefreshTokenValidity:            c.RefreshTokenValidity,
		TokenValidityUnits:              c.TokenValidityUnits,
		EnableTokenRevocation:           c.EnableTokenRevocation,
		AllowedOAuthFlowsUserPoolClient: c.AllowedOAuthFlowsUserPoolClient,
	}
}

func (h *Handler) handleCreateUserPoolClientWithOpts(
	_ context.Context,
	in *createUserPoolClientWithOptsInput,
) (*createUserPoolClientWithOptsOutput, error) {
	opts := UserPoolClientOptions{
		AllowedOAuthFlows:               in.AllowedOAuthFlows,
		AllowedOAuthScopes:              in.AllowedOAuthScopes,
		PreventUserExistenceErrors:      in.PreventUserExistenceErrors,
		ExplicitAuthFlows:               in.ExplicitAuthFlows,
		CallbackURLs:                    in.CallbackURLs,
		LogoutURLs:                      in.LogoutURLs,
		SupportedIdentityProviders:      in.SupportedIdentityProviders,
		GenerateSecret:                  in.GenerateSecret,
		EnableTokenRevocation:           in.EnableTokenRevocation,
		AllowedOAuthFlowsUserPoolClient: in.AllowedOAuthFlowsUserPoolClient,
		AccessTokenValidity:             in.AccessTokenValidity,
		IDTokenValidity:                 in.IDTokenValidity,
		RefreshTokenValidity:            in.RefreshTokenValidity,
		TokenValidityUnits:              in.TokenValidityUnits,
	}

	client, err := h.Backend.CreateUserPoolClientWithOpts(in.UserPoolID, in.ClientName, opts)
	if err != nil {
		return nil, err
	}

	return &createUserPoolClientWithOptsOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

func (h *Handler) handleUpdateUserPoolClientWithOpts(
	_ context.Context,
	in *updateUserPoolClientWithOptsInput,
) (*updateUserPoolClientWithOptsOutput, error) {
	opts := UserPoolClientOptions{
		AllowedOAuthFlows:               in.AllowedOAuthFlows,
		AllowedOAuthScopes:              in.AllowedOAuthScopes,
		ExplicitAuthFlows:               in.ExplicitAuthFlows,
		CallbackURLs:                    in.CallbackURLs,
		LogoutURLs:                      in.LogoutURLs,
		SupportedIdentityProviders:      in.SupportedIdentityProviders,
		PreventUserExistenceErrors:      in.PreventUserExistenceErrors,
		EnableTokenRevocation:           in.EnableTokenRevocation,
		AllowedOAuthFlowsUserPoolClient: in.AllowedOAuthFlowsUserPoolClient,
		AccessTokenValidity:             in.AccessTokenValidity,
		IDTokenValidity:                 in.IDTokenValidity,
		RefreshTokenValidity:            in.RefreshTokenValidity,
		TokenValidityUnits:              in.TokenValidityUnits,
	}

	client, err := h.Backend.UpdateUserPoolClientWithOpts(in.UserPoolID, in.ClientID, in.ClientName, opts)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolClientWithOptsOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

func (h *Handler) handleDescribeUserPoolClientAccurate(
	_ context.Context,
	in *describeUserPoolClientAccurateInput,
) (*describeUserPoolClientAccurateOutput, error) {
	client, err := h.Backend.DescribeUserPoolClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolClientAccurateOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

func (h *Handler) handleListUserPoolClientsAccurate(
	_ context.Context,
	in *listUserPoolClientsAccurateInput,
) (*listUserPoolClientsAccurateOutput, error) {
	clients, err := h.Backend.ListUserPoolClients(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	items := make([]clientDataAccurate, 0, len(clients))
	for _, c := range clients {
		items = append(items, clientToAccurateData(c))
	}

	return &listUserPoolClientsAccurateOutput{UserPoolClients: items}, nil
}

func (h *Handler) handleDeleteUserPoolClientSecret(
	_ context.Context,
	in *deleteUserPoolClientSecretInput,
) (*deleteUserPoolClientSecretOutput, error) {
	if err := h.Backend.DeleteUserPoolClientSecret(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &deleteUserPoolClientSecretOutput{}, nil
}

func (h *Handler) handleListUserPoolClientSecrets(
	_ context.Context,
	in *listUserPoolClientSecretsInput,
) (*listUserPoolClientSecretsOutput, error) {
	secrets, err := h.Backend.ListUserPoolClientSecrets(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &listUserPoolClientSecretsOutput{Secrets: secrets}, nil
}

func (h *Handler) userPoolClientsOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserPoolClient":    service.WrapOp(h.handleCreateUserPoolClient),
		"DescribeUserPoolClient":  service.WrapOp(h.handleDescribeUserPoolClient),
		"ListUserPoolClients":     service.WrapOp(h.handleListUserPoolClients),
		"DeleteUserPoolClient":    service.WrapOp(h.handleDeleteUserPoolClient),
		"UpdateUserPoolClient":    service.WrapOp(h.handleUpdateUserPoolClient),
		"AddUserPoolClientSecret": service.WrapOp(h.handleAddUserPoolClientSecret),
	}
}

func (h *Handler) userPoolClientsOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DeleteUserPoolClientSecret": service.WrapOp(h.handleDeleteUserPoolClientSecret),
		"ListUserPoolClientSecrets":  service.WrapOp(h.handleListUserPoolClientSecrets),
	}
}

func (h *Handler) userPoolClientsOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateUserPoolClient:   wrapAccuracy(h.handleCreateUserPoolClientWithOpts),
		opUpdateUserPoolClient:   wrapAccuracy(h.handleUpdateUserPoolClientWithOpts),
		opDescribeUserPoolClient: wrapAccuracy(h.handleDescribeUserPoolClientAccurate),
		opListUserPoolClients:    wrapAccuracy(h.handleListUserPoolClientsAccurate),
	}
}
