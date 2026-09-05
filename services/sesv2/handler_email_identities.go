package sesv2

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---- request types ----

type createEmailIdentityInput struct {
	EmailIdentity        string     `json:"EmailIdentity"`
	ConfigurationSetName string     `json:"ConfigurationSetName"`
	Tags                 []tagEntry `json:"Tags"`
}

type createEmailIdentityPolicyInput struct {
	Policy string `json:"Policy"`
}

// ---- response types ----

type dkimAttributesOutput struct {
	Status                  string   `json:"Status,omitempty"`
	SigningAttributesOrigin string   `json:"SigningAttributesOrigin,omitempty"`
	Tokens                  []string `json:"Tokens,omitempty"`
	SigningEnabled          bool     `json:"SigningEnabled"`
}

type mailFromAttributesOutput struct {
	MailFromDomain       string `json:"MailFromDomain,omitempty"`
	MailFromDomainStatus string `json:"MailFromDomainStatus,omitempty"`
	BehaviorOnMxFailure  string `json:"BehaviorOnMxFailure,omitempty"`
}

type createEmailIdentityOutput struct {
	DkimAttributes     *dkimAttributesOutput `json:"DkimAttributes,omitempty"`
	IdentityType       string                `json:"IdentityType"`
	VerifiedForSending bool                  `json:"VerifiedForSendingStatus"`
}

type getEmailIdentityOutput struct {
	DkimAttributes       *dkimAttributesOutput     `json:"DkimAttributes,omitempty"`
	MailFromAttributes   *mailFromAttributesOutput `json:"MailFromAttributes,omitempty"`
	Policies             map[string]string         `json:"Policies,omitempty"`
	EmailIdentity        string                    `json:"EmailIdentity"`
	IdentityType         string                    `json:"IdentityType"`
	ConfigurationSetName string                    `json:"ConfigurationSetName,omitempty"`
	VerificationStatus   string                    `json:"VerificationStatus,omitempty"`
	Tags                 []tagEntry                `json:"Tags,omitempty"`
	VerifiedForSending   bool                      `json:"VerifiedForSendingStatus"`
	FeedbackForwarding   bool                      `json:"FeedbackForwardingStatus"`
}

type emailIdentitySummary struct {
	IdentityName       string `json:"IdentityName"`
	IdentityType       string `json:"IdentityType"`
	VerificationStatus string `json:"VerificationStatus,omitempty"`
	SendingEnabled     bool   `json:"SendingEnabled"`
}

type listEmailIdentitiesOutput struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	EmailIdentities []emailIdentitySummary `json:"EmailIdentities"`
}

// ---- action handlers ----

func (h *Handler) handleCreateEmailIdentity(c *echo.Context) (any, error) {
	var in createEmailIdentityInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	tags := tagsFromEntries(in.Tags)

	ei, err := h.Backend.CreateEmailIdentity(in.EmailIdentity, in.ConfigurationSetName, tags)
	if err != nil {
		return nil, err
	}

	out := &createEmailIdentityOutput{
		IdentityType:       ei.IdentityType,
		VerifiedForSending: ei.VerifiedForSending,
	}

	if len(ei.DkimTokens) > 0 {
		out.DkimAttributes = &dkimAttributesOutput{
			SigningEnabled:          ei.DkimSigningEnabled,
			Status:                  ei.DkimStatus,
			SigningAttributesOrigin: "AWS_SES",
			Tokens:                  ei.DkimTokens,
		}
	}

	return out, nil
}

func (h *Handler) handleGetEmailIdentity(identity string) (any, error) {
	ei, err := h.Backend.GetEmailIdentity(identity)
	if err != nil {
		return nil, err
	}

	policies, _ := h.Backend.GetEmailIdentityPolicies(identity)

	out := &getEmailIdentityOutput{
		EmailIdentity:        ei.Identity,
		IdentityType:         ei.IdentityType,
		VerifiedForSending:   ei.VerifiedForSending,
		FeedbackForwarding:   ei.FeedbackForwarding,
		ConfigurationSetName: ei.ConfigurationSetName,
		VerificationStatus:   ei.VerificationStatus,
		Policies:             policies,
		Tags:                 tagsToEntries(ei.Tags),
	}

	out.DkimAttributes = &dkimAttributesOutput{
		SigningEnabled:          ei.DkimSigningEnabled,
		Status:                  ei.DkimStatus,
		SigningAttributesOrigin: "AWS_SES",
		Tokens:                  ei.DkimTokens,
	}

	if ei.MailFromDomain != "" {
		out.MailFromAttributes = &mailFromAttributesOutput{
			MailFromDomain:       ei.MailFromDomain,
			MailFromDomainStatus: ei.MailFromDomainStatus,
			BehaviorOnMxFailure:  ei.BehaviorOnMxFailure,
		}
	}

	return out, nil
}

func (h *Handler) handleListEmailIdentities(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListEmailIdentities(nextToken, 0)

	items := make([]emailIdentitySummary, 0, len(pg.Data))

	for _, ei := range pg.Data {
		items = append(items, emailIdentitySummary{
			IdentityName:       ei.Identity,
			IdentityType:       ei.IdentityType,
			VerificationStatus: ei.VerificationStatus,
			SendingEnabled:     ei.VerifiedForSending,
		})
	}

	return &listEmailIdentitiesOutput{
		EmailIdentities: items,
		NextToken:       pg.Next,
	}
}

func (h *Handler) handleDeleteEmailIdentity(identity string) (any, error) {
	if err := h.Backend.DeleteEmailIdentity(identity); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	// Extract policyName from the URL: .../identities/{identity}/policies/{policyName}
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]
	if decoded, decErr := url.PathUnescape(policyName); decErr == nil {
		policyName = decoded
	}

	var in createEmailIdentityPolicyInput

	if decErr := json.NewDecoder(c.Request().Body).Decode(&in); decErr != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, decErr.Error())
	}

	if backErr := h.Backend.CreateEmailIdentityPolicy(identity, policyName, in.Policy); backErr != nil {
		return nil, backErr
	}

	return &emptyDeleteOutput{}, nil
}

// email identity policy handlers

func (h *Handler) handleGetEmailIdentityPolicies(identity string) (any, error) {
	policies, err := h.Backend.GetEmailIdentityPolicies(identity)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Policies": policies}, nil
}

func (h *Handler) handleDeleteEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]

	if decoded, err := url.PathUnescape(policyName); err == nil {
		policyName = decoded
	}

	if err := h.Backend.DeleteEmailIdentityPolicy(identity, policyName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleUpdateEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]

	if decoded, err := url.PathUnescape(policyName); err == nil {
		policyName = decoded
	}

	var in createEmailIdentityPolicyInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateEmailIdentityPolicy(identity, policyName, in.Policy); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// email identity attribute handlers

func (h *Handler) handlePutEmailIdentityConfigurationSetAttributes(
	c *echo.Context,
	identity string,
) (any, error) {
	var in struct {
		ConfigurationSetName string `json:"ConfigurationSetName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityConfigurationSetAttributes(
		identity,
		in.ConfigurationSetName,
	)
}

func (h *Handler) handlePutEmailIdentityDkimAttributes(
	c *echo.Context,
	identity string,
) (any, error) {
	var in struct {
		SigningEnabled bool `json:"SigningEnabled"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityDkimAttributes(
		identity,
		in.SigningEnabled,
	)
}

type putEmailIdentityDkimSigningAttributesOutput struct {
	DkimStatus string   `json:"DkimStatus"`
	DkimTokens []string `json:"DkimTokens,omitempty"`
}

func (h *Handler) handlePutEmailIdentityDkimSigningAttributes(identity string) (any, error) {
	ei, err := h.Backend.PutEmailIdentityDkimSigningAttributes(identity)
	if err != nil {
		return nil, err
	}

	return &putEmailIdentityDkimSigningAttributesOutput{
		DkimStatus: ei.DkimStatus,
		DkimTokens: ei.DkimTokens,
	}, nil
}

func (h *Handler) handlePutEmailIdentityFeedbackAttributes(
	c *echo.Context,
	identity string,
) (any, error) {
	var in struct {
		EmailForwardingEnabled bool `json:"EmailForwardingEnabled"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityFeedbackAttributes(
		identity,
		in.EmailForwardingEnabled,
	)
}

func (h *Handler) handlePutEmailIdentityMailFromAttributes(
	c *echo.Context,
	identity string,
) (any, error) {
	var in struct {
		MailFromDomain      string `json:"MailFromDomain"`
		BehaviorOnMxFailure string `json:"BehaviorOnMxFailure"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityMailFromAttributes(
		identity,
		in.MailFromDomain,
		in.BehaviorOnMxFailure,
	)
}
