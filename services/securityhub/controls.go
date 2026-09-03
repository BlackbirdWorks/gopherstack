package securityhub

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) securityControlARN(id string) string {
	return arn.Build("securityhub", b.region, "", fmt.Sprintf("security-control/%s", id))
}

var knownSecurityControls = []SecurityControlDefinition{ //nolint:gochecknoglobals // read-only lookup data
	{
		SecurityControlID:         "CloudTrail.1",
		Title:                     "CloudTrail should be enabled and configured with at least one multi-Region trail",
		Description:               "This control checks that there is at least one multi-region AWS CloudTrail trail.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/cloudtrail-controls.html",
		SeverityRating:            severityLabelHigh,
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
	{
		SecurityControlID: "IAM.1",
		Title:             "IAM policies should not allow full administrative privileges",
		Description: "This control checks whether the default version of IAM policies have " +
			"administrator access.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/iam-controls.html",
		SeverityRating:            severityLabelHigh,
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
	{
		SecurityControlID:         "S3.1",
		Title:                     "S3 Block Public Access setting should be enabled",
		Description:               "This control checks whether the S3 block public access setting is enabled.",
		RemediationURL:            "https://docs.aws.amazon.com/securityhub/latest/userguide/s3-controls.html",
		SeverityRating:            severityLabelMedium,
		CurrentRegionAvailability: statusAvailable,
		CustomizableProperties:    []string{},
		ParameterDefinitions:      map[string]any{},
	},
}

func (b *InMemoryBackend) GetSecurityControlDefinition(securityControlID string) (*SecurityControlDefinition, error) {
	b.mu.RLock("GetSecurityControlDefinition")
	defer b.mu.RUnlock()

	for i := range knownSecurityControls {
		if knownSecurityControls[i].SecurityControlID == securityControlID {
			cp := knownSecurityControls[i]

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: security control %s", ErrNotFound, securityControlID)
}

func (b *InMemoryBackend) ListSecurityControlDefinitions(
	_, nextToken string,
	maxResults int,
) ([]*SecurityControlDefinition, string) {
	b.mu.RLock("ListSecurityControlDefinitions")
	defer b.mu.RUnlock()

	results := make([]*SecurityControlDefinition, len(knownSecurityControls))
	for i := range knownSecurityControls {
		cp := knownSecurityControls[i]
		results[i] = &cp
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []*SecurityControlDefinition{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

func (b *InMemoryBackend) BatchGetSecurityControls(securityControlIDs []string) ([]*SecurityControl, []map[string]any) {
	b.mu.RLock("BatchGetSecurityControls")
	defer b.mu.RUnlock()

	var controls []*SecurityControl
	var unprocessed []map[string]any

	for _, id := range securityControlIDs {
		var def *SecurityControlDefinition

		for i := range knownSecurityControls {
			if knownSecurityControls[i].SecurityControlID == id {
				cp := knownSecurityControls[i]
				def = &cp

				break
			}
		}

		if def == nil {
			unprocessed = append(unprocessed, map[string]any{
				keySecurityControlID: id,
				keyErrorCode:         errCodeUnprocessedInvalidInput,
				keyErrorMessage:      "Security control not found",
			})

			continue
		}

		params := b.controlParams[id]
		if params == nil {
			params = map[string]any{}
		}

		controls = append(controls, &SecurityControl{
			SecurityControlID:     def.SecurityControlID,
			SecurityControlArn:    b.securityControlARN(def.SecurityControlID),
			Title:                 def.Title,
			Description:           def.Description,
			RemediationURL:        def.RemediationURL,
			SeverityRating:        def.SeverityRating,
			SecurityControlStatus: statusEnabled,
			UpdateStatus:          statusReady,
			Parameters:            params,
		})
	}

	if controls == nil {
		controls = []*SecurityControl{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return controls, unprocessed
}

func (b *InMemoryBackend) UpdateSecurityControl(
	securityControlID string,
	parameters map[string]any,
	_ string,
) error {
	b.mu.Lock("UpdateSecurityControl")
	defer b.mu.Unlock()

	b.controlParams[securityControlID] = parameters

	return nil
}
