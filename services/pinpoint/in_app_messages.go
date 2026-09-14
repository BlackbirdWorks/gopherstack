package pinpoint

import "sort"

// GetInAppMessages returns the in-app messages targeted for an endpoint.
//
// Real AWS selects campaigns by evaluating whether the endpoint matches each
// campaign's segment (dimension-based audience matching). gopherstack has no
// segment/dimension evaluation engine anywhere in this service, so this
// returns every non-paused campaign in the app that targets an in-app
// template, regardless of endpointID -- a structural limitation, not a
// fabrication: every returned campaign and its message content is real,
// unlike the prior version, which manufactured a fake "campaign" out of
// every in-app template that existed anywhere in the account.
func (b *InMemoryBackend) GetInAppMessages(appID, _ string) (*inAppMessagesResponse, error) {
	b.mu.RLock("GetInAppMessages")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	var campaigns []*Campaign

	for _, c := range b.campaigns.All() {
		if c.ApplicationID == appID && !c.IsPaused {
			campaigns = append(campaigns, c)
		}
	}

	sort.Slice(campaigns, func(i, j int) bool { return campaigns[i].ID < campaigns[j].ID })

	out := make([]inAppMessageCampaign, 0, len(campaigns))

	for _, c := range campaigns {
		templateName, ok := campaignInAppTemplateName(c)
		if !ok {
			continue
		}

		tmpl, found := b.inAppTemplates.Get(templateName)
		if !found {
			continue
		}

		out = append(out, buildInAppMessageCampaign(c, tmpl))
	}

	return &inAppMessagesResponse{InAppMessageCampaigns: out}, nil
}

// campaignInAppTemplateName extracts TemplateConfiguration.InAppTemplate.Name
// from a campaign, real AWS's way of associating a campaign with the
// InAppTemplate it renders (pinpoint@v1.42.4 types/types.go's
// TemplateConfiguration.InAppTemplate).
func campaignInAppTemplateName(c *Campaign) (string, bool) {
	raw, ok := c.TemplateConfiguration["InAppTemplate"]
	if !ok {
		return "", false
	}

	m, ok := raw.(map[string]any)
	if !ok {
		return "", false
	}

	name, ok := m["Name"].(string)

	return name, ok && name != ""
}

func buildInAppMessageCampaign(c *Campaign, tmpl *InAppTemplate) inAppMessageCampaign {
	out := inAppMessageCampaign{
		CampaignID: c.ID,
		Priority:   c.Priority,
		InAppMessage: &inAppMessage{
			Content:      cloneContentSlice(tmpl.Content),
			CustomConfig: nonNilTagsCopy(tmpl.CustomConfig),
			Layout:       tmpl.Layout,
		},
	}

	if daily, ok := intFromAny(c.Limits["Daily"]); ok {
		out.DailyCap = daily
	}

	if session, ok := intFromAny(c.Limits["Session"]); ok {
		out.SessionCap = session
	}

	if total, ok := intFromAny(c.Limits["Total"]); ok {
		out.TotalCap = total
	}

	out.Schedule = buildInAppCampaignSchedule(c.Schedule)

	return out
}

func buildInAppCampaignSchedule(schedule map[string]any) *inAppCampaignSchedule {
	endDate, _ := schedule["EndTime"].(string)
	eventFilter, _ := schedule["EventFilter"].(map[string]any)
	quietTime, _ := schedule["QuietTime"].(map[string]any)

	if endDate == "" && eventFilter == nil && quietTime == nil {
		return nil
	}

	return &inAppCampaignSchedule{
		EndDate:     endDate,
		EventFilter: cloneAnyMap(eventFilter),
		QuietTime:   cloneAnyMap(quietTime),
	}
}

// intFromAny converts a JSON-decoded numeric value (float64 from
// map[string]any, or a plain int set internally) to int.
func intFromAny(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	default:
		return 0, false
	}
}
