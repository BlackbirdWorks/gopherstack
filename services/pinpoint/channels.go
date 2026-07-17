package pinpoint

import (
	"strings"
)

const (
	// ChannelTypeADM is the Amazon Device Messaging channel.
	ChannelTypeADM = "ADM"
	// ChannelTypeAPNS is the Apple Push Notification service channel.
	ChannelTypeAPNS = "APNS"
	// ChannelTypeAPNSSandbox is the APNS Sandbox channel.
	ChannelTypeAPNSSandbox = "APNS_SANDBOX"
	// ChannelTypeAPNSVoip is the APNS VoIP channel.
	ChannelTypeAPNSVoip = "APNS_VOIP"
	// ChannelTypeAPNSVoipSandbox is the APNS VoIP Sandbox channel.
	ChannelTypeAPNSVoipSandbox = "APNS_VOIP_SANDBOX"
	// ChannelTypeBaidu is the Baidu Cloud Push channel.
	ChannelTypeBaidu = "BAIDU"
	// ChannelTypeEmail is the Email channel.
	ChannelTypeEmail = "EMAIL"
	// ChannelTypeGCM is the Google Cloud Messaging / FCM channel.
	ChannelTypeGCM = "GCM"
	// ChannelTypeSMS is the SMS channel.
	ChannelTypeSMS = "SMS"
	// ChannelTypeVoice is the Voice channel.
	ChannelTypeVoice = "VOICE"
	// ChannelTypeInApp is the In-App channel.
	ChannelTypeInApp = "IN_APP"
	// ChannelTypeCustom is the Custom channel.
	ChannelTypeCustom = "CUSTOM"
	// ChannelTypePush is the generic push channel.
	ChannelTypePush = "PUSH"
)

// isValidEndpointChannelType reports whether ct is a valid ChannelType for UpdateEndpoint.
func isValidEndpointChannelType(ct string) bool {
	switch ct {
	case ChannelTypeADM, ChannelTypeAPNS, ChannelTypeAPNSSandbox,
		ChannelTypeAPNSVoip, ChannelTypeAPNSVoipSandbox,
		ChannelTypeBaidu, ChannelTypeEmail, ChannelTypeGCM,
		ChannelTypeSMS, ChannelTypeVoice, ChannelTypeInApp,
		ChannelTypeCustom, ChannelTypePush:
		return true
	}

	return false
}

// Channel represents a generic Pinpoint channel response.
type Channel struct {
	ExtraData         map[string]any `json:"ExtraData,omitempty"`
	ApplicationID     string         `json:"ApplicationId"`
	ChannelType       string         `json:"ChannelType"`
	Platform          string         `json:"Platform,omitempty"`
	CreationDate      string         `json:"CreationDate,omitempty"`
	LastModifiedDate  string         `json:"LastModifiedDate,omitempty"`
	Version           int            `json:"Version,omitempty"`
	MessagesPerSecond int            `json:"MessagesPerSecond,omitempty"`
	Enabled           bool           `json:"Enabled"`
	IsArchived        bool           `json:"IsArchived"`
	HasCredential     bool           `json:"HasCredential,omitempty"`
	HasTokenKey       bool           `json:"HasTokenKey,omitempty"`
}

// GetChannel retrieves or synthesises a channel response for an app.
func (b *InMemoryBackend) GetChannel(appID, channelType string) *Channel {
	b.mu.RLock("GetChannel")
	defer b.mu.RUnlock()

	key := appID + "/" + channelType
	if ch, ok := b.channels.Get(key); ok {
		cp := *ch
		cp.ExtraData = cloneAnyMap(ch.ExtraData)

		return &cp
	}

	return &Channel{
		ApplicationID: appID,
		ChannelType:   channelType,
		Platform:      strings.ToUpper(channelType),
		Enabled:       false,
		IsArchived:    false,
	}
}

// channelCredentialFlags derives HasCredential and HasTokenKey from extra channel data.
func channelCredentialFlags(extra map[string]any) (bool, bool) {
	if extra == nil {
		return false, false
	}

	cred := false

	for _, k := range []string{"ApiKey", "BundleId", "Certificate", "ClientId", "FromAddress"} {
		if v, ok := extra[k].(string); ok && v != "" {
			cred = true

			break
		}
	}

	tokenKey := false

	if v, ok := extra["TokenKey"].(string); ok && v != "" {
		tokenKey = true
	}

	return cred, tokenKey
}

// UpsertChannel creates or updates a channel for an app with type-specific data.
func (b *InMemoryBackend) UpsertChannel(
	appID, channelType string,
	enabled bool,
	extra map[string]any,
) *Channel {
	b.mu.Lock("UpsertChannel")
	defer b.mu.Unlock()

	key := appID + "/" + channelType

	existing, _ := b.channels.Get(key)

	now := nowRFC3339()
	version := 1

	if existing != nil {
		version = existing.Version + 1
	}

	hasCredential, hasTokenKey := channelCredentialFlags(extra)

	creationDate := now

	if existing != nil && existing.CreationDate != "" {
		creationDate = existing.CreationDate
	}

	ch := &Channel{
		ApplicationID:    appID,
		ChannelType:      channelType,
		Platform:         strings.ToUpper(channelType),
		Enabled:          enabled,
		HasCredential:    hasCredential,
		HasTokenKey:      hasTokenKey,
		Version:          version,
		CreationDate:     creationDate,
		LastModifiedDate: now,
		ExtraData:        cloneAnyMap(extra),
	}

	b.channels.Put(ch)

	cp := *ch
	cp.ExtraData = cloneAnyMap(ch.ExtraData)

	return &cp
}

// DeleteChannel removes a channel for an app.
func (b *InMemoryBackend) DeleteChannel(appID, channelType string) *Channel {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	key := appID + "/" + channelType
	ch, _ := b.channels.Get(key)

	if ch == nil {
		ch = &Channel{ApplicationID: appID, ChannelType: channelType}
	}

	b.channels.Delete(key)

	cp := *ch
	cp.ExtraData = cloneAnyMap(ch.ExtraData)

	return &cp
}

// GetAllChannels returns all channels for an app.
func (b *InMemoryBackend) GetAllChannels(appID string) map[string]*Channel {
	b.mu.RLock("GetAllChannels")
	defer b.mu.RUnlock()

	result := make(map[string]*Channel)

	for _, ch := range b.channels.All() {
		if ch.ApplicationID == appID {
			cp := *ch
			cp.ExtraData = cloneAnyMap(ch.ExtraData)
			result[ch.ApplicationID+"/"+ch.ChannelType] = &cp
		}
	}

	return result
}
