package kinesisvideo

import (
	"fmt"
	"time"
)

const (
	storageTierHot  = "HOT"
	storageTierWarm = "WARM"

	mediaStorageEnabled  = "ENABLED"
	mediaStorageDisabled = "DISABLED"

	protocolWSS    = "WSS"
	protocolHTTPS  = "HTTPS"
	protocolWebRTC = "WEBRTC"
)

// DescribeStreamStorageConfiguration returns the stream, whose DefaultStorageTier
// is its storage configuration (HOT when never set).
func (b *InMemoryBackend) DescribeStreamStorageConfiguration(name, streamARN string) (*Stream, error) {
	b.mu.Lock("DescribeStreamStorageConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return nil, err
	}

	cp := s.clone()
	if cp.DefaultStorageTier == "" {
		cp.DefaultStorageTier = storageTierHot
	}

	return cp, nil
}

// UpdateStreamStorageConfiguration sets a stream's default storage tier under optimistic lock.
func (b *InMemoryBackend) UpdateStreamStorageConfiguration(
	name, streamARN, currentVersion, defaultStorageTier string,
) error {
	if defaultStorageTier != storageTierHot && defaultStorageTier != storageTierWarm {
		return ErrValidation
	}

	b.mu.Lock("UpdateStreamStorageConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	s, err := b.resolveStreamLocked(name, streamARN)
	if err != nil {
		return err
	}

	if s.Version != currentVersion {
		return ErrVersionMismatch
	}

	s.DefaultStorageTier = defaultStorageTier
	s.Version = newVersion()

	return nil
}

// DescribeMediaStorageConfiguration returns a channel's media storage configuration, or nil when unset.
func (b *InMemoryBackend) DescribeMediaStorageConfiguration(name, channelARN string) (*MediaStorage, error) {
	b.mu.Lock("DescribeMediaStorageConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked(name, channelARN)
	if err != nil {
		return nil, err
	}

	return ptrCopy(c.MediaStorage), nil
}

// UpdateMediaStorageConfiguration enables or disables storing a channel's media in a stream.
func (b *InMemoryBackend) UpdateMediaStorageConfiguration(channelARN string, cfg MediaStorage) error {
	if cfg.Status != mediaStorageEnabled && cfg.Status != mediaStorageDisabled {
		return ErrValidation
	}

	b.mu.Lock("UpdateMediaStorageConfiguration")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked("", channelARN)
	if err != nil {
		return err
	}

	if cfg.Status == mediaStorageEnabled {
		name, ok := streamNameFromARN(cfg.StreamARN)
		if !ok {
			return ErrValidation
		}

		s, found := b.streams.Get(name)
		if !found || s.ARN != cfg.StreamARN {
			return ErrStreamNotFound
		}

		if s.DataRetentionInHours == 0 {
			return ErrNoDataRetention
		}
	}

	c.MediaStorage = &MediaStorage{Status: cfg.Status, StreamARN: cfg.StreamARN}

	return nil
}

// GetSignalingChannelEndpoint returns emulator-hosted, AWS-shaped endpoints of a
// channel for each requested protocol.
func (b *InMemoryBackend) GetSignalingChannelEndpoint(
	channelARN, role, region string,
	protocols []string,
) ([]ChannelEndpoint, error) {
	if channelARN == "" {
		return nil, ErrValidation
	}

	if role != "" && role != "MASTER" && role != "VIEWER" {
		return nil, ErrValidation
	}

	b.mu.Lock("GetSignalingChannelEndpoint")
	defer b.mu.Unlock()

	b.sweepLocked(time.Now())

	c, err := b.resolveChannelLocked("", channelARN)
	if err != nil {
		return nil, err
	}

	out := make([]ChannelEndpoint, 0, len(protocols))

	for _, p := range protocols {
		host := fmt.Sprintf("%s.kinesisvideo.%s.amazonaws.com", shortHash(c.ARN+p+role), region)

		switch p {
		case protocolWSS:
			out = append(out, ChannelEndpoint{Protocol: p, Endpoint: "wss://v-" + host})
		case protocolHTTPS:
			out = append(out, ChannelEndpoint{Protocol: p, Endpoint: "https://r-" + host})
		case protocolWebRTC:
			out = append(out, ChannelEndpoint{Protocol: p, Endpoint: host})
		default:
			return nil, ErrValidation
		}
	}

	return out, nil
}
