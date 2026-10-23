package app

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

const ampAttrString = "String"

// ampSNSPublisher delivers Managed Service for Prometheus alert manager notifications to the SNS emulator.
type ampSNSPublisher struct{ sns *snsbackend.InMemoryBackend }

func (p ampSNSPublisher) Publish(_ context.Context, m ampbackend.SNSMessage) error {
	if m.PhoneNumber != "" {
		if _, err := p.sns.PublishSMS(m.PhoneNumber, m.Message); err != nil {
			return fmt.Errorf("sns sms publish: %w", err)
		}

		return nil
	}

	attrs := make(map[string]snsbackend.MessageAttribute, len(m.Attributes))
	for k, v := range m.Attributes {
		attrs[k] = snsbackend.MessageAttribute{DataType: ampAttrString, StringValue: v}
	}

	target := m.TopicARN
	if target == "" {
		target = m.TargetARN
	}

	if _, err := p.sns.Publish(target, m.Message, m.Subject, "", attrs); err != nil {
		return fmt.Errorf("sns publish %s: %w", target, err)
	}

	return nil
}

// wireAMPAlertNotifications routes alert manager notifications to the SNS emulator.
func wireAMPAlertNotifications(ampH *ampbackend.Handler, byName map[string]service.Registerable) {
	snsH, ok := byName["SNS"].(*snsbackend.Handler)
	if !ok {
		return
	}

	if bk, bok := snsH.Backend.(*snsbackend.InMemoryBackend); bok {
		ampH.Backend.SetSNSPublisher(ampSNSPublisher{sns: bk})
	}
}

// ampClusterResolver answers "does this EKS cluster exist" for scraper source validation.
type ampClusterResolver struct{ eks *eksbackend.Handler }

func (r ampClusterResolver) ClusterExists(region, name string) bool {
	b := r.eks.BackendFor(region)
	if b == nil {
		return false
	}

	_, err := b.DescribeCluster(name)

	return err == nil
}

// wireAMPScraperSources lets Managed Service for Prometheus scrapers validate their EKS source cluster.
func wireAMPScraperSources(byName map[string]service.Registerable) {
	ampH, aok := byName["AMP"].(*ampbackend.Handler)
	eksH, eok := byName["EKS"].(*eksbackend.Handler)

	if aok && eok {
		ampH.Backend.SetClusterResolver(ampClusterResolver{eks: eksH})
	}

	if aok {
		wireAMPAlertNotifications(ampH, byName)
	}
}
