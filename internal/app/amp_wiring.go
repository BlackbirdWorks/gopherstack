package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
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
		wireAMPKeysAndLogs(ampH, byName)
	}
}

// ampKeys answers KMS key lookups for CreateWorkspace kmsKeyArn.
type ampKeys struct{ kms kmsbackend.StorageBackend }

func (k ampKeys) KMSKeyState(ctx context.Context, keyARN string) (bool, bool) {
	out, err := k.kms.DescribeKey(ctx, &kmsbackend.DescribeKeyInput{KeyID: keyARN})
	if err != nil {
		return false, false
	}

	return true, out.KeyMetadata.Enabled && out.KeyMetadata.KeyState == kmsbackend.KeyStateEnabled
}

// ampLogs checks log groups and delivers workspace log lines in the in-process CloudWatch Logs.
type ampLogs struct{ logs cwlogsbackend.StorageBackend }

func (l ampLogs) LogGroupExists(ctx context.Context, region, group string) bool {
	groups, _, err := l.logs.DescribeLogGroups(cwlogsbackend.WithRegion(ctx, region), group, "", "", 0)
	if err != nil {
		return false
	}

	for _, g := range groups {
		if g.LogGroupName == group {
			return true
		}
	}

	return false
}

func (l ampLogs) PutLogLine(ctx context.Context, region, group, stream, message string, at time.Time) error {
	rctx := cwlogsbackend.WithRegion(ctx, region)

	if _, err := l.logs.CreateLogStream(rctx, group, stream); err != nil &&
		!errors.Is(err, cwlogsbackend.ErrLogStreamAlreadyExist) {
		return fmt.Errorf("amp log stream: %w", err)
	}

	_, err := l.logs.PutLogEvents(rctx, group, stream, "",
		[]cwlogsbackend.InputLogEvent{{Message: message, Timestamp: at.UnixMilli()}})
	if err != nil {
		return fmt.Errorf("amp log events: %w", err)
	}

	return nil
}

func wireAMPKeysAndLogs(ampH *ampbackend.Handler, byName map[string]service.Registerable) {
	if kmsH, ok := byName["KMS"].(*kmsbackend.Handler); ok {
		ampH.Backend.SetKeyChecker(ampKeys{kms: kmsH.Backend})
	}

	if logsH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); ok {
		ampH.Backend.SetLogSink(ampLogs{logs: logsH.Backend})
	}
}

// ampPolicyEvaluator runs a workspace resource policy through the IAM evaluator for the caller account.
func ampPolicyEvaluator(doc, action, resource, callerAccount string) (bool, bool) {
	res := iambackend.EvaluatePolicies([]string{doc}, action, resource,
		iambackend.ConditionContext{PrincipalAccount: callerAccount})

	return res == iambackend.EvalAllow, res == iambackend.EvalExplicitDeny
}

// wireAMPDataPlaneAuth makes the Prometheus endpoints require aps-scoped SigV4 when --validate-sigv4 is on.
func wireAMPDataPlaneAuth(appCtx *service.AppContext, byName map[string]service.Registerable) {
	cli, ok := appCtx.Config.(*CLI)
	if !ok || !cli.ValidateSigV4 {
		return
	}

	if ampH, aok := byName["AMP"].(*ampbackend.Handler); aok {
		ampH.Backend.SetDataPlaneAuth(&ampbackend.DataPlaneAuth{Secret: cli.SigV4Secret, Evaluate: ampPolicyEvaluator})
	}
}
