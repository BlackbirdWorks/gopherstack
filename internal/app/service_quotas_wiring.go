package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

type quotaCounter func(a *quotaUsageAdapter, account, region string) (int, bool)

// quotaCounters maps "service/quota" to the live resource count behind it.
var quotaCounters = map[string]quotaCounter{ //nolint:gochecknoglobals // immutable dispatch table
	"s3/L-DC2B2D3D":       func(a *quotaUsageAdapter, acct, _ string) (int, bool) { return a.s3BucketCount(acct) },
	"dynamodb/L-F98FE922": func(a *quotaUsageAdapter, acct, rgn string) (int, bool) { return a.ddbTableCount(acct, rgn) },
	"iam/L-F4A5425F":      func(a *quotaUsageAdapter, _, _ string) (int, bool) { return a.iamCount(iamRoles) },
	"iam/L-F55AF5E4":      func(a *quotaUsageAdapter, _, _ string) (int, bool) { return a.iamCount(iamUsers) },
	"iam/L-BF35879D":      func(a *quotaUsageAdapter, _, _ string) (int, bool) { return a.iamCount(iamGroups) },
	"vpc/L-F678F1CE":      func(a *quotaUsageAdapter, _, rgn string) (int, bool) { return a.ec2Count(rgn, false) },
	"ec2/L-0263D0A3":      func(a *quotaUsageAdapter, _, rgn string) (int, bool) { return a.ec2Count(rgn, true) },
}

type iamKind int

const (
	iamRoles iamKind = iota
	iamUsers
	iamGroups
)

// quotaUsageAdapter reads live resource counts from the emulated backends for Service Quotas.
type quotaUsageAdapter struct {
	s3  *s3backend.InMemoryBackend
	ddb *ddbbackend.InMemoryDB
	iam *iambackend.InMemoryBackend
	ec2 *ec2backend.Handler
}

// QuotaUsage implements servicequotas.UsageSource.
func (a *quotaUsageAdapter) QuotaUsage(account, region, serviceCode, quotaCode string) (float64, bool) {
	count, ok := quotaCounters[serviceCode+"/"+quotaCode]
	if !ok {
		return 0, false
	}

	n, tracked := count(a, account, region)

	return float64(n), tracked
}

func (a *quotaUsageAdapter) s3BucketCount(account string) (int, bool) {
	if a.s3 == nil {
		return 0, false
	}

	return a.s3.BucketCount(account), true
}

func (a *quotaUsageAdapter) ddbTableCount(account, region string) (int, bool) {
	if a.ddb == nil {
		return 0, false
	}

	return a.ddb.TableCount(account, region), true
}

func (a *quotaUsageAdapter) iamCount(kind iamKind) (int, bool) {
	if a.iam == nil {
		return 0, false
	}

	switch kind {
	case iamRoles:
		return a.iam.RoleCount(), true
	case iamUsers:
		return a.iam.UserCount(), true
	default:
		return a.iam.GroupCount(), true
	}
}

type ec2Counter interface {
	VpcCount() int
	AddressCount() int
}

func (a *quotaUsageAdapter) ec2Count(region string, addresses bool) (int, bool) {
	if a.ec2 == nil {
		return 0, false
	}

	bk, ok := a.ec2.BackendFor(region).(ec2Counter)
	if !ok {
		return 0, false
	}

	if addresses {
		return bk.AddressCount(), true
	}

	return bk.VpcCount(), true
}

// quotaSNSNotifier publishes auto-management notifications to SNS.
type quotaSNSNotifier struct{ sns *snsbackend.InMemoryBackend }

// Notify implements servicequotas.Notifier.
func (n quotaSNSNotifier) Notify(ctx context.Context, topicArn, subject, message string) error {
	_, err := n.sns.PublishContext(ctx, topicArn, message, subject, "", nil)

	return err
}

func newQuotaUsageAdapter(byName map[string]service.Registerable) *quotaUsageAdapter {
	a := &quotaUsageAdapter{}

	if h, ok := byName["S3"].(*s3backend.S3Handler); ok {
		a.s3, _ = h.Backend.(*s3backend.InMemoryBackend)
	}

	if h, ok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); ok {
		a.ddb, _ = h.Backend.(*ddbbackend.InMemoryDB)
	}

	if h, ok := byName["IAM"].(*iambackend.Handler); ok {
		a.iam, _ = h.Backend.(*iambackend.InMemoryBackend)
	}

	a.ec2, _ = byName["EC2"].(*ec2backend.Handler)

	return a
}

// wireServiceQuotas feeds Service Quotas utilization from the live backends and SNS notifications;
// with enforce set, the owning services also reject creates beyond the applied quota.
func wireServiceQuotas(enforce bool, byName map[string]service.Registerable) {
	sq, ok := byName["ServiceQuotas"].(*servicequotasbackend.Handler)
	if !ok {
		return
	}

	adapter := newQuotaUsageAdapter(byName)
	sq.Backend.SetUsageSource(adapter)

	if h, hok := byName["SNS"].(*snsbackend.Handler); hok {
		if bk, bok := h.Backend.(*snsbackend.InMemoryBackend); bok {
			sq.Backend.SetNotifier(quotaSNSNotifier{sns: bk})
		}
	}

	if enforce {
		enforceServiceQuotas(sq.Backend.QuotaLimit, adapter)
	}
}

func enforceServiceQuotas(limit quotalimit.Func, a *quotaUsageAdapter) {
	if a.s3 != nil {
		a.s3.SetQuotaLimiter(limit)
	}

	if a.ddb != nil {
		a.ddb.SetQuotaLimiter(limit)
	}

	if a.iam != nil {
		a.iam.SetQuotaLimiter(limit)
	}

	if a.ec2 != nil {
		a.ec2.SetQuotaLimiter(limit)
	}
}
