package app

import (
	asbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// counted adapts a typed handler read to a quotaCounter; a quota is untracked while its service is not wired.
func counted[H any](read func(h H, account, region string) int) quotaCounter {
	return func(a *quotaUsageAdapter, account, region string) (int, bool) {
		for _, r := range a.byName {
			if h, ok := r.(H); ok {
				return read(h, account, region), true
			}
		}

		return 0, false
	}
}

func eventsCounted(read func(c ebbackend.QuotaCounter, region string) int) quotaCounter {
	return counted(func(h *ebbackend.Handler, _, region string) int {
		if c := h.QuotaCounts(); c != nil {
			return read(c, region)
		}

		return 0
	})
}

func route53Counted(read func(c route53backend.QuotaCounter) int) quotaCounter {
	return counted(func(h *route53backend.Handler, _, _ string) int {
		if c := h.QuotaCounts(); c != nil {
			return read(c)
		}

		return 0
	})
}

type ec2QuotaCounter interface{ QuotaCount(code string) (int, bool) }

func ec2Counted(code string) quotaCounter {
	return counted(func(h *ec2backend.Handler, _, region string) int {
		if c, ok := h.BackendFor(region).(ec2QuotaCounter); ok {
			n, _ := c.QuotaCount(code)

			return n
		}

		return 0
	})
}

func serviceQuotaCounters() map[string]quotaCounter {
	return map[string]quotaCounter{
		"logs/L-C7B9AAAB": counted(func(h *cwlogsbackend.Handler, _, rgn string) int { return h.LogGroupCount(rgn) }),

		"kms/L-C2F1777E": counted(func(h *kmsbackend.Handler, _, rgn string) int { return h.KeyCount(rgn) }),
		"kms/L-340F62FB": counted(func(h *kmsbackend.Handler, _, rgn string) int { return h.MaxAliasesPerKey(rgn) }),
		"kms/L-D594A657": counted(func(h *kmsbackend.Handler, _, rgn string) int { return h.MaxGrantsPerKey(rgn) }),

		"events/L-244521F2": eventsCounted(
			func(c ebbackend.QuotaCounter, rgn string) int { return c.MaxRulesPerBus(rgn) },
		),
		"events/L-658A4FD9": eventsCounted(
			func(c ebbackend.QuotaCounter, rgn string) int { return c.CustomBusCount(rgn) },
		),
		"events/L-595D6D42": eventsCounted(
			func(c ebbackend.QuotaCounter, rgn string) int { return c.ConnectionsInRegion(rgn) },
		),
		"events/L-EAC9A2AC": eventsCounted(
			func(c ebbackend.QuotaCounter, rgn string) int { return c.EndpointsInRegion(rgn) },
		),
		"events/L-FB1C3A6D": eventsCounted(
			func(c ebbackend.QuotaCounter, rgn string) int { return c.APIDestinationsInRegion(rgn) },
		),

		"states/L-B66B0F91": counted(
			func(h *sfnbackend.Handler, _, rgn string) int { return h.StateMachineCount(rgn) },
		),
		"states/L-A9562A73": counted(func(h *sfnbackend.Handler, _, rgn string) int { return h.ActivityCount(rgn) }),
		"states/L-6AFCB355": counted(
			func(h *sfnbackend.Handler, _, rgn string) int { return h.MaxVersionsPerStateMachine(rgn) },
		),
		"states/L-C1D5EBBB": counted(
			func(h *sfnbackend.Handler, _, rgn string) int { return h.MaxAliasesPerStateMachine(rgn) },
		),

		"route53/L-4EA4796A": route53Counted(func(c route53backend.QuotaCounter) int { return c.HostedZoneCount() }),
		"route53/L-ACB674F3": route53Counted(func(c route53backend.QuotaCounter) int { return c.HealthCheckCount() }),
		"route53/L-A72C7724": route53Counted(func(c route53backend.QuotaCounter) int { return c.DelegationSetCount() }),
		"route53/L-FC688E7C": route53Counted(func(c route53backend.QuotaCounter) int { return c.TrafficPolicyCount() }),
		"route53/L-AD61772F": route53Counted(
			func(c route53backend.QuotaCounter) int { return c.CidrCollectionCount() },
		),
		"route53/L-E209CC9F": route53Counted(
			func(c route53backend.QuotaCounter) int { return c.MaxRecordsPerHostedZone() },
		),

		"vpc/L-407747CB": ec2Counted("L-407747CB"),
		"vpc/L-A4707A72": ec2Counted("L-A4707A72"),
		"vpc/L-FE5A380F": ec2Counted("L-FE5A380F"),
		"vpc/L-DF5E4CA3": ec2Counted("L-DF5E4CA3"),
		"vpc/L-E79EC296": ec2Counted("L-E79EC296"),
		"vpc/L-0EA8095F": ec2Counted("L-0EA8095F"),
		"vpc/L-589F43AA": ec2Counted("L-589F43AA"),
		"vpc/L-93826ACB": ec2Counted("L-93826ACB"),
		"ec2/L-1216C47A": ec2Counted("L-1216C47A"),
		"ec2/L-DB2E81BA": ec2Counted("L-DB2E81BA"),
		"ec2/L-417A185B": ec2Counted("L-417A185B"),
		"ec2/L-74FC7D96": ec2Counted("L-74FC7D96"),
		"ec2/L-7295265B": ec2Counted("L-7295265B"),
		"ec2/L-1945791B": ec2Counted("L-1945791B"),
		"ec2/L-43DA4232": ec2Counted("L-43DA4232"),
		"ec2/L-34B43A08": ec2Counted("L-34B43A08"),
		"ec2/L-7212CCBC": ec2Counted("L-7212CCBC"),
		"ec2/L-3819A6DF": ec2Counted("L-3819A6DF"),

		"autoscaling/L-CDE20ADC": counted(func(h *asbackend.Handler, _, rgn string) int { return h.GroupCount(rgn) }),
		"autoscaling/L-6B80B8FA": counted(
			func(h *asbackend.Handler, _, rgn string) int { return h.LaunchConfigurationCount(rgn) },
		),
		"elasticfilesystem/L-848C634D": counted(func(h *efsbackend.Handler, _, rgn string) int {
			return h.FileSystemCount(rgn)
		}),
		"firehose/L-14BB0BE7": counted(func(h *firehosebackend.Handler, _, rgn string) int {
			return h.DeliveryStreamCount(rgn)
		}),
		"cognito-identity/L-8692CE1C": counted(func(h *cognitoidentitybackend.Handler, _, rgn string) int {
			return h.IdentityPoolCount(rgn)
		}),
		"cognito-idp/L-66E6DF30": counted(func(h *cognitoidpbackend.Handler, _, rgn string) int {
			return h.UserPoolCount(rgn)
		}),

		"ecr/L-CFEB8E8D": counted(func(h *ecrbackend.Handler, _, rgn string) int { return h.RepositoryCount(rgn) }),
	}
}
