package app

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const limitTestTrafficPolicy = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
	`"Endpoints":{"e":{"Type":"value","Value":"192.0.2.1"}},"StartEndpoint":"e"}`

const (
	limitTestRole       = "arn:aws:iam::000000000000:role/limit"
	limitTestDefinition = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
)

func name(prefix string, i int) string { return prefix + qnum(i) }

func qnum(i int) string { return strconv.Itoa(i) }

func logsClient(e limitEnv) *cloudwatchlogs.Client {
	return cloudwatchlogs.NewFromConfig(e.cfg, func(o *cloudwatchlogs.Options) { o.BaseEndpoint = e.endpoint() })
}

func kmsClient(e limitEnv) *kms.Client {
	return kms.NewFromConfig(e.cfg, func(o *kms.Options) { o.BaseEndpoint = e.endpoint() })
}

func eventsClient(e limitEnv) *eventbridge.Client {
	return eventbridge.NewFromConfig(e.cfg, func(o *eventbridge.Options) { o.BaseEndpoint = e.endpoint() })
}

func sfnClient(e limitEnv) *sfn.Client {
	return sfn.NewFromConfig(e.cfg, func(o *sfn.Options) { o.BaseEndpoint = e.endpoint() })
}

func route53Client(e limitEnv) *route53.Client {
	return route53.NewFromConfig(e.cfg, func(o *route53.Options) { o.BaseEndpoint = e.endpoint() })
}

func ecrClient(e limitEnv) *ecr.Client {
	return ecr.NewFromConfig(e.cfg, func(o *ecr.Options) { o.BaseEndpoint = e.endpoint() })
}

func kmsTargetKey(ctx context.Context, c *kms.Client) (string, error) {
	list, err := c.ListKeys(ctx, &kms.ListKeysInput{})
	if err != nil {
		return "", err
	}

	if len(list.Keys) > 0 {
		return aws.ToString(list.Keys[0].KeyId), nil
	}

	out, err := c.CreateKey(ctx, &kms.CreateKeyInput{})
	if err != nil {
		return "", err
	}

	return aws.ToString(out.KeyMetadata.KeyId), nil
}

func kmsCases() []limitCase {
	handler := func(*testing.T) service.Registerable {
		return kmsbackend.NewHandler(kmsbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion))
	}

	return []limitCase{
		{
			name: "kms keys", svc: "kms", quota: "L-C2F1777E",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				_, err := kmsClient(e).CreateKey(ctx, &kms.CreateKeyInput{})

				return err
			},
		},
		{
			name: "kms aliases per key", svc: "kms", quota: "L-340F62FB",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := kmsClient(e)

				key, err := kmsTargetKey(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateAlias(
					ctx,
					&kms.CreateAliasInput{AliasName: aws.String("alias/" + name("a", i)), TargetKeyId: &key},
				)

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := kmsClient(
					e,
				).DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: aws.String("alias/" + name("a", i))})

				return err
			},
		},
		{
			name: "kms grants per key", svc: "kms", quota: "L-D594A657",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := kmsClient(e)

				key, err := kmsTargetKey(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateGrant(ctx, &kms.CreateGrantInput{
					KeyId: &key, Name: aws.String(name("g", i)),
					GranteePrincipal: aws.String(limitTestRole),
					Operations:       []kmstypes.GrantOperation{kmstypes.GrantOperationEncrypt},
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := kmsClient(e)

				key, err := kmsTargetKey(ctx, c)
				if err != nil {
					return err
				}

				grants, err := c.ListGrants(ctx, &kms.ListGrantsInput{KeyId: &key})
				if err != nil || len(grants.Grants) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.RevokeGrant(ctx, &kms.RevokeGrantInput{KeyId: &key, GrantId: grants.Grants[0].GrantId})

				return err
			},
		},
	}
}

func logsAndECRCases() []limitCase {
	return []limitCase{
		{
			name: "log groups", svc: "logs", quota: "L-C7B9AAAB",
			code: "LimitExceededException", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return cwlogsbackend.NewHandler(
					cwlogsbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion),
				)
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := logsClient(
					e,
				).CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(name("g", i))})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := logsClient(
					e,
				).DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{LogGroupName: aws.String(name("g", i))})

				return err
			},
		},
		{
			name: "ecr repositories", svc: "ecr", quota: "L-CFEB8E8D",
			code: "LimitExceededException", limit: 2,
			handler: func(*testing.T) service.Registerable {
				return ecrbackend.NewHandler(ecrbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion, ""), nil)
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := ecrClient(
					e,
				).CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String(name("r", i))})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := ecrClient(
					e,
				).DeleteRepository(ctx, &ecr.DeleteRepositoryInput{RepositoryName: aws.String(name("r", i))})

				return err
			},
		},
	}
}

func eventsCases() []limitCase {
	handler := func(*testing.T) service.Registerable {
		return ebbackend.NewHandler(ebbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion))
	}
	apiKey := &ebtypes.CreateConnectionAuthRequestParameters{
		ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
			ApiKeyName: aws.String("k"), ApiKeyValue: aws.String("v"),
		},
	}
	createConn := func(ctx context.Context, e limitEnv, n string) (*eventbridge.CreateConnectionOutput, error) {
		return eventsClient(e).CreateConnection(ctx, &eventbridge.CreateConnectionInput{
			Name: aws.String(n), AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey, AuthParameters: apiKey,
		})
	}

	return []limitCase{
		{
			name: "event buses", svc: "events", quota: "L-658A4FD9",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(
					e,
				).CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String(name("b", i))})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(
					e,
				).DeleteEventBus(ctx, &eventbridge.DeleteEventBusInput{Name: aws.String(name("b", i))})

				return err
			},
		},
		{
			name: "event rules per bus", svc: "events", quota: "L-244521F2",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(e).PutRule(ctx, &eventbridge.PutRuleInput{
					Name: aws.String(name("r", i)), EventPattern: aws.String(`{"source":["a"]}`),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(e).DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: aws.String(name("r", i))})

				return err
			},
		},
		{
			name: "event connections", svc: "events", quota: "L-595D6D42",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := createConn(ctx, e, name("c", i))

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(
					e,
				).DeleteConnection(ctx, &eventbridge.DeleteConnectionInput{Name: aws.String(name("c", i))})

				return err
			},
		},
		{
			name: "event api destinations", svc: "events", quota: "L-FB1C3A6D",
			code: "LimitExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				conn, err := createConn(ctx, e, "base")
				if err != nil {
					out, derr := eventsClient(
						e,
					).DescribeConnection(ctx, &eventbridge.DescribeConnectionInput{Name: aws.String("base")})
					if derr != nil {
						return derr
					}

					conn = &eventbridge.CreateConnectionOutput{ConnectionArn: out.ConnectionArn}
				}

				_, err = eventsClient(e).CreateApiDestination(ctx, &eventbridge.CreateApiDestinationInput{
					Name: aws.String(name("d", i)), ConnectionArn: conn.ConnectionArn,
					InvocationEndpoint: aws.String(
						"https://example.com/hook",
					), HttpMethod: ebtypes.ApiDestinationHttpMethodPost,
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := eventsClient(
					e,
				).DeleteApiDestination(ctx, &eventbridge.DeleteApiDestinationInput{Name: aws.String(name("d", i))})

				return err
			},
		},
	}
}

func sfnCases() []limitCase {
	handler := func(*testing.T) service.Registerable {
		return sfnbackend.NewHandler(sfnbackend.NewInMemoryBackendWithConfig(quotaTestAccount, quotaTestRegion))
	}
	smARN := func(n string) *string {
		return aws.String("arn:aws:states:us-east-1:" + quotaTestAccount + ":stateMachine:" + n)
	}
	ensureMachine := func(ctx context.Context, e limitEnv) error {
		_, err := sfnClient(e).CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
			Name: aws.String("base"), Definition: aws.String(limitTestDefinition), RoleArn: aws.String(limitTestRole),
		})

		return err
	}

	return []limitCase{
		{
			name: "state machines", svc: "states", quota: "L-B66B0F91",
			code: "StateMachineLimitExceeded", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := sfnClient(e).CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
					Name: aws.String(
						name("m", i),
					), Definition: aws.String(limitTestDefinition), RoleArn: aws.String(limitTestRole),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := sfnClient(
					e,
				).DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: smARN(name("m", i))})

				return err
			},
		},
		{
			name: "activities", svc: "states", quota: "L-A9562A73",
			code: "ActivityLimitExceeded", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := sfnClient(e).CreateActivity(ctx, &sfn.CreateActivityInput{Name: aws.String(name("a", i))})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := sfnClient(e).DeleteActivity(ctx, &sfn.DeleteActivityInput{
					ActivityArn: aws.String(
						"arn:aws:states:us-east-1:" + quotaTestAccount + ":activity:" + name("a", i),
					),
				})

				return err
			},
		},
		{
			name: "state machine versions", svc: "states", quota: "L-6AFCB355",
			code: "ServiceQuotaExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				if err := ensureMachine(ctx, e); err != nil && quotaErrCode(err) != "StateMachineAlreadyExists" {
					return err
				}

				_, err := sfnClient(
					e,
				).PublishStateMachineVersion(ctx, &sfn.PublishStateMachineVersionInput{StateMachineArn: smARN("base")})

				return err
			},
		},
		{
			name: "state machine aliases", svc: "states", quota: "L-C1D5EBBB",
			code: "ServiceQuotaExceededException", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := sfnClient(e)

				if err := ensureMachine(ctx, e); err != nil && quotaErrCode(err) != "StateMachineAlreadyExists" {
					return err
				}

				ver, err := c.PublishStateMachineVersion(
					ctx,
					&sfn.PublishStateMachineVersionInput{StateMachineArn: smARN("base")},
				)
				if err != nil {
					return err
				}

				_, err = c.CreateStateMachineAlias(ctx, &sfn.CreateStateMachineAliasInput{
					Name: aws.String(name("al", i)),
					RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
						{StateMachineVersionArn: ver.StateMachineVersionArn, Weight: 100},
					},
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, i int) error {
				_, err := sfnClient(e).DeleteStateMachineAlias(ctx, &sfn.DeleteStateMachineAliasInput{
					StateMachineAliasArn: smARN("base:" + name("al", i)),
				})

				return err
			},
		},
	}
}

func route53Cases() []limitCase {
	handler := func(*testing.T) service.Registerable {
		return route53backend.NewHandler(route53backend.NewInMemoryBackend())
	}

	return []limitCase{
		{
			name: "hosted zones", svc: "route53", quota: "L-4EA4796A",
			code: "TooManyHostedZones", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := route53Client(e).CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
					Name: aws.String(name("z", i) + ".example.com"), CallerReference: aws.String(name("c", i)),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := route53Client(e)

				zones, err := c.ListHostedZones(ctx, &route53.ListHostedZonesInput{})
				if err != nil || len(zones.HostedZones) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: zones.HostedZones[0].Id})

				return err
			},
		},
		{
			name: "health checks", svc: "route53", quota: "L-ACB674F3",
			code: "TooManyHealthChecks", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := route53Client(e).CreateHealthCheck(ctx, &route53.CreateHealthCheckInput{
					CallerReference: aws.String(name("c", i)),
					HealthCheckConfig: &r53types.HealthCheckConfig{
						Type: r53types.HealthCheckTypeTcp, IPAddress: aws.String("192.0.2.1"), Port: aws.Int32(80),
					},
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := route53Client(e)

				list, err := c.ListHealthChecks(ctx, &route53.ListHealthChecksInput{})
				if err != nil || len(list.HealthChecks) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteHealthCheck(
					ctx,
					&route53.DeleteHealthCheckInput{HealthCheckId: list.HealthChecks[0].Id},
				)

				return err
			},
		},
		{
			name: "delegation sets", svc: "route53", quota: "L-A72C7724",
			code: "LimitsExceeded", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := route53Client(e).CreateReusableDelegationSet(ctx, &route53.CreateReusableDelegationSetInput{
					CallerReference: aws.String(name("c", i)),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := route53Client(e)

				list, err := c.ListReusableDelegationSets(ctx, &route53.ListReusableDelegationSetsInput{})
				if err != nil || len(list.DelegationSets) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteReusableDelegationSet(
					ctx,
					&route53.DeleteReusableDelegationSetInput{Id: list.DelegationSets[0].Id},
				)

				return err
			},
		},
		{
			name: "traffic policies", svc: "route53", quota: "L-FC688E7C",
			code: "TooManyTrafficPolicies", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := route53Client(e).CreateTrafficPolicy(ctx, &route53.CreateTrafficPolicyInput{
					Name:     aws.String(name("p", i)),
					Document: aws.String(limitTestTrafficPolicy),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := route53Client(e)

				list, err := c.ListTrafficPolicies(ctx, &route53.ListTrafficPoliciesInput{})
				if err != nil || len(list.TrafficPolicySummaries) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteTrafficPolicy(ctx, &route53.DeleteTrafficPolicyInput{
					Id: list.TrafficPolicySummaries[0].Id, Version: aws.Int32(1),
				})

				return err
			},
		},
		{
			name: "cidr collections", svc: "route53", quota: "L-AD61772F",
			code: "LimitsExceeded", limit: 2,
			handler: handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := route53Client(e).CreateCidrCollection(ctx, &route53.CreateCidrCollectionInput{
					Name: aws.String(name("cc", i)), CallerReference: aws.String(name("c", i)),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := route53Client(e)

				list, err := c.ListCidrCollections(ctx, &route53.ListCidrCollectionsInput{})
				if err != nil || len(list.CidrCollections) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteCidrCollection(ctx, &route53.DeleteCidrCollectionInput{Id: list.CidrCollections[0].Id})

				return err
			},
		},
	}
}

func TestQuotaLimitsCoreServices(t *testing.T) {
	t.Parallel()

	cases := slices.Concat(kmsCases(), logsAndECRCases(), eventsCases(), sfnCases(), route53Cases())

	runLimitCases(t, cases)
}
