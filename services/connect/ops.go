package connect

import (
	"context"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func instOf(req *restroute.Request) string { return req.Params["InstanceId"] }

func pageArgs(req *restroute.Request) (string, int32, error) {
	raw := req.Query.Get("maxResults")
	if raw == "" {
		return req.Query.Get("nextToken"), 0, nil
	}

	m, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return "", 0, invalidParamErr("maxResults must be an integer")
	}

	return req.Query.Get("nextToken"), int32(m), nil
}

func listOp[T any](
	key string,
	fn func(req *restroute.Request, token string, maxResults int32) ([]T, string, error),
) restroute.OpFunc {
	return func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		items, next, err := fn(req, token, m)
		if err != nil {
			return nil, 0, err
		}

		out := map[string]any{key: orEmpty(items)}
		if next != "" {
			out["NextToken"] = next
		}

		return out, 0, nil
	}
}

func describeOp[T any](label, key string, fn func(inst, id string) (*T, error)) restroute.OpFunc {
	return func(_ context.Context, req *restroute.Request) (any, int, error) {
		v, err := fn(instOf(req), req.Params[label])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{key: v}, 0, nil
	}
}

func deleteOp(label string, fn func(inst, id string) (*empty, error)) restroute.OpFunc {
	return func(_ context.Context, req *restroute.Request) (any, int, error) {
		_, err := fn(instOf(req), req.Params[label])

		return nil, 0, err
	}
}

func typed[I, O any](fn func(*I) (*O, error)) restroute.OpFunc {
	return restroute.Typed(func(_ context.Context, in *I) (*O, error) { return fn(in) })
}

func typedCtx[I, O any](fn func(context.Context, *I) (*O, error)) restroute.OpFunc {
	return restroute.Typed(fn)
}

// lexIn is the body of the Lex association operations.
type lexIn struct {
	LexBot   *LexBot   `json:"LexBot"`
	LexV2Bot *LexV2Bot `json:"LexV2Bot"`
	instPath
}

type lexQueryIn struct {
	BotName   string `json:"-" query:"botName"`
	LexRegion string `json:"-" query:"lexRegion"`
	instPath
}

type createInstanceOut struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

type attrIn struct {
	AttributeType string `json:"-" path:"AttributeType"`
	Value         string
	instPath
}

type storageIn struct {
	StorageConfig *InstanceStorageConfig
	ResourceType  string `json:"ResourceType"`
	ResourceQuery string `json:"-"            query:"resourceType"`
	AssociationID string `json:"-"                                 path:"AssociationId"`
	instPath
}

type originIn struct {
	Origin      string
	OriginQuery string `json:"-" query:"origin"`
	instPath
}

type lambdaIn struct {
	FunctionArn      string
	FunctionArnQuery string `json:"-" query:"functionArn"`
	instPath
}

type keyIn struct {
	Key           string
	AssociationID string `json:"-" path:"AssociationId"`
	instPath
}

type resourceTagsIn struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"-"    path:"resourceArn"`
	TagKeys     []string          `json:"-"                       query:"tagKeys"`
}

type useCaseRef struct {
	AssociationID string `json:"-" path:"IntegrationAssociationId"`
	UseCaseID     string `json:"-" path:"UseCaseId"`
	instPath
}

type storageOut struct {
	AssociationID string `json:"AssociationId"`
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	ops := map[string]restroute.OpFunc{}

	h.instanceOps(ops)
	h.accessOps(ops)
	h.peopleOps(ops)
	h.routingOps(ops)
	h.flowOps(ops)
	h.phoneOps(ops)
	h.contactOps(ops)
	h.tagOps(ops)

	return ops
}

func (h *Handler) instanceOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateInstance"] = replay(h, "CreateInstance", func(in *CreateInstanceInput) string { return in.ClientToken },
		func(ctx context.Context, in *CreateInstanceInput) (*createInstanceOut, error) {
			i, err := b.CreateInstance(ctx, in)
			if err != nil {
				return nil, err
			}

			return &createInstanceOut{Arn: i.Arn, ID: i.ID}, nil
		})
	ops["DescribeInstance"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		i, err := b.DescribeInstance(instOf(req))
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"Instance": i}, 0, nil
	}
	ops["ListInstances"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		items, next, err := b.ListInstances(token, m)
		if err != nil {
			return nil, 0, err
		}

		out := map[string]any{"InstanceSummaryList": orEmpty(items)}
		if next != "" {
			out["NextToken"] = next
		}

		return out, 0, nil
	}
	ops["DeleteInstance"] = func(ctx context.Context, req *restroute.Request) (any, int, error) {
		return nil, 0, b.DeleteInstance(ctx, instOf(req))
	}
	ops["DescribeInstanceAttribute"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		a, err := b.DescribeInstanceAttribute(instOf(req), req.Params["AttributeType"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"Attribute": a}, 0, nil
	}
	ops["ListInstanceAttributes"] = listOp(
		"Attributes",
		func(r *restroute.Request, t string, m int32) ([]Attribute, string, error) {
			return b.ListInstanceAttributes(instOf(r), t, m)
		},
	)
	ops["UpdateInstanceAttribute"] = typed(func(in *attrIn) (*empty, error) {
		return nil, b.UpdateInstanceAttribute(in.InstanceID, in.AttributeType, in.Value)
	})
	h.storageOps(ops)
}

func (h *Handler) storageOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["AssociateInstanceStorageConfig"] = typedCtx(func(ctx context.Context, in *storageIn) (*storageOut, error) {
		id, err := b.AssociateInstanceStorageConfig(ctx, in.InstanceID, in.ResourceType, in.StorageConfig)

		return &storageOut{AssociationID: id}, err
	})
	ops["DescribeInstanceStorageConfig"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		c, err := b.DescribeInstanceStorageConfig(
			instOf(req),
			req.Query.Get("resourceType"),
			req.Params["AssociationId"],
		)
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"StorageConfig": c}, 0, nil
	}
	ops["ListInstanceStorageConfigs"] = listOp(
		"StorageConfigs",
		func(r *restroute.Request, t string, m int32) ([]InstanceStorageConfig, string, error) {
			return b.ListInstanceStorageConfigs(instOf(r), r.Query.Get("resourceType"), t, m)
		},
	)
	ops["UpdateInstanceStorageConfig"] = typedCtx(func(ctx context.Context, in *storageIn) (*empty, error) {
		return nil, b.UpdateInstanceStorageConfig(
			ctx,
			in.InstanceID,
			in.ResourceQuery,
			in.AssociationID,
			in.StorageConfig,
		)
	})
	ops["DisassociateInstanceStorageConfig"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		return nil, 0, b.DisassociateInstanceStorageConfig(
			instOf(req),
			req.Query.Get("resourceType"),
			req.Params["AssociationId"],
		)
	}
}

func (h *Handler) accessOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["AssociateApprovedOrigin"] = typed(
		func(in *originIn) (*empty, error) { return nil, b.AssociateApprovedOrigin(in.InstanceID, in.Origin) },
	)
	ops["DisassociateApprovedOrigin"] = typed(func(in *originIn) (*empty, error) {
		return nil, b.DisassociateApprovedOrigin(in.InstanceID, in.OriginQuery)
	})
	ops["ListApprovedOrigins"] = listOp(
		"Origins",
		func(r *restroute.Request, t string, m int32) ([]string, string, error) {
			return b.ListApprovedOrigins(instOf(r), t, m)
		},
	)
	ops["AssociateLambdaFunction"] = typedCtx(func(ctx context.Context, in *lambdaIn) (*empty, error) {
		return nil, b.AssociateLambdaFunction(ctx, in.InstanceID, in.FunctionArn)
	})
	ops["DisassociateLambdaFunction"] = typed(func(in *lambdaIn) (*empty, error) {
		return nil, b.DisassociateLambdaFunction(in.InstanceID, in.FunctionArnQuery)
	})
	ops["ListLambdaFunctions"] = listOp(
		"LambdaFunctions",
		func(r *restroute.Request, t string, m int32) ([]string, string, error) {
			return b.ListLambdaFunctions(instOf(r), t, m)
		},
	)
	ops["AssociateLexBot"] = typed(func(in *lexIn) (*empty, error) {
		return nil, b.AssociateBot(in.InstanceID, LexBotConfig{LexBot: in.LexBot})
	})
	ops["DisassociateLexBot"] = typed(func(in *lexQueryIn) (*empty, error) {
		return nil, b.DisassociateBot(
			in.InstanceID,
			LexBotConfig{LexBot: &LexBot{Name: in.BotName, LexRegion: in.LexRegion}},
		)
	})
	ops["ListLexBots"] = listOp("LexBots", func(r *restroute.Request, t string, m int32) ([]LexBot, string, error) {
		items, next, err := b.ListBots(instOf(r), "V1", t, m)
		bots := make([]LexBot, 0, len(items))

		for _, c := range items {
			bots = append(bots, *c.LexBot)
		}

		return bots, next, err
	})
	ops["AssociateBot"] = typed(func(in *lexIn) (*empty, error) {
		return nil, b.AssociateBot(in.InstanceID, LexBotConfig{LexBot: in.LexBot, LexV2Bot: in.LexV2Bot})
	})
	ops["DisassociateBot"] = typed(func(in *lexIn) (*empty, error) {
		return nil, b.DisassociateBot(in.InstanceID, LexBotConfig{LexBot: in.LexBot, LexV2Bot: in.LexV2Bot})
	})
	ops["ListBots"] = listOp("LexBots", func(r *restroute.Request, t string, m int32) ([]LexBotConfig, string, error) {
		return b.ListBots(instOf(r), r.Query.Get("lexVersion"), t, m)
	})
	ops["AssociateSecurityKey"] = typed(func(in *keyIn) (*storageOut, error) {
		id, err := b.AssociateSecurityKey(in.InstanceID, in.Key)

		return &storageOut{AssociationID: id}, err
	})
	ops["DisassociateSecurityKey"] = typed(func(in *keyIn) (*empty, error) {
		return nil, b.DisassociateSecurityKey(in.InstanceID, in.AssociationID)
	})
	ops["ListSecurityKeys"] = listOp(
		"SecurityKeys",
		func(r *restroute.Request, t string, m int32) ([]SecurityKey, string, error) {
			return b.ListSecurityKeys(instOf(r), t, m)
		},
	)
	ops["CreateIntegrationAssociation"] = typedCtx(b.CreateIntegrationAssociation)
	ops["ListIntegrationAssociations"] = listOp(
		"IntegrationAssociationSummaryList",
		func(r *restroute.Request, t string, m int32) ([]IntegrationAssociation, string, error) {
			return b.ListIntegrationAssociations(
				instOf(r),
				r.Query.Get("integrationType"),
				r.Query.Get("integrationArn"),
				t,
				m,
			)
		},
	)
	ops["DeleteIntegrationAssociation"] = func(ctx context.Context, req *restroute.Request) (any, int, error) {
		_, err := b.DeleteIntegrationAssociation(ctx, instOf(req), req.Params["IntegrationAssociationId"])

		return nil, 0, err
	}
	ops["CreateUseCase"] = typedCtx(b.CreateUseCase)
	ops["ListUseCases"] = listOp(
		"UseCaseSummaryList",
		func(r *restroute.Request, t string, m int32) ([]UseCase, string, error) {
			return b.ListUseCases(instOf(r), r.Params["IntegrationAssociationId"], t, m)
		},
	)
	ops["DeleteUseCase"] = typed(func(in *useCaseRef) (*empty, error) {
		return b.DeleteUseCase(in.InstanceID, in.AssociationID, in.UseCaseID)
	})
}

func (h *Handler) peopleOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateUser"] = typedCtx(b.CreateUser)
	ops["DescribeUser"] = describeOp("UserId", "User", b.DescribeUser)
	ops["ListUsers"] = listOp(
		"UserSummaryList",
		func(r *restroute.Request, t string, m int32) ([]UserSummary, string, error) {
			return b.ListUsers(instOf(r), t, m)
		},
	)
	ops["DeleteUser"] = deleteOp("UserId", b.DeleteUser)

	for name, fn := range map[string]func(context.Context, *UpdateUserInput) (*empty, error){
		"UpdateUserIdentityInfo": b.UpdateUserIdentityInfo, "UpdateUserPhoneConfig": b.UpdateUserPhoneConfig,
		"UpdateUserRoutingProfile": b.UpdateUserRoutingProfile, "UpdateUserSecurityProfiles": b.UpdateUserSecurityProfiles,
		"UpdateUserHierarchy": b.UpdateUserHierarchy, "PutUserStatus": b.PutUserStatus,
	} {
		ops[name] = typedCtx(fn)
	}

	ops["CreateSecurityProfile"] = typedCtx(b.CreateSecurityProfile)
	ops["DescribeSecurityProfile"] = describeOp("SecurityProfileId", "SecurityProfile", b.DescribeSecurityProfile)
	ops["ListSecurityProfiles"] = listOp(
		"SecurityProfileSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Summary, string, error) {
			return b.ListSecurityProfiles(instOf(r), t, m)
		},
	)
	ops["UpdateSecurityProfile"] = typedCtx(b.UpdateSecurityProfile)
	ops["DeleteSecurityProfile"] = deleteOp("SecurityProfileId", b.DeleteSecurityProfile)
	ops["ListSecurityProfilePermissions"] = listOp(
		"Permissions",
		func(r *restroute.Request, t string, m int32) ([]string, string, error) {
			return b.ListSecurityProfilePermissions(instOf(r), r.Params["SecurityProfileId"], t, m)
		},
	)

	ops["CreateUserHierarchyGroup"] = typedCtx(b.CreateUserHierarchyGroup)
	ops["DescribeUserHierarchyGroup"] = describeOp("HierarchyGroupId", "HierarchyGroup", b.DescribeUserHierarchyGroup)
	ops["ListUserHierarchyGroups"] = listOp(
		"UserHierarchyGroupSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Summary, string, error) {
			return b.ListUserHierarchyGroups(instOf(r), t, m)
		},
	)
	ops["DeleteUserHierarchyGroup"] = deleteOp("HierarchyGroupId", b.DeleteUserHierarchyGroup)
	ops["UpdateUserHierarchyGroupName"] = typedCtx(b.UpdateUserHierarchyGroupName)
	ops["DescribeUserHierarchyStructure"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		s, err := b.DescribeUserHierarchyStructure(instOf(req))
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"HierarchyStructure": s}, 0, nil
	}
	ops["UpdateUserHierarchyStructure"] = typedCtx(b.UpdateUserHierarchyStructure)

	ops["CreateAgentStatus"] = typedCtx(b.CreateAgentStatus)
	ops["DescribeAgentStatus"] = describeOp("AgentStatusId", "AgentStatus", b.DescribeAgentStatus)
	ops["ListAgentStatuses"] = listOp(
		"AgentStatusSummaryList",
		func(r *restroute.Request, t string, m int32) ([]AgentStatusSummary, string, error) {
			return b.ListAgentStatuses(instOf(r), r.Query["AgentStatusTypes"], t, m)
		},
	)
	ops["UpdateAgentStatus"] = typedCtx(b.UpdateAgentStatus)

	ops["CreatePrompt"] = typedCtx(b.CreatePrompt)
	ops["DescribePrompt"] = describeOp("PromptId", "Prompt", b.DescribePrompt)
	ops["ListPrompts"] = listOp(
		"PromptSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Summary, string, error) {
			return b.ListPrompts(instOf(r), t, m)
		},
	)
	ops["UpdatePrompt"] = typedCtx(b.UpdatePrompt)
	ops["DeletePrompt"] = deleteOp("PromptId", b.DeletePrompt)
}

func (h *Handler) routingOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateRoutingProfile"] = typedCtx(b.CreateRoutingProfile)
	ops["DescribeRoutingProfile"] = describeOp("RoutingProfileId", "RoutingProfile", b.DescribeRoutingProfile)
	ops["ListRoutingProfiles"] = listOp(
		"RoutingProfileSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Summary, string, error) {
			return b.ListRoutingProfiles(instOf(r), t, m)
		},
	)
	ops["DeleteRoutingProfile"] = deleteOp("RoutingProfileId", b.DeleteRoutingProfile)
	ops["ListRoutingProfileQueues"] = listOp(
		"RoutingProfileQueueConfigSummaryList",
		func(r *restroute.Request, t string, m int32) ([]RoutingProfileQueueConfigSummary, string, error) {
			return b.ListRoutingProfileQueues(instOf(r), r.Params["RoutingProfileId"], t, m)
		},
	)

	for name, fn := range map[string]func(context.Context, *UpdateRoutingProfileInput) (*empty, error){
		"UpdateRoutingProfileName":                   b.UpdateRoutingProfileName,
		"UpdateRoutingProfileConcurrency":            b.UpdateRoutingProfileConcurrency,
		"UpdateRoutingProfileDefaultOutboundQueue":   b.UpdateRoutingProfileDefaultOutboundQueue,
		"UpdateRoutingProfileAgentAvailabilityTimer": b.UpdateRoutingProfileAgentAvailabilityTimer,
		"AssociateRoutingProfileQueues":              b.AssociateRoutingProfileQueues,
		"DisassociateRoutingProfileQueues":           b.DisassociateRoutingProfileQueues,
		"UpdateRoutingProfileQueues":                 b.UpdateRoutingProfileQueues,
	} {
		ops[name] = typedCtx(fn)
	}

	ops["CreateQueue"] = typedCtx(b.CreateQueue)
	ops["DescribeQueue"] = describeOp("QueueId", "Queue", b.DescribeQueue)
	ops["ListQueues"] = listOp(
		"QueueSummaryList",
		func(r *restroute.Request, t string, m int32) ([]QueueSummary, string, error) {
			return b.ListQueues(instOf(r), r.Query["queueTypes"], t, m)
		},
	)
	ops["DeleteQueue"] = deleteOp("QueueId", b.DeleteQueue)

	for name, fn := range map[string]func(context.Context, *UpdateQueueInput) (*empty, error){
		"UpdateQueueName": b.UpdateQueueName, "UpdateQueueStatus": b.UpdateQueueStatus,
		"UpdateQueueMaxContacts": b.UpdateQueueMaxContacts, "UpdateQueueHoursOfOperation": b.UpdateQueueHoursOfOperation,
		"UpdateQueueOutboundCallerConfig": b.UpdateQueueOutboundCallerConfig,
		"UpdateQueueOutboundEmailConfig":  b.UpdateQueueOutboundEmailConfig,
	} {
		ops[name] = typedCtx(fn)
	}

	ops["AssociateQueueQuickConnects"] = typed(b.AssociateQueueQuickConnects)
	ops["DisassociateQueueQuickConnects"] = typed(b.DisassociateQueueQuickConnects)
	ops["ListQueueQuickConnects"] = listOp(
		"QuickConnectSummaryList",
		func(r *restroute.Request, t string, m int32) ([]QuickConnectSummary, string, error) {
			return b.ListQueueQuickConnects(instOf(r), r.Params["QueueId"], t, m)
		},
	)

	ops["CreateHoursOfOperation"] = typedCtx(b.CreateHoursOfOperation)
	ops["DescribeHoursOfOperation"] = describeOp("HoursOfOperationId", "HoursOfOperation", b.DescribeHoursOfOperation)
	ops["ListHoursOfOperations"] = listOp(
		"HoursOfOperationSummaryList",
		func(r *restroute.Request, t string, m int32) ([]Summary, string, error) {
			return b.ListHoursOfOperations(instOf(r), t, m)
		},
	)
	ops["UpdateHoursOfOperation"] = typedCtx(b.UpdateHoursOfOperation)
	ops["DeleteHoursOfOperation"] = deleteOp("HoursOfOperationId", b.DeleteHoursOfOperation)

	ops["CreateQuickConnect"] = typedCtx(b.CreateQuickConnect)
	ops["DescribeQuickConnect"] = describeOp("QuickConnectId", "QuickConnect", b.DescribeQuickConnect)
	ops["ListQuickConnects"] = listOp(
		"QuickConnectSummaryList",
		func(r *restroute.Request, t string, m int32) ([]QuickConnectSummary, string, error) {
			return b.ListQuickConnects(instOf(r), r.Query["QuickConnectTypes"], t, m)
		},
	)
	ops["UpdateQuickConnectName"] = typedCtx(b.UpdateQuickConnectName)
	ops["UpdateQuickConnectConfig"] = typedCtx(b.UpdateQuickConnectConfig)
	ops["DeleteQuickConnect"] = deleteOp("QuickConnectId", b.DeleteQuickConnect)
}

func (h *Handler) flowOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateContactFlow"] = typedCtx(b.CreateContactFlow)
	ops["DescribeContactFlow"] = describeOp("ContactFlowId", "ContactFlow", b.DescribeContactFlow)
	ops["ListContactFlows"] = listOp(
		"ContactFlowSummaryList",
		func(r *restroute.Request, t string, m int32) ([]ContactFlowSummary, string, error) {
			return b.ListContactFlows(instOf(r), r.Query["contactFlowTypes"], t, m)
		},
	)
	ops["UpdateContactFlowContent"] = typedCtx(b.UpdateContactFlowContent)
	ops["UpdateContactFlowMetadata"] = typedCtx(b.UpdateContactFlowMetadata)
	ops["UpdateContactFlowName"] = typedCtx(b.UpdateContactFlowName)
	ops["DeleteContactFlow"] = deleteOp("ContactFlowId", b.DeleteContactFlow)

	ops["CreateContactFlowModule"] = replay(
		h,
		"CreateContactFlowModule",
		func(in *CreateContactFlowModuleInput) string { return in.ClientToken },
		b.CreateContactFlowModule,
	)
	ops["DescribeContactFlowModule"] = describeOp(
		"ContactFlowModuleId",
		"ContactFlowModule",
		b.DescribeContactFlowModule,
	)
	ops["ListContactFlowModules"] = listOp(
		"ContactFlowModulesSummaryList",
		func(r *restroute.Request, t string, m int32) ([]ContactFlowModuleSummary, string, error) {
			return b.ListContactFlowModules(instOf(r), r.Query.Get("state"), t, m)
		},
	)
	ops["UpdateContactFlowModuleContent"] = typedCtx(b.UpdateContactFlowModuleContent)
	ops["UpdateContactFlowModuleMetadata"] = typedCtx(b.UpdateContactFlowModuleMetadata)
	ops["DeleteContactFlowModule"] = deleteOp("ContactFlowModuleId", b.DeleteContactFlowModule)
}

func (h *Handler) phoneOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["ClaimPhoneNumber"] = replay(
		h,
		"ClaimPhoneNumber",
		func(in *ClaimPhoneNumberInput) string { return in.ClientToken },
		b.ClaimPhoneNumber,
	)
	ops["DescribePhoneNumber"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		p, err := b.DescribePhoneNumber(req.Params["PhoneNumberId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"ClaimedPhoneNumberSummary": p}, 0, nil
	}
	ops["UpdatePhoneNumber"] = typed(b.UpdatePhoneNumber)
	ops["UpdatePhoneNumberMetadata"] = typed(b.UpdatePhoneNumberMetadata)
	ops["ListPhoneNumbers"] = listOp(
		"PhoneNumberSummaryList",
		func(r *restroute.Request, t string, m int32) ([]PhoneNumberSummary, string, error) {
			return b.ListPhoneNumbers(instOf(r), r.Query["phoneNumberTypes"], r.Query["phoneNumberCountryCodes"], t, m)
		},
	)
	ops["ListPhoneNumbersV2"] = typed(b.ListPhoneNumbersV2)
	ops["ReleasePhoneNumber"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		_, err := b.ReleasePhoneNumber(req.Params["PhoneNumberId"])

		return nil, 0, err
	}
	ops["SearchAvailablePhoneNumbers"] = typed(b.SearchAvailablePhoneNumbers)
	ops["AssociatePhoneNumberContactFlow"] = typed(b.AssociatePhoneNumberContactFlow)
	ops["DisassociatePhoneNumberContactFlow"] = typed(b.DisassociatePhoneNumberContactFlow)
}

func (h *Handler) contactOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["StartOutboundVoiceContact"] = replay(
		h,
		"StartOutboundVoiceContact",
		func(in *StartOutboundVoiceContactInput) string { return in.ClientToken },
		b.StartOutboundVoiceContact,
	)
	ops["StartChatContact"] = replay(
		h,
		"StartChatContact",
		func(in *StartChatContactInput) string { return in.ClientToken },
		b.StartChatContact,
	)
	ops["StartTaskContact"] = replay(
		h,
		"StartTaskContact",
		func(in *StartTaskContactInput) string { return in.ClientToken },
		b.StartTaskContact,
	)
	ops["DescribeContact"] = describeOp("ContactId", "Contact", b.DescribeContact)
	ops["StopContact"] = typed(b.StopContact)
	ops["UpdateContactAttributes"] = typed(b.UpdateContactAttributes)
	ops["GetContactAttributes"] = describeAttrs(b)
	ops["UpdateContact"] = typed(b.UpdateContact)
	ops["SearchContacts"] = typed(b.SearchContacts)
	ops["TagContact"] = typed(b.TagContact)
	ops["UntagContact"] = typed(b.UntagContact)
	ops["GetCurrentMetricData"] = typed(b.GetCurrentMetricData)
	ops["GetMetricData"] = typed(b.GetMetricData)
	ops["GetCurrentUserData"] = typed(b.GetCurrentUserData)
}

func describeAttrs(b *Backend) restroute.OpFunc {
	return func(_ context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(b.GetContactAttributes(instOf(req), req.Params["InitialContactId"]))
	}
}

func passthrough[T any](v *T, err error) (any, int, error) {
	if err != nil {
		return nil, 0, err
	}

	return v, 0, nil
}

func (h *Handler) tagOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["TagResource"] = typed(
		func(in *resourceTagsIn) (*empty, error) { return nil, b.TagResource(in.ResourceArn, in.Tags) },
	)
	ops["UntagResource"] = typed(
		func(in *resourceTagsIn) (*empty, error) { return nil, b.UntagResource(in.ResourceArn, in.TagKeys) },
	)
	ops["ListTagsForResource"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		t, err := b.ListTagsForResource(req.Params["resourceArn"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"tags": t}, 0, nil
	}
}
