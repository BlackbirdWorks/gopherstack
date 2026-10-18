package bedrockagentcorecontrol

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type callFn func(sc scope, op string, in args) (*reply, error)

// run decodes and validates the request for op, runs fn, and returns the rendered result.
func (h *Handler) run(op string, fn callFn) restroute.OpFunc {
	sp := specs()[op]

	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		in, err := buildArgs(req, sp)
		if err != nil {
			return nil, 0, err
		}

		if err = checkRequired(sp, in); err != nil {
			return nil, 0, err
		}

		if err = checkEnums(sp, in); err != nil {
			return nil, 0, err
		}

		if err = validateCommon(in); err != nil {
			return nil, 0, err
		}

		rep, err := fn(h.Backend.scopeOf(ctx), op, in)
		if err != nil {
			return nil, 0, err
		}

		return rep.body, rep.status, nil
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	b := h.Backend
	ops := map[string]restroute.OpFunc{}

	for _, k := range kindTable() {
		bindKind(h, ops, k)
	}

	special := map[string]callFn{
		"BatchPutGatewayRateLimits":                  b.batchPutRateLimits,
		"SynchronizeGatewayTargets":                  b.synchronizeTargets,
		"SubmitRegistryRecordForApproval":            b.submitRecord,
		"UpdateRegistryRecordStatus":                 b.setRecordStatus,
		"RotatePaymentConnectorCredentials":          b.rotateConnectorCredentials,
		"ListAgentRuntimeVersions":                   b.listRuntimeVersions,
		"ListAgentRuntimeVersionsByCapacityProvider": b.listRuntimesByCapacityProvider,
		"ListHarnessVersions":                        b.listHarnessVersions,
		"ListConfigurationBundleVersions":            b.listBundleVersions,
		"GetConfigurationBundleVersion":              b.getBundleVersion,
		"CreateDatasetVersion":                       b.createDatasetVersion,
		"ListDatasetVersions":                        b.listDatasetVersions,
		"ListDatasetExamples":                        b.listDatasetExamples,
		"AddDatasetExamples":                         b.addDatasetExamples,
		"UpdateDatasetExamples":                      b.updateDatasetExamples,
		"DeleteDatasetExamples":                      b.deleteDatasetExamples,
		"ListPolicyGenerationAssets":                 plain(b.listGenerationAssets),
		"GetTokenVault": func(sc scope, op string, _ args) (*reply, error) {
			return b.getTokenVault(sc, op), nil
		},
		"SetTokenVaultCMK":     b.setTokenVaultCMK,
		"PutResourcePolicy":    plain(b.putResourcePolicy),
		"GetResourcePolicy":    plain(b.getResourcePolicy),
		"DeleteResourcePolicy": plain(b.deleteResourcePolicy),
		"TagResource":          plain(b.tagResource),
		"ListTagsForResource": func(sc scope, _ string, in args) (*reply, error) {
			return b.listTags(sc, in.str("resourceArn"))
		},
	}

	for op, fn := range special {
		ops[op] = h.run(op, fn)
	}

	ops["UntagResource"] = h.untag()

	return ops
}

func (h *Handler) untag() restroute.OpFunc {
	return func(ctx context.Context, req *restroute.Request) (any, int, error) {
		keys := req.Query["tagKeys"]
		if len(keys) == 0 {
			return nil, 0, invalid("tagKeys must not be empty")
		}

		rep, err := h.Backend.untagResource(h.Backend.scopeOf(ctx), keys, req.Params["resourceArn"])
		if err != nil {
			return nil, 0, err
		}

		return rep.body, rep.status, nil
	}
}

func bindKind(h *Handler, ops map[string]restroute.OpFunc, k *kindSpec) {
	b := h.Backend

	if k.Create != "" {
		ops[k.Create] = h.run(
			k.Create,
			func(sc scope, op string, in args) (*reply, error) { return b.create(sc, k, op, in) },
		)
	}

	if k.Update != "" {
		ops[k.Update] = h.run(
			k.Update,
			func(sc scope, op string, in args) (*reply, error) { return b.update(sc, k, op, in) },
		)
	}

	if k.Delete != "" {
		ops[k.Delete] = h.run(
			k.Delete,
			func(sc scope, op string, in args) (*reply, error) { return b.del(sc, k, op, in) },
		)
	}

	for _, g := range k.Gets {
		ops[g] = h.run(g, func(sc scope, op string, in args) (*reply, error) { return b.get(sc, k, op, in) })
	}

	for _, l := range k.Lists {
		ops[l] = h.run(l, func(sc scope, op string, in args) (*reply, error) { return b.list(sc, k, op, in) })
	}
}

// plain adapts an operation that ignores the operation name.
func plain(fn func(sc scope, in args) (*reply, error)) callFn {
	return func(sc scope, _ string, in args) (*reply, error) { return fn(sc, in) }
}
