package azurekeyvault

import (
	"net/http"
	"slices"
	"strings"
)

// routeVariants is the version-qualified and version-less route per operation.
const routeVariants = 2

func keyOpRoutes(op string) []route {
	segment := strings.ToLower(op)
	opName := strings.ToUpper(op[:1]) + op[1:]

	fn := bodyRoute(func(h *Handler, rc *reqCtx, in *KeyOperationRequest) (int, any, error) {
		return reply(h.Backend.KeyOperation(rc.vault, rc.base, rc.params["name"], rc.params["version"], op, in))
	})

	return []route{
		rt(http.MethodPost, "/keys/{name}/{version}/"+segment, opName, fn),
		rt(http.MethodPost, "/keys/{name}/"+segment, opName, fn),
	}
}

func keyRoutes() []route {
	return slices.Concat(keyCoreRoutes(), keyCryptoRoutes(), keyAccessRoutes())
}

func keyCoreRoutes() []route {
	return []route{
		rt(http.MethodPost, "/keys/{name}/create", "CreateKey", bodyRoute(
			func(h *Handler, rc *reqCtx, in *CreateKeyRequest) (int, any, error) {
				return reply(h.Backend.CreateKey(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(http.MethodPut, "/keys/{name}", "ImportKey", bodyRoute(
			func(h *Handler, rc *reqCtx, in *ImportKeyRequest) (int, any, error) {
				return reply(h.Backend.ImportKey(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(http.MethodGet, "/keys", "GetKeys", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListKeys(rc.vault, rc.base, false)
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
		rt(http.MethodGet, "/keys/{name}/versions", "GetKeyVersions", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListKeyVersions(rc.vault, rc.base, rc.params["name"])
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
		rt(http.MethodGet, "/keys/{name}/rotationpolicy", "GetKeyRotationPolicy",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetRotationPolicy(rc.vault, rc.base, rc.params["name"]))
			}),
		rt(http.MethodPut, "/keys/{name}/rotationpolicy", "UpdateKeyRotationPolicy", bodyRoute(
			func(h *Handler, rc *reqCtx, in *RotationPolicy) (int, any, error) {
				return reply(h.Backend.SetRotationPolicy(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(http.MethodPost, "/keys/{name}/rotate", "RotateKey", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.RotateKey(rc.vault, rc.base, rc.params["name"]))
		}),
		rt(http.MethodPost, "/keys/restore", "RestoreKey", bodyRoute(
			func(h *Handler, rc *reqCtx, in *RestoreRequest) (int, any, error) {
				return reply(h.Backend.RestoreKey(rc.vault, rc.base, in.Value))
			})),
		rt(http.MethodPost, "/keys/{name}/backup", "BackupKey", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.BackupKey(rc.vault, rc.params["name"]))
		}),
		rt(http.MethodPost, "/rng", "GetRandomBytes", bodyRoute(
			func(_ *Handler, _ *reqCtx, in *RandomBytesRequest) (int, any, error) {
				v, err := RandomBytes(in.Count)

				return reply(RandomBytesResult{Value: v}, err)
			})),
	}
}

func keyCryptoRoutes() []route {
	ops := []string{opEncrypt, opDecrypt, opSign, opVerify, opWrapKey, opUnwrapKey}
	routes := make([]route, 0, routeVariants*len(ops))

	for _, op := range ops {
		routes = append(routes, keyOpRoutes(op)...)
	}

	return routes
}

func keyAccessRoutes() []route {
	return []route{
		rt(http.MethodGet, "/keys/{name}/{version}", "GetKey", getKey),
		rt(http.MethodGet, "/keys/{name}", "GetKey", getKey),
		rt(http.MethodPatch, "/keys/{name}/{version}", "UpdateKey", bodyRoute(updateKey)),
		rt(http.MethodPatch, "/keys/{name}", "UpdateKey", bodyRoute(updateKey)),
		rt(http.MethodDelete, "/keys/{name}", "DeleteKey", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.DeleteKey(rc.vault, rc.base, rc.params["name"]))
		}),
		rt(http.MethodGet, "/deletedkeys", "GetDeletedKeys", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListKeys(rc.vault, rc.base, true)
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
		rt(http.MethodGet, "/deletedkeys/{name}", "GetDeletedKey", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.GetDeletedKey(rc.vault, rc.base, rc.params["name"]))
		}),
		rt(http.MethodDelete, "/deletedkeys/{name}", "PurgeDeletedKey", func(h *Handler, rc *reqCtx) (int, any, error) {
			return http.StatusNoContent, nil, h.Backend.PurgeKey(rc.vault, rc.params["name"])
		}),
		rt(
			http.MethodPost,
			"/deletedkeys/{name}/recover",
			"RecoverDeletedKey",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.RecoverKey(rc.vault, rc.base, rc.params["name"]))
			},
		),
	}
}

// RandomBytesRequest is the POST /rng body.
type RandomBytesRequest struct {
	Count int `json:"count"`
}

// RandomBytesResult is the POST /rng response.
type RandomBytesResult struct {
	Value string `json:"value"`
}

func getKey(h *Handler, rc *reqCtx) (int, any, error) {
	return reply(h.Backend.GetKey(rc.vault, rc.base, rc.params["name"], rc.params["version"]))
}

func updateKey(h *Handler, rc *reqCtx, in *UpdateKeyRequest) (int, any, error) {
	return reply(h.Backend.UpdateKey(rc.vault, rc.base, rc.params["name"], rc.params["version"], in))
}
