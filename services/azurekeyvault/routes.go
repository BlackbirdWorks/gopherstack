package azurekeyvault

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultPageSize = 25

// reqCtx carries one resolved request into a route function.
type reqCtx struct {
	r      *http.Request
	params map[string]string
	vault  string
	base   string
	body   []byte
}

type routeFn func(h *Handler, rc *reqCtx) (int, any, error)

type route struct {
	fn      routeFn
	method  string
	op      string
	pattern []string
}

func rt(method, pattern, op string, fn routeFn) route {
	return route{method: method, op: op, pattern: splitPath(pattern), fn: fn}
}

// matchRoute returns the first route fitting method and segs, with captured params.
func matchRoute(routes []route, method string, segs []string) (*route, map[string]string) {
	for i := range routes {
		r := &routes[i]
		if r.method != method || len(r.pattern) != len(segs) {
			continue
		}

		params := map[string]string{}
		ok := true

		for j, p := range r.pattern {
			if name, found := strings.CutPrefix(p, "{"); found {
				params[strings.TrimSuffix(name, "}")] = segs[j]
			} else if p != segs[j] {
				ok = false

				break
			}
		}

		if ok {
			return r, params
		}
	}

	return nil, nil
}

func buildRoutes() []route {
	return slices.Concat(secretRoutes(), keyRoutes(), certRoutes())
}

// ListResponse is the paged list envelope.
type ListResponse[T any] struct {
	NextLink string `json:"nextLink,omitempty"`
	Value    []T    `json:"value"`
}

func listPage[T any](rc *reqCtx, items []T) (int, any, error) {
	q := rc.r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("maxresults"))
	pg := page.New(items, q.Get("$skiptoken"), limit, defaultPageSize)
	out := ListResponse[T]{Value: pg.Data}

	if out.Value == nil {
		out.Value = []T{}
	}

	if pg.Next != "" {
		next := url.Values{}
		next.Set("api-version", q.Get("api-version"))
		next.Set("$skiptoken", pg.Next)

		if limit > 0 {
			next.Set("maxresults", strconv.Itoa(limit))
		}

		out.NextLink = "https://" + rc.r.Host + rc.r.URL.Path + "?" + next.Encode()
	}

	return http.StatusOK, out, nil
}

func bodyRoute[T any](f func(h *Handler, rc *reqCtx, in *T) (int, any, error)) routeFn {
	return func(h *Handler, rc *reqCtx) (int, any, error) {
		in, err := decodeBody[T](rc)
		if err != nil {
			return 0, nil, err
		}

		return f(h, rc, in)
	}
}

// reply adapts a (value, error) backend result into a 200 route result.
func reply[T any](v T, err error) (int, any, error) {
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, v, nil
}

func secretRoutes() []route {
	return []route{
		rt(http.MethodPut, "/secrets/{name}", "SetSecret", bodyRoute(setSecret)),
		rt(http.MethodGet, "/secrets", "GetSecrets", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListSecrets(rc.vault, rc.base, false)
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
		rt(
			http.MethodGet,
			"/secrets/{name}/versions",
			"GetSecretVersions",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				items, err := h.Backend.ListSecretVersions(rc.vault, rc.base, rc.params["name"])
				if err != nil {
					return 0, nil, err
				}

				return listPage(rc, items)
			},
		),
		rt(http.MethodPost, "/secrets/restore", "RestoreSecret", bodyRoute(
			func(h *Handler, rc *reqCtx, in *RestoreRequest) (int, any, error) {
				return reply(h.Backend.RestoreSecret(rc.vault, rc.base, in.Value))
			})),
		rt(http.MethodPost, "/secrets/{name}/backup", "BackupSecret", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.BackupSecret(rc.vault, rc.params["name"]))
		}),
		rt(http.MethodGet, "/secrets/{name}/{version}", "GetSecret", getSecret),
		rt(http.MethodGet, "/secrets/{name}", "GetSecret", getSecret),
		rt(http.MethodPatch, "/secrets/{name}/{version}", "UpdateSecret", bodyRoute(updateSecret)),
		rt(http.MethodPatch, "/secrets/{name}", "UpdateSecret", bodyRoute(updateSecret)),
		rt(http.MethodDelete, "/secrets/{name}", "DeleteSecret", func(h *Handler, rc *reqCtx) (int, any, error) {
			return reply(h.Backend.DeleteSecret(rc.vault, rc.base, rc.params["name"]))
		}),
		rt(http.MethodGet, "/deletedsecrets", "GetDeletedSecrets", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListSecrets(rc.vault, rc.base, true)
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
		rt(
			http.MethodGet,
			"/deletedsecrets/{name}",
			"GetDeletedSecret",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetDeletedSecret(rc.vault, rc.base, rc.params["name"]))
			},
		),
		rt(
			http.MethodDelete,
			"/deletedsecrets/{name}",
			"PurgeDeletedSecret",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return http.StatusNoContent, nil, h.Backend.PurgeSecret(rc.vault, rc.params["name"])
			},
		),
		rt(
			http.MethodPost,
			"/deletedsecrets/{name}/recover",
			"RecoverDeletedSecret",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.RecoverSecret(rc.vault, rc.base, rc.params["name"]))
			},
		),
	}
}

func setSecret(h *Handler, rc *reqCtx, in *SetSecretRequest) (int, any, error) {
	return reply(h.Backend.SetSecret(rc.vault, rc.base, rc.params["name"], in))
}

func getSecret(h *Handler, rc *reqCtx) (int, any, error) {
	return reply(h.Backend.GetSecret(rc.vault, rc.base, rc.params["name"], rc.params["version"]))
}

func updateSecret(h *Handler, rc *reqCtx, in *UpdateSecretRequest) (int, any, error) {
	return reply(h.Backend.UpdateSecret(rc.vault, rc.base, rc.params["name"], rc.params["version"], in))
}
