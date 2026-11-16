package gcpidentitytoolkit

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

type idpListQuery struct {
	token string
	size  int
}

func idpListParams(c *echo.Context) (idpListQuery, error) {
	q := idpListQuery{token: c.QueryParam("pageToken")}

	if raw := c.QueryParam("pageSize"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return q, errInvalidArg("Invalid page size.")
		}

		q.size = n
	}

	return q, nil
}

func idpOpFor[T any](c *echo.Context, rt route, b *InMemoryBackend, sp idpSpec[T], idParam string) (any, error) {
	switch {
	case rt.id == "" && c.Request().Method == http.MethodPost:
		in, _, err := readJSON[T](c)
		if err != nil {
			return nil, err
		}

		id := c.QueryParam(idParam)
		if id == "" {
			return nil, errInvalidArg("%s is required.", idParam)
		}

		return idpCreate(b, sp, rt.project, rt.tenant, id, in)
	case rt.id == "":
		q, err := idpListParams(c)
		if err != nil {
			return nil, err
		}

		return idpListResponse(b, sp, rt, q)
	case c.Request().Method == http.MethodGet:
		return idpGet(b, sp, rt.project, rt.tenant, rt.id)
	case c.Request().Method == http.MethodPatch:
		_, raw, err := readJSON[T](c)
		if err != nil {
			return nil, err
		}

		return idpUpdate(b, sp, rt.project, rt.tenant, rt.id, raw, c.QueryParam("updateMask"))
	}

	if err := idpDelete(b, sp, rt.project, rt.tenant, rt.id); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func idpListResponse[T any](b *InMemoryBackend, sp idpSpec[T], rt route, q idpListQuery) (any, error) {
	pg, err := idpList(b, sp, rt.project, rt.tenant, q.token, q.size)
	if err != nil {
		return nil, err
	}

	switch any(new(T)).(type) {
	case *defaultIdp:
		return &idtkv2.GoogleCloudIdentitytoolkitAdminV2ListDefaultSupportedIdpConfigsResponse{
			DefaultSupportedIdpConfigs: anySlice[defaultIdp](pg.Data), NextPageToken: pg.Next,
		}, nil
	case *oauthIdp:
		return &idtkv2.GoogleCloudIdentitytoolkitAdminV2ListOAuthIdpConfigsResponse{
			OauthIdpConfigs: anySlice[oauthIdp](pg.Data), NextPageToken: pg.Next,
		}, nil
	}

	return &idtkv2.GoogleCloudIdentitytoolkitAdminV2ListInboundSamlConfigsResponse{
		InboundSamlConfigs: anySlice[samlIdp](pg.Data), NextPageToken: pg.Next,
	}, nil
}

func anySlice[T any, S any](in []*S) []*T {
	out := make([]*T, 0, len(in))

	for _, v := range in {
		if t, ok := any(v).(*T); ok {
			out = append(out, t)
		}
	}

	return out
}

func (h *Handler) idpOp(c *echo.Context, rt route) (any, error) {
	b := h.Backend

	switch rt.res {
	case collDefaultIdp:
		return idpOpFor(c, rt, b, defaultIdpSpec, "defaultSupportedIdpId")
	case collOAuthIdp:
		return idpOpFor(c, rt, b, oauthIdpSpec, "oauthIdpConfigId")
	}

	return idpOpFor(c, rt, b, samlIdpSpec, "inboundSamlConfigId")
}
