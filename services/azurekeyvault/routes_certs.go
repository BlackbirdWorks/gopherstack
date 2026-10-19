package azurekeyvault

import (
	"net/http"
	"slices"
)

func certRoutes() []route {
	return slices.Concat(certMiscRoutes(), certDeletedRoutes(), certCoreRoutes())
}

func certMiscRoutes() []route {
	return []route{
		rt(http.MethodPut, "/certificates/contacts", "SetCertificateContacts", bodyRoute(
			func(h *Handler, rc *reqCtx, in *Contacts) (int, any, error) {
				return reply(h.Backend.SetContacts(rc.vault, rc.base, in))
			})),
		rt(
			http.MethodGet,
			"/certificates/contacts",
			"GetCertificateContacts",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetContacts(rc.vault, rc.base))
			},
		),
		rt(
			http.MethodDelete,
			"/certificates/contacts",
			"DeleteCertificateContacts",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.DeleteContacts(rc.vault, rc.base))
			},
		),
		rt(
			http.MethodGet,
			"/certificates/issuers",
			"GetCertificateIssuers",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				items, err := h.Backend.ListIssuers(rc.vault, rc.base)
				if err != nil {
					return 0, nil, err
				}

				return listPage(rc, items)
			},
		),
		rt(http.MethodPut, "/certificates/issuers/{issuer}", "SetCertificateIssuer", bodyRoute(
			func(h *Handler, rc *reqCtx, in *Issuer) (int, any, error) {
				return reply(h.Backend.SetIssuer(rc.vault, rc.base, rc.params["issuer"], in))
			})),
		rt(http.MethodPatch, "/certificates/issuers/{issuer}", "UpdateCertificateIssuer", bodyRoute(
			func(h *Handler, rc *reqCtx, in *Issuer) (int, any, error) {
				return reply(h.Backend.UpdateIssuer(rc.vault, rc.base, rc.params["issuer"], in))
			})),
		rt(
			http.MethodGet,
			"/certificates/issuers/{issuer}",
			"GetCertificateIssuer",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetIssuer(rc.vault, rc.base, rc.params["issuer"]))
			},
		),
		rt(
			http.MethodDelete,
			"/certificates/issuers/{issuer}",
			"DeleteCertificateIssuer",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.DeleteIssuer(rc.vault, rc.base, rc.params["issuer"]))
			},
		),
		rt(http.MethodPost, "/certificates/restore", "RestoreCertificate", bodyRoute(
			func(h *Handler, rc *reqCtx, in *RestoreRequest) (int, any, error) {
				return reply(h.Backend.RestoreCertificate(rc.vault, rc.base, in.Value))
			})),
		rt(http.MethodGet, "/certificates", "GetCertificates", func(h *Handler, rc *reqCtx) (int, any, error) {
			items, err := h.Backend.ListCertificates(rc.vault, rc.base, false)
			if err != nil {
				return 0, nil, err
			}

			return listPage(rc, items)
		}),
	}
}

func certDeletedRoutes() []route {
	return []route{
		rt(
			http.MethodGet,
			"/deletedcertificates",
			"GetDeletedCertificates",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				items, err := h.Backend.ListCertificates(rc.vault, rc.base, true)
				if err != nil {
					return 0, nil, err
				}

				return listPage(rc, items)
			},
		),
		rt(
			http.MethodGet,
			"/deletedcertificates/{name}",
			"GetDeletedCertificate",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetDeletedCertificate(rc.vault, rc.base, rc.params["name"]))
			},
		),
		rt(
			http.MethodDelete,
			"/deletedcertificates/{name}",
			"PurgeDeletedCertificate",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return http.StatusNoContent, nil, h.Backend.PurgeCertificate(rc.vault, rc.params["name"])
			},
		),
		rt(
			http.MethodPost,
			"/deletedcertificates/{name}/recover",
			"RecoverDeletedCertificate",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.RecoverCertificate(rc.vault, rc.base, rc.params["name"]))
			},
		),
	}
}

func certCoreRoutes() []route {
	return []route{
		rt(http.MethodPost, "/certificates/{name}/create", "CreateCertificate", bodyRoute(
			func(h *Handler, rc *reqCtx, in *CreateCertRequest) (int, any, error) {
				op, err := h.Backend.CreateCertificate(rc.vault, rc.base, rc.params["name"], in)
				if err != nil {
					return 0, nil, err
				}

				return http.StatusAccepted, op, nil
			})),
		rt(http.MethodPost, "/certificates/{name}/import", "ImportCertificate", bodyRoute(
			func(h *Handler, rc *reqCtx, in *ImportCertRequest) (int, any, error) {
				return reply(h.Backend.ImportCertificate(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(
			http.MethodPost,
			"/certificates/{name}/backup",
			"BackupCertificate",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.BackupCertificate(rc.vault, rc.params["name"]))
			},
		),
		rt(
			http.MethodGet,
			"/certificates/{name}/versions",
			"GetCertificateVersions",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				items, err := h.Backend.ListCertificateVersions(rc.vault, rc.base, rc.params["name"])
				if err != nil {
					return 0, nil, err
				}

				return listPage(rc, items)
			},
		),
		rt(
			http.MethodGet,
			"/certificates/{name}/policy",
			"GetCertificatePolicy",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetCertificatePolicy(rc.vault, rc.base, rc.params["name"]))
			},
		),
		rt(http.MethodPatch, "/certificates/{name}/policy", "UpdateCertificatePolicy", bodyRoute(
			func(h *Handler, rc *reqCtx, in *CertPolicy) (int, any, error) {
				return reply(h.Backend.UpdateCertificatePolicy(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(
			http.MethodGet,
			"/certificates/{name}/pending",
			"GetCertificateOperation",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.GetPendingCertificate(rc.vault, rc.base, rc.params["name"]))
			},
		),
		rt(http.MethodPatch, "/certificates/{name}/pending", "UpdateCertificateOperation", bodyRoute(
			func(h *Handler, rc *reqCtx, in *UpdatePendingRequest) (int, any, error) {
				return reply(h.Backend.UpdatePendingCertificate(rc.vault, rc.base, rc.params["name"], in))
			})),
		rt(
			http.MethodDelete,
			"/certificates/{name}/pending",
			"DeleteCertificateOperation",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.DeletePendingCertificate(rc.vault, rc.base, rc.params["name"]))
			},
		),
		rt(http.MethodGet, "/certificates/{name}/{version}", "GetCertificate", getCert),
		rt(http.MethodGet, "/certificates/{name}", "GetCertificate", getCert),
		rt(http.MethodPatch, "/certificates/{name}/{version}", "UpdateCertificate", bodyRoute(updateCert)),
		rt(http.MethodPatch, "/certificates/{name}", "UpdateCertificate", bodyRoute(updateCert)),
		rt(
			http.MethodDelete,
			"/certificates/{name}",
			"DeleteCertificate",
			func(h *Handler, rc *reqCtx) (int, any, error) {
				return reply(h.Backend.DeleteCertificate(rc.vault, rc.base, rc.params["name"]))
			},
		),
	}
}

func getCert(h *Handler, rc *reqCtx) (int, any, error) {
	return reply(h.Backend.GetCertificate(rc.vault, rc.base, rc.params["name"], rc.params["version"]))
}

func updateCert(h *Handler, rc *reqCtx, in *UpdateCertRequest) (int, any, error) {
	return reply(h.Backend.UpdateCertificate(rc.vault, rc.base, rc.params["name"], rc.params["version"], in))
}
