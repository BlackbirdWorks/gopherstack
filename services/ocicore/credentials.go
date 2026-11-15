package ocicore

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	windowsUser     = "opc"
	windowsPassword = 20
)

// InstanceCredentials is the initial login of a Windows instance.
type InstanceCredentials struct {
	Password string `json:"password"`
	Username string `json:"username"`
}

// stampCredentials gives a Windows instance the initial credentials its guest would have generated.
func (b *InMemoryBackend) stampCredentials(inst *Instance) {
	if img, err := b.findImage(inst.ImageID); err == nil && img.OperatingSystem == osWindows {
		inst.Internal.InitialPassword = awsid.NewN("", windowsPassword)
	}
}

// GetWindowsInstanceInitialCredentials returns the initial credentials of a Windows instance.
func (b *InMemoryBackend) GetWindowsInstanceInitialCredentials(id string) (InstanceCredentials, *APIError) {
	b.mu.RLock("GetWindowsInstanceInitialCredentials")
	defer b.mu.RUnlock()

	inst, aerr := getLive[*Instance](b, id)
	if aerr != nil {
		return InstanceCredentials{}, aerr
	}

	if inst.Internal.InitialPassword == "" {
		return InstanceCredentials{}, errNotFound(id)
	}

	return InstanceCredentials{Username: windowsUser, Password: inst.Internal.InitialPassword}, nil
}

func credentialEntries() []entry {
	return []entry{{func(h *Handler, q *req) error {
		out, aerr := h.Backend.GetWindowsInstanceInitialCredentials(q.param("instanceId"))
		if aerr != nil {
			return aerr
		}

		return q.json(http.StatusOK, out, "")
	}, http.MethodGet, idPath("/instances", "instanceId") + "/initialCredentials", "GetWindowsInstanceInitialCredentials"}}
}
