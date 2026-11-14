package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ociobjectstoragebackend "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

// wireOCIObjectStoragePrivateEndpoints backs private endpoints with OCI Core subnets and VNICs.
func wireOCIObjectStoragePrivateEndpoints(byName map[string]service.Registerable) {
	h, ok := byName["OCIObjectStorage"].(*ociobjectstoragebackend.Handler)
	coreH, cok := byName["OCICore"].(*ocicorebackend.Handler)

	if !ok || !cok {
		return
	}

	h.Backend.SetNetwork(ociobjectstoragebackend.Network{
		Subnet: func(id string) (ociobjectstoragebackend.SubnetInfo, bool) {
			ref, live := coreH.Backend.LookupSubnet(id)

			return ociobjectstoragebackend.SubnetInfo{
				VcnID: ref.VcnID, AD: ref.AvailabilityDomain, CompartmentID: ref.CompartmentID,
			}, live
		},
		CreateNic: func(r ociobjectstoragebackend.NicRequest) (ociobjectstoragebackend.Nic, error) {
			nic, err := coreH.Backend.CreateServiceNic(ocicorebackend.ServiceNicIn{
				CompartmentID: r.CompartmentID, AD: r.AD, SubnetID: r.SubnetID, DisplayName: r.DisplayName,
				PrivateIP: r.IPAddress, NsgIDs: r.NsgIDs,
			})
			if err != nil {
				if ae, isAPI := err.(*ocicorebackend.APIError); isAPI { //nolint:errorlint // own type, never wrapped
					return ociobjectstoragebackend.Nic{}, &ociobjectstoragebackend.APIError{
						Status: ae.Status, Code: ae.Code, Message: ae.Message,
					}
				}

				return ociobjectstoragebackend.Nic{}, err
			}

			return ociobjectstoragebackend.Nic{VnicID: nic.VnicID, IPAddress: nic.IPAddress}, nil
		},
		DeleteNic: coreH.Backend.DeleteServiceNic,
	})
}
