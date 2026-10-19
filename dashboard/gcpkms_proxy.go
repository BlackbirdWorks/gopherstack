package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpKMSProxyPrefix = "/dashboard/api/v1/gcp/kms"
	queryTrue         = "true"

	kmsLocationSegs  = 4
	kmsRingsSegs     = 5
	kmsRingSegs      = 6
	kmsKeysSegs      = 7
	kmsKeySegs       = 8
	kmsVersionsSegs  = 9
	kmsVersionSegs   = 10
	kmsPublicKeySegs = 11
	kmsLocationsIdx  = 2
	kmsRingsIdx      = 4
	kmsKeysIdx       = 6
	kmsVersionsIdx   = 8
	kmsPublicKeyIdx  = 10
	kmsLocationsKind = "locations"
	kmsRingsKind     = "keyRings"
	kmsKeysKind      = "cryptoKeys"
	kmsVersionsKind  = "cryptoKeyVersions"
	kmsPublicKeyKind = "publicKey"
	kmsVerbEncrypt   = "encrypt"
	kmsVerbDecrypt   = "decrypt"
	kmsVerbSign      = "asymmetricSign"
	kmsVerbMacSign   = "macSign"
	kmsVerbMacVerify = "macVerify"
	kmsVerbDestroy   = "destroy"
	kmsVerbRestore   = "restore"
	kmsVerbPrimary   = "updatePrimaryVersion"
)

// registerGCPKMSRoutes exposes Cloud KMS to the SPA as the REST JSON shapes of
// projects.locations.keyRings(.cryptoKeys(.cryptoKeyVersions)), calling the backend in-process.
func (h *DashboardHandler) registerGCPKMSRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPKMSOps; ops != nil {
		route = (&gcpKMSAPI{kms: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpKMSProxyPrefix, route)
}

type gcpKMSAPI struct {
	kms kmspb.KeyManagementServiceServer
}

func (a *gcpKMSAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < kmsRingsSegs || r.segs[0] != gcpRESTProjectsSeg || r.segs[kmsLocationsIdx] != kmsLocationsKind ||
		r.segs[kmsRingsIdx] != kmsRingsKind {
		return nil, errGCPUnsupported()
	}

	if n <= kmsKeySegs {
		return a.ringLevel(ctx, r)
	}

	if r.segs[kmsKeysIdx] != kmsKeysKind || r.segs[kmsVersionsIdx] != kmsVersionsKind {
		return nil, errGCPUnsupported()
	}

	switch n {
	case kmsVersionsSegs:
		return a.versions(ctx, r)
	case kmsVersionSegs:
		return a.version(ctx, r)
	case kmsPublicKeySegs:
		if r.segs[kmsPublicKeyIdx] == kmsPublicKeyKind && r.verb == "" && r.method == http.MethodGet {
			return a.kms.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{
				Name: r.name(kmsVersionSegs), PublicKeyFormat: kmspb.PublicKey_PEM,
			})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) ringLevel(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n >= kmsKeysSegs && r.segs[kmsKeysIdx] != kmsKeysKind {
		return nil, errGCPUnsupported()
	}

	switch n {
	case kmsRingsSegs:
		return a.rings(ctx, r)
	case kmsRingSegs:
		if r.verb == "" && r.method == http.MethodGet {
			return a.kms.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: r.name(kmsRingSegs)})
		}
	case kmsKeysSegs:
		return a.keys(ctx, r)
	case kmsKeySegs:
		return a.key(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) rings(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(kmsLocationSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.kms.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		ring := &kmspb.KeyRing{}
		if err := r.unmarshal(ring); err != nil {
			return nil, err
		}

		return a.kms.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
			Parent: parent, KeyRingId: r.query.Get("keyRingId"), KeyRing: ring,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) keys(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(kmsRingSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.kms.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		key := &kmspb.CryptoKey{}
		if err := r.unmarshal(key); err != nil {
			return nil, err
		}

		return a.kms.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent: parent, CryptoKeyId: r.query.Get("cryptoKeyId"), CryptoKey: key,
			SkipInitialVersionCreation: r.query.Get("skipInitialVersionCreation") == queryTrue,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) key(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(kmsKeySegs)

	switch {
	case r.verb == kmsVerbEncrypt && r.method == http.MethodPost:
		req := &kmspb.EncryptRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.Encrypt(ctx, req)
	case r.verb == kmsVerbDecrypt && r.method == http.MethodPost:
		req := &kmspb.DecryptRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.Decrypt(ctx, req)
	case r.verb == kmsVerbPrimary && r.method == http.MethodPost:
		req := &kmspb.UpdateCryptoKeyPrimaryVersionRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.UpdateCryptoKeyPrimaryVersion(ctx, req)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.kms.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: name})
	case r.method == http.MethodPatch:
		key := &kmspb.CryptoKey{}
		if err := r.unmarshal(key); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		key.Name = name

		return a.kms.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{CryptoKey: key, UpdateMask: mask})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) versions(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(kmsKeySegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.kms.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		v := &kmspb.CryptoKeyVersion{}
		if err := r.unmarshal(v); err != nil {
			return nil, err
		}

		return a.kms.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
			Parent: parent, CryptoKeyVersion: v,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpKMSAPI) version(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(kmsVersionSegs)

	switch {
	case r.verb == "" && r.method == http.MethodGet:
		return a.kms.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: name})
	case r.verb == "" && r.method == http.MethodPatch:
		v := &kmspb.CryptoKeyVersion{}
		if err := r.unmarshal(v); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		v.Name = name

		return a.kms.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
			CryptoKeyVersion: v, UpdateMask: mask,
		})
	case r.method != http.MethodPost:
		return nil, errGCPUnsupported()
	}

	return a.versionVerb(ctx, r, name)
}

func (a *gcpKMSAPI) versionVerb(ctx context.Context, r gcpRESTCall, name string) (proto.Message, error) {
	switch r.verb {
	case kmsVerbDestroy:
		return a.kms.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: name})
	case kmsVerbRestore:
		return a.kms.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: name})
	case kmsVerbSign:
		req := &kmspb.AsymmetricSignRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.AsymmetricSign(ctx, req)
	case kmsVerbMacSign:
		req := &kmspb.MacSignRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.MacSign(ctx, req)
	case kmsVerbMacVerify:
		req := &kmspb.MacVerifyRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.kms.MacVerify(ctx, req)
	}

	return nil, errGCPUnsupported()
}
