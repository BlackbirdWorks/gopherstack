package azurearm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

const (
	createImport       = "Import"
	createImportSecure = "ImportSecure"

	vhdFooterSize       = 512
	vhdFixedType        = 2
	vhdCookie           = "conectix"
	vhdCurrentSize      = 48
	vhdDiskType         = 60
	vhdChecksumAt       = 64
	vhdChecksumEnd      = 68
	vhdSectorSize       = 512
	cmpFieldSrcURI      = "sourceUri"
	cmpFieldSecData     = "securityDataUri"
	cmpFieldSecMeta     = "securityMetadataUri"
	cmpFieldStorageAcct = "storageAccountId"
)

// ErrDiskBlobNotFound is returned by DiskBlobs for a missing container or blob.
var ErrDiskBlobNotFound = errors.New("disk blob not found")

// BlobProbe describes a blob in the Blob data plane.
type BlobProbe struct {
	// Footer is the last vhdFooterSize bytes of a page blob (empty for other blob types).
	Footer   []byte
	Size     int64
	PageBlob bool
}

// DiskBlobs reads blobs for disk and snapshot Import. Implementations must not call back into the ARM provider.
type DiskBlobs interface {
	ProbeBlob(container, blob string) (BlobProbe, error)
}

// SetDiskBlobs gives the Compute provider the Blob data plane used by Import and ImportSecure.
func (r *Registry) SetDiskBlobs(b DiskBlobs) {
	p, ok := r.ProviderNamed(namespaceMicrosoftCompute)
	if !ok {
		return
	}

	if cp, cpOk := p.(*ComputeProvider); cpOk {
		cp.mu.Lock("SetDiskBlobs")
		cp.blobs = b
		cp.mu.Unlock()
	}
}

// diskImport is the outcome of probing the blobs a disk or snapshot create refers to; it is computed outside the
// provider lock and consumed only when the resource is new.
type diskImport struct {
	err error
	gb  int
}

type blobRef struct {
	account, container, blob string
}

func parseBlobURI(field, raw string) (blobRef, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != acaSchemeHTTPS && u.Scheme != acaScaleHTTP) || u.Host == "" {
		return blobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
	}

	segs := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	labels := strings.Split(u.Hostname(), ".")

	var account string

	if len(labels) > 2 && strings.EqualFold(labels[1], "blob") {
		account = labels[0]
	} else {
		if len(segs) < 1 {
			return blobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
		}

		account, segs = segs[0], segs[1:]
	}

	if len(segs) < 2 || segs[0] == "" || strings.Join(segs[1:], "/") == "" {
		return blobRef{}, cmpInvalid("The creationData.%s '%s' must name a container and a blob.", field, raw)
	}

	blob, err := url.PathUnescape(strings.Join(segs[1:], "/"))
	if err != nil {
		return blobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
	}

	return blobRef{account: strings.ToLower(account), container: segs[0], blob: blob}, nil
}

func storageAccountName(id string) (string, bool) {
	parsed, err := ParseGenericResourcePath("/" + strings.TrimPrefix(id, "/"))
	if err != nil || !strings.EqualFold(parsed.Namespace, namespaceMicrosoftStorage) ||
		len(parsed.Types) != 1 || !strings.EqualFold(parsed.Types[0], "storageAccounts") || len(parsed.Names) != 1 {
		return "", false
	}

	return strings.ToLower(parsed.Names[0]), true
}

func (p *ComputeProvider) diskBlobs() DiskBlobs {
	p.mu.RLock("diskBlobs")
	defer p.mu.RUnlock()

	return p.blobs
}

func (p *ComputeProvider) probeRef(blobs DiskBlobs, field string, ref blobRef) (BlobProbe, error) {
	got, err := blobs.ProbeBlob(ref.container, ref.blob)
	if errors.Is(err, ErrDiskBlobNotFound) {
		return BlobProbe{}, cmpInvalid("The creationData.%s blob '%s/%s' does not exist in storage account '%s'.",
			field, ref.container, ref.blob, ref.account)
	}

	if err != nil {
		return BlobProbe{}, netErr(http.StatusInternalServerError, "InternalExecutionError",
			"The blob service could not be read: %v", err)
	}

	return got, nil
}

// vhdVirtualBytes validates a fixed VHD (blob size includes the 512-byte footer) and returns its virtual size.
func vhdVirtualBytes(got BlobProbe) (int64, error) {
	if !got.PageBlob {
		return 0, cmpInvalid("The source blob must be a page blob containing a fixed-size VHD.")
	}

	if got.Size < vhdFooterSize+vhdSectorSize || got.Size%vhdSectorSize != 0 || len(got.Footer) != vhdFooterSize {
		return 0, cmpInvalid("The source blob size %d is not valid for a fixed VHD: it must be a multiple of %d bytes "+
			"and include the %d-byte footer.", got.Size, vhdSectorSize, vhdFooterSize)
	}

	f := got.Footer
	if !bytes.Equal(f[:len(vhdCookie)], []byte(vhdCookie)) ||
		binary.BigEndian.Uint32(f[vhdDiskType:]) != vhdFixedType ||
		binary.BigEndian.Uint64(f[vhdCurrentSize:]) != uint64(got.Size-vhdFooterSize) ||
		binary.BigEndian.Uint32(f[vhdChecksumAt:]) != vhdChecksum(f) {
		return 0, cmpInvalid("The source blob is not a valid fixed-size VHD: the footer is missing or inconsistent.")
	}

	return got.Size - vhdFooterSize, nil
}

func vhdChecksum(footer []byte) uint32 {
	var sum uint32

	for i, b := range footer {
		if i >= vhdChecksumAt && i < vhdChecksumEnd {
			continue
		}

		sum += uint32(b)
	}

	return ^sum
}

// resolveDiskImport probes the blobs of an Import or ImportSecure create. It returns nil for any other createOption.
func (p *ComputeProvider) resolveDiskImport(body map[string]any, snapshot bool) *diskImport {
	cd := mapOf(propsOf(body)[cmpFieldCreation])
	opt := strOf(cd, cmpFieldCreateOpt)

	secure := strings.EqualFold(opt, createImportSecure)
	if !secure && !strings.EqualFold(opt, createImport) {
		return nil
	}

	gb, err := p.importDisk(cd, secure, snapshot)

	return &diskImport{gb: gb, err: err}
}

func (p *ComputeProvider) importDisk(cd map[string]any, secure, snapshot bool) (int, error) {
	if secure && snapshot {
		return 0, cmpInvalid("Snapshots are created with creationData.createOption 'Copy', 'CopyStart' or 'Import'.")
	}

	src, err := parseBlobURI(cmpFieldSrcURI, strOf(cd, cmpFieldSrcURI))
	if err != nil {
		return 0, err
	}

	if err = checkImportAccount(cd, secure, src); err != nil {
		return 0, err
	}

	blobs := p.diskBlobs()
	if blobs == nil {
		return 0, cmpInvalid("Import is not available: the Blob service is not wired to this provider.")
	}

	if secure {
		if err = p.checkSecureBlobs(blobs, cd, src); err != nil {
			return 0, err
		}
	}

	got, err := p.probeRef(blobs, cmpFieldSrcURI, src)
	if err != nil {
		return 0, err
	}

	virt, err := vhdVirtualBytes(got)
	if err != nil {
		return 0, err
	}

	gb := int((virt + bytesPerGiB - 1) / bytesPerGiB)
	if gb > cmpMaxDiskGB {
		return 0, cmpInvalid("The source VHD of %d bytes exceeds the maximum disk size of %d GB.", virt, cmpMaxDiskGB)
	}

	return gb, nil
}

func checkImportAccount(cd map[string]any, secure bool, src blobRef) error {
	id := strOf(cd, cmpFieldStorageAcct)
	if id == "" && secure {
		return cmpInvalid("Required parameter 'creationData.storageAccountId' is missing (null).")
	}

	if id == "" {
		return nil
	}

	acct, ok := storageAccountName(id)
	if !ok {
		return cmpInvalid("The creationData.storageAccountId '%s' is not a storage account resource id.", id)
	}

	if acct != src.account {
		return cmpInvalid("The creationData.sourceUri is in storage account '%s' but storageAccountId names '%s'.",
			src.account, acct)
	}

	return nil
}

func (p *ComputeProvider) checkSecureBlobs(blobs DiskBlobs, cd map[string]any, src blobRef) error {
	for _, f := range []string{cmpFieldSecData, cmpFieldSecMeta} {
		raw := strOf(cd, f)
		if raw == "" {
			if f == cmpFieldSecData {
				return cmpInvalid("Required parameter 'creationData.securityDataUri' is missing (null).")
			}

			continue
		}

		ref, err := parseBlobURI(f, raw)
		if err != nil {
			return err
		}

		if ref.account != src.account {
			return cmpInvalid("The creationData.%s must be in the same storage account as sourceUri.", f)
		}

		if _, err = p.probeRef(blobs, f, ref); err != nil {
			return err
		}
	}

	return nil
}

func (p *ComputeProvider) diskImportFor(id ResourceID, typ string, body map[string]any) *diskImport {
	if len(id.Types) != 1 || (typ != cmpDisks && typ != cmpSnaps) {
		return nil
	}

	return p.resolveDiskImport(body, typ == cmpSnaps)
}
