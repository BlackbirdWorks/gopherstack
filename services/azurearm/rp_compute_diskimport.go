package azurearm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
)

const (
	createImport       = "Import"
	createImportSecure = "ImportSecure"

	vhdFooterSize       = 512
	vhdFixedType        = 2
	vhdCookie           = "conectix"
	vhdFeatures         = 8
	vhdVersion          = 12
	vhdDataOffset       = 16
	vhdCurrentSize      = 48
	vhdFeatureReserved  = 2
	vhdFormatVersion    = 0x00010000
	vhdMiB              = 1 << 20
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

// BlobRef addresses a blob by the URI an Import names; Query is the URI's raw query (a SAS token, if any).
type BlobRef struct {
	Account, Container, Blob, Query string
}

// ErrDiskBlobDenied is returned by DiskBlobs when the Blob service would refuse the read.
var ErrDiskBlobDenied = errors.New("disk blob access denied")

// DiskBlobs reads blobs for disk and snapshot Import. Implementations must authorize the read as the Blob service
// would and must not call back into the ARM provider.
type DiskBlobs interface {
	ProbeBlob(ref BlobRef) (BlobProbe, error)
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
		cp.hasAccount = r.storageAccountLookup()
		cp.mu.Unlock()
	}
}

// diskImport is the outcome of probing the blobs a disk or snapshot create refers to; it is computed outside the
// provider lock and consumed only when the resource is new.
type diskImport struct {
	err error
	gb  int
}

func parseBlobURI(field, raw string) (BlobRef, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != acaSchemeHTTPS && u.Scheme != acaScaleHTTP) || u.Host == "" || u.User != nil {
		return BlobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
	}

	account, segs, ok := splitBlobPath(u)
	if !ok || !validStorageAccountName(account) {
		return BlobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
	}

	if len(segs) < 2 || segs[0] == "" || strings.Join(segs[1:], "/") == "" {
		return BlobRef{}, cmpInvalid("The creationData.%s '%s' must name a container and a blob.", field, raw)
	}

	container, blob, ok := cleanBlobSegments(segs)
	if !ok {
		return BlobRef{}, cmpInvalid("The creationData.%s '%s' is not a valid blob URI.", field, raw)
	}

	return BlobRef{Account: strings.ToLower(account), Container: container, Blob: blob, Query: u.RawQuery}, nil
}

// splitBlobPath returns the account and the remaining path segments of an emulator blob URI, in the virtual-host
// (account.blob.suffix) or path-style form.
func splitBlobPath(u *url.URL) (string, []string, bool) {
	segs := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	labels := strings.Split(u.Hostname(), ".")

	switch {
	case len(labels) > 2 && strings.EqualFold(labels[1], "blob"):
		return labels[0], segs, true
	case isPathStyleHost(u.Hostname()):
		return segs[0], segs[1:], true
	}

	return "", nil, false
}

// cleanBlobSegments unescapes the container and blob segments; dot segments, encoded slashes and backslashes are
// refused so a name cannot address anything but one blob.
func cleanBlobSegments(segs []string) (string, string, bool) {
	dec := make([]string, len(segs))

	for i, seg := range segs {
		d, err := url.PathUnescape(seg)
		if err != nil || d == "." || d == ".." || strings.ContainsAny(d, "/\\") {
			return "", "", false
		}

		dec[i] = d
	}

	return dec[0], strings.Join(dec[1:], "/"), true
}

// isPathStyleHost reports whether host is a form the emulator serves path-style blob URIs on: an IP literal,
// localhost, or a single-label name. Any other host is not an emulator storage endpoint.
func isPathStyleHost(host string) bool {
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}

	return !strings.Contains(host, ".") || strings.EqualFold(host, "localhost")
}

func validStorageAccountName(name string) bool {
	const minLen, maxLen = 3, 24

	if len(name) < minLen || len(name) > maxLen {
		return false
	}

	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}

	return true
}

func storageAccountRef(id string) (string, string, bool) {
	parsed, err := ParseGenericResourcePath("/" + strings.TrimPrefix(id, "/"))
	if err != nil || !strings.EqualFold(parsed.Namespace, namespaceMicrosoftStorage) ||
		len(parsed.Types) != 1 || !strings.EqualFold(parsed.Types[0], "storageAccounts") || len(parsed.Names) != 1 {
		return "", "", false
	}

	return strings.ToLower(parsed.Names[0]), parsed.ResourceGroup, true
}

func (p *ComputeProvider) diskSeams() (DiskBlobs, func(name, rg string) bool) {
	p.mu.RLock("diskSeams")
	defer p.mu.RUnlock()

	return p.blobs, p.hasAccount
}

func (p *ComputeProvider) probeRef(blobs DiskBlobs, field string, ref BlobRef) (BlobProbe, error) {
	got, err := blobs.ProbeBlob(ref)
	if errors.Is(err, ErrDiskBlobNotFound) || errors.Is(err, ErrDiskBlobDenied) {
		return BlobProbe{}, cmpInvalid(
			"The creationData.%s blob '%s/%s' does not exist in storage account '%s' or is not accessible.",
			field, ref.Container, ref.Blob, ref.Account)
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

	virt := got.Size - vhdFooterSize
	if virt%vhdMiB != 0 {
		return 0, cmpInvalid("The source VHD virtual size %d is not aligned to 1 MiB.", virt)
	}

	f := got.Footer
	if !bytes.Equal(f[:len(vhdCookie)], []byte(vhdCookie)) ||
		binary.BigEndian.Uint32(f[vhdFeatures:])&vhdFeatureReserved == 0 ||
		binary.BigEndian.Uint32(f[vhdVersion:]) != vhdFormatVersion ||
		binary.BigEndian.Uint64(f[vhdDataOffset:]) != math.MaxUint64 ||
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

	blobs, hasAccount := p.diskSeams()
	if blobs == nil {
		return 0, cmpInvalid("Import is not available: the Blob service is not wired to this provider.")
	}

	if err = checkImportAccount(cd, secure, src, hasAccount); err != nil {
		return 0, err
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

func checkImportAccount(
	cd map[string]any, secure bool, src BlobRef, hasAccount func(name, rg string) bool,
) error {
	id := strOf(cd, cmpFieldStorageAcct)
	if id == "" && secure {
		return cmpInvalid("Required parameter 'creationData.storageAccountId' is missing (null).")
	}

	var rg string

	if id != "" {
		acct, acctRG, ok := storageAccountRef(id)
		if !ok {
			return cmpInvalid("The creationData.storageAccountId '%s' is not a storage account resource id.", id)
		}

		if acct != src.Account {
			return cmpInvalid("The creationData.sourceUri is in storage account '%s' but storageAccountId names '%s'.",
				src.Account, acct)
		}

		rg = acctRG
	}

	if hasAccount == nil || !hasAccount(src.Account, rg) {
		return cmpInvalid("The storage account '%s' named by creationData.sourceUri does not exist.", src.Account)
	}

	return nil
}

func (p *ComputeProvider) checkSecureBlobs(blobs DiskBlobs, cd map[string]any, src BlobRef) error {
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

		if ref.Account != src.Account {
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
