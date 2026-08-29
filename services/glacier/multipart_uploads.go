package glacier

import (
	"sort"
	"time"
)

const (
	// multipartUploadIDLength is the length of the random multipart upload ID.
	multipartUploadIDLength = 60
	// minMultipartPartSize is the minimum part size for multipart uploads (1 MiB).
	minMultipartPartSize = 1 << 20
	// maxMultipartPartSize is the maximum part size for multipart uploads (4 GiB).
	maxMultipartPartSize = 4 << 30
)

// uploadKey uniquely identifies a multipart upload, and remains the key type
// for the (still-raw) multipartParts map -- see store_setup.go's package doc
// for why multipartParts wasn't converted to a *store.Table.
type uploadKey struct {
	AccountID string `json:"accountID"`
	Region    string `json:"region"`
	VaultName string `json:"vaultName"`
	UploadID  string `json:"uploadID"`
}

// isPowerOfTwo reports whether n is a power of two (n > 0).
func isPowerOfTwo(n int64) bool {
	return n > 0 && (n&(n-1)) == 0
}

// InitiateMultipartUpload begins a multipart upload for a vault.
func (b *InMemoryBackend) InitiateMultipartUpload(
	accountID, region, vaultName, description string,
	partSize int64,
) (*MultipartUpload, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return nil, ErrVaultNotFound
	}

	// Part size must be a power of 2 between 1 MiB and 4 GiB (inclusive).
	if partSize != 0 &&
		(!isPowerOfTwo(partSize) || partSize < minMultipartPartSize || partSize > maxMultipartPartSize) {
		return nil, ErrValidation
	}

	uploadID := generateID(multipartUploadIDLength)
	up := &MultipartUpload{
		MultipartUploadID:  uploadID,
		VaultARN:           v.VaultARN,
		ArchiveDescription: description,
		PartSizeInBytes:    partSize,
		CreationDate:       formatDate(time.Now()),
	}

	b.multipartUploads.Put(up)

	return up, nil
}

// UploadMultipartPart records a part for an in-progress multipart upload.
func (b *InMemoryBackend) UploadMultipartPart(
	accountID, region, vaultName, uploadID, rangeHeader, checksum string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
	}

	if !b.multipartUploads.Has(multipartUploadKey(vArn, uploadID)) {
		return ErrUploadNotFound
	}

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	b.multipartParts[uKey] = append(b.multipartParts[uKey], MultipartPart{
		RangeInBytes:   rangeHeader,
		SHA256TreeHash: checksum,
	})

	return nil
}

// CompleteMultipartUpload finalises a multipart upload and creates an archive.
func (b *InMemoryBackend) CompleteMultipartUpload(
	accountID, region, vaultName, uploadID, checksum string,
	archiveSize int64,
) (*Archive, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	v, ok := b.vaults.Get(vArn)
	if !ok {
		return nil, ErrVaultNotFound
	}

	upKey := multipartUploadKey(vArn, uploadID)

	up, ok := b.multipartUploads.Get(upKey)
	if !ok {
		return nil, ErrUploadNotFound
	}

	archiveID := generateID(archiveIDLength)
	a := &Archive{
		ArchiveID:      archiveID,
		Description:    up.ArchiveDescription,
		CreationDate:   formatDate(time.Now()),
		Size:           archiveSize,
		SHA256TreeHash: checksum,
	}

	if v.Archives == nil {
		v.Archives = make(map[string]*Archive)
	}

	v.Archives[archiveID] = a
	v.NumberOfArchives++
	v.SizeInBytes += archiveSize

	b.multipartUploads.Delete(upKey)

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	delete(b.multipartParts, uKey)

	return a, nil
}

// AbortMultipartUpload cancels an in-progress multipart upload.
func (b *InMemoryBackend) AbortMultipartUpload(accountID, region, vaultName, uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return ErrVaultNotFound
	}

	upKey := multipartUploadKey(vArn, uploadID)

	if !b.multipartUploads.Has(upKey) {
		return ErrUploadNotFound
	}

	b.multipartUploads.Delete(upKey)

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	delete(b.multipartParts, uKey)

	return nil
}

// ListMultipartUploads returns all in-progress multipart uploads for a vault.
func (b *InMemoryBackend) ListMultipartUploads(accountID, region, vaultName string) []*MultipartUpload {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ups := b.multipartUploadsByVault.Get(vaultARN(accountID, region, vaultName))

	result := make([]*MultipartUpload, 0, len(ups))
	for _, up := range ups {
		cp := *up
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MultipartUploadID < result[j].MultipartUploadID
	})

	return result
}

// ListParts returns the parts for an in-progress multipart upload.
func (b *InMemoryBackend) ListParts(
	accountID, region, vaultName, uploadID string,
) (*ListPartsOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return nil, ErrVaultNotFound
	}

	up, ok := b.multipartUploads.Get(multipartUploadKey(vArn, uploadID))
	if !ok {
		return nil, ErrUploadNotFound
	}

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	stored := b.multipartParts[uKey]
	parts := make([]MultipartPart, len(stored))
	copy(parts, stored)

	// Sort parts by their byte-range start value for deterministic output.
	sort.Slice(parts, func(i, j int) bool {
		return rangeStart(parts[i].RangeInBytes) < rangeStart(parts[j].RangeInBytes)
	})

	return &ListPartsOutput{
		MultipartUploadID:  uploadID,
		VaultARN:           up.VaultARN,
		ArchiveDescription: up.ArchiveDescription,
		PartSizeInBytes:    up.PartSizeInBytes,
		CreationDate:       up.CreationDate,
		Parts:              parts,
	}, nil
}

// rangeStart parses the byte start from a Content-Range header value (e.g. "0-1048575/*").
// Returns 0 on parse failure to maintain stable sort behaviour.
func rangeStart(rangeHeader string) int64 {
	for i := range len(rangeHeader) {
		if rangeHeader[i] == '-' || rangeHeader[i] == '/' {
			n := int64(0)
			for j := range i {
				if rangeHeader[j] < '0' || rangeHeader[j] > '9' {
					return 0
				}

				n = n*10 + int64(rangeHeader[j]-'0') //nolint:mnd // decimal digit extraction
			}

			return n
		}
	}

	return 0
}

// AddMultipartUploadInternal adds an in-progress multipart upload directly to the backend for testing.
// VaultARN is always recomputed from the accountID/region/vaultName parameters -- see the
// AddVaultInternal doc comment for why.
func (b *InMemoryBackend) AddMultipartUploadInternal(accountID, region, vaultName string, up *MultipartUpload) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *up
	cp.VaultARN = vaultARN(accountID, region, vaultName)
	b.multipartUploads.Put(&cp)
}

// AddMultipartPartInternal adds an uploaded part directly to the backend for testing,
// bypassing the real byte-range upload + tree-hash computation.
func (b *InMemoryBackend) AddMultipartPartInternal(accountID, region, vaultName, uploadID string, part MultipartPart) {
	b.mu.Lock()
	defer b.mu.Unlock()

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	b.multipartParts[uKey] = append(b.multipartParts[uKey], part)
}
