package glacier

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleUploadArchive(c *echo.Context, vaultName string, body []byte) error {
	// Enforce 4 GiB single-upload limit before allocating.
	if int64(len(body)) > maxSingleUploadBytes {
		return h.writeError(c, http.StatusRequestEntityTooLarge, "InvalidParameterValueException",
			"archive exceeds maximum single-upload size of 4 GiB")
	}

	description := c.Request().Header.Get("X-Amz-Archive-Description")
	if err := validateDescription(description); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	clientChecksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	// Compute the real tree-hash from the body.
	computed := computeTreeHash(body)

	// If the client supplied a checksum, verify it matches.
	if clientChecksum != "" {
		if len(clientChecksum) != sha256HexLen {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash must be a 64-character hex string")
		}

		if _, hexErr := hex.DecodeString(clientChecksum); hexErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash contains invalid hex characters")
		}

		if clientChecksum != computed {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash mismatch: computed "+computed)
		}
	}

	checksum := computed
	if clientChecksum != "" {
		checksum = clientChecksum
	}

	a, err := h.Backend.UploadArchive(
		h.AccountID, h.DefaultRegion, vaultName, description, checksum, int64(len(body)), body,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/archives/" + a.ArchiveID

	c.Response().Header().Set("X-Amz-Archive-Id", a.ArchiveID)
	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", a.SHA256TreeHash)
	c.Response().Header().Set("Location", location)

	return c.JSON(http.StatusCreated, uploadArchiveResponse{
		ArchiveID: a.ArchiveID,
		Checksum:  a.SHA256TreeHash,
		Location:  location,
	})
}

// computeLeafHashes returns SHA-256 hashes of successive 1 MiB blocks of data.
func computeLeafHashes(data []byte) [][]byte {
	if len(data) == 0 {
		h := sha256.Sum256(nil)

		return [][]byte{h[:]}
	}

	var hashes [][]byte

	for i := 0; i < len(data); i += treeHashLeafSize {
		end := min(i+treeHashLeafSize, len(data))
		sum := sha256.Sum256(data[i:end])
		hashes = append(hashes, sum[:])
	}

	return hashes
}

// reduceTreeHashes iteratively pair-hashes adjacent entries until one remains.
func reduceTreeHashes(hashes [][]byte) []byte {
	const pairStep = 2

	for len(hashes) > 1 {
		next := make([][]byte, 0, (len(hashes)+1)/pairStep)

		for i := 0; i < len(hashes); i += pairStep {
			if i+1 >= len(hashes) {
				next = append(next, hashes[i])

				continue
			}

			combined := make([]byte, len(hashes[i])+len(hashes[i+1]))
			copy(combined, hashes[i])
			copy(combined[len(hashes[i]):], hashes[i+1])
			sum := sha256.Sum256(combined)
			next = append(next, sum[:])
		}

		hashes = next
	}

	return hashes[0]
}

// computeTreeHash returns the SHA-256 tree-hash of data as a lowercase hex string.
func computeTreeHash(data []byte) string {
	leaves := computeLeafHashes(data)

	return hex.EncodeToString(reduceTreeHashes(leaves))
}

func (h *Handler) handleDeleteArchive(c *echo.Context, vaultName, archiveID string) error {
	if err := h.Backend.DeleteArchive(h.AccountID, h.DefaultRegion, vaultName, archiveID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
