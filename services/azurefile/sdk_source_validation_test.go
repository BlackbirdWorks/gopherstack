package azurefile_test

import (
	"encoding/binary"
	"hash/crc64"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func azureCRC64(s string) uint64 {
	return crc64.Checksum([]byte(s), crc64.MakeTable(0x9A6C9329AC4BC9B5))
}

func crc64Bytes(s string) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, azureCRC64(s))

	return b
}

func TestSDK_PutRangeFromURLValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts *file.UploadRangeFromURLOptions
		name string
		code fileerror.Code
	}{
		{name: "matching crc64", opts: &file.UploadRangeFromURLOptions{
			SourceContentValidation: file.SourceContentValidationTypeCRC64(azureCRC64("3456")),
		}},
		{name: "mismatched crc64", opts: &file.UploadRangeFromURLOptions{
			SourceContentValidation: file.SourceContentValidationTypeCRC64(azureCRC64("zzzz")),
		}, code: "Crc64Mismatch"},
		{name: "if match crc64 fails", opts: &file.UploadRangeFromURLOptions{
			SourceModifiedAccessConditions: &file.SourceModifiedAccessConditions{
				SourceIfMatchCRC64: crc64Bytes("zzzz"),
			},
		}, code: "SourceConditionNotMet"},
		{name: "if none match crc64 fails", opts: &file.UploadRangeFromURLOptions{
			SourceModifiedAccessConditions: &file.SourceModifiedAccessConditions{
				SourceIfNoneMatchCRC64: crc64Bytes("3456"),
			},
		}, code: "SourceConditionNotMet"},
		{name: "if match crc64 passes", opts: &file.UploadRangeFromURLOptions{
			SourceModifiedAccessConditions: &file.SourceModifiedAccessConditions{
				SourceIfMatchCRC64: crc64Bytes("3456"),
			},
		}},
		{name: "bearer source authorization accepted", opts: &file.UploadRangeFromURLOptions{
			CopySourceAuthorization: new("Bearer token"),
		}},
		{name: "non bearer source authorization", opts: &file.UploadRangeFromURLOptions{
			CopySourceAuthorization: new("SharedKey a:b"),
		}, code: fileerror.InvalidHeaderValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			src := newFile(t, sc, "src.bin", 10)
			upload(t, src, 0, "0123456789")
			dst := newFile(t, sc, "dst.bin", 10)

			resp, err := dst.UploadRangeFromURL(ctx(t), src.URL(), 3, 0, 4, tt.opts)
			if tt.code != "" {
				requireCode(t, err, tt.code)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, crc64Bytes("3456"), resp.XMSContentCRC64)
		})
	}
}

func TestSDK_SourceTrailingDot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		allow bool
	}{
		{name: "trimmed by default"},
		{name: "kept when allowed", allow: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("dotshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			src := newFile(t, sc, "src.bin", 4)
			upload(t, src, 0, "abcd")
			newFile(t, sc, "dst.bin", 4)

			dst, err := file.NewClientWithSharedKeyCredential(sc.URL()+"/dst.bin", e.cred, &file.ClientOptions{
				AllowSourceTrailingDot: &tt.allow,
			})
			require.NoError(t, err)

			_, err = dst.UploadRangeFromURL(ctx(t), src.URL()+".", 0, 0, 4, nil)

			if tt.allow {
				requireCode(t, err, "CannotVerifyCopySource")

				return
			}

			require.NoError(t, err)
		})
	}
}
