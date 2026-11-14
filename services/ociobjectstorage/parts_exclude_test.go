package ociobjectstorage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestCommitPartsToExclude(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		commit  []int
		exclude []int
	}{
		{name: "exclude one", commit: []int{1}, exclude: []int{2}},
		{name: "uncovered part", commit: []int{1}, wantErr: "InvalidUploadPart"},
		{name: "both lists", commit: []int{1, 2}, exclude: []int{2}, wantErr: "InvalidUploadPart"},
		{name: "unknown exclude", commit: []int{1, 2}, exclude: []int{9}, wantErr: "InvalidUploadPart"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ns, pre := oci.DefaultNamespace, oci.Preconds{}
			b := oci.NewInMemoryBackend()
			_, err := b.CreateBucket(
				ns,
				oci.CreateBucketInput{Name: "bk", CompartmentID: "ocid1.compartment.oc1..c"},
				"",
			)
			require.NoError(t, err)

			u, err := b.CreateUpload(ns, "bk", oci.CreateUploadInput{Object: "o"}, pre)
			require.NoError(t, err)

			etags := map[int]string{}

			for n, d := range map[int]string{1: "aa", 2: "bb"} {
				etags[n], _, err = b.UploadPart(ns, "bk", "o", u.ID, n, []byte(d), "")
				require.NoError(t, err)
			}

			in := oci.CommitInput{PartsToExclude: tt.exclude}
			for _, n := range tt.commit {
				in.PartsToCommit = append(in.PartsToCommit, oci.CommitPart{Etag: etags[n], PartNum: n})
			}

			_, err = b.CommitUpload(ns, "bk", "o", u.ID, in, pre)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
