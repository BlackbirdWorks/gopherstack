package azureblob_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func seedContainers(t *testing.T, env *sdkEnv, names ...string) {
	t.Helper()

	for _, n := range names {
		opts := &container.CreateOptions{Metadata: map[string]*string{"owner": new(n)}}

		_, err := env.svc.CreateContainer(t.Context(), n, opts)
		require.NoError(t, err)
	}
}

func TestSDK_ListContainers_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		wantPages [][]string
		maxResult int32
	}{
		{name: "all_one_page", wantPages: [][]string{{"a1", "a2", "a3", "b1", "b2", "sdkenv"}}},
		{name: "prefix", prefix: "a", wantPages: [][]string{{"a1", "a2", "a3"}}},
		{name: "prefix_paged", prefix: "a", maxResult: 2, wantPages: [][]string{{"a1", "a2"}, {"a3"}}},
		{name: "exact_fit", prefix: "b", maxResult: 2, wantPages: [][]string{{"b1", "b2"}}},
		{name: "no_match", prefix: "zzz", wantPages: [][]string{{}}},
		{name: "size_one", prefix: "b", maxResult: 1, wantPages: [][]string{{"b1"}, {"b2"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			seedContainers(t, env, "b2", "a2", "a1", "b1", "a3")

			opts := &azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}}
			if tt.prefix != "" {
				opts.Prefix = new(tt.prefix)
			}

			if tt.maxResult > 0 {
				opts.MaxResults = new(tt.maxResult)
			}

			pager := env.svc.NewListContainersPager(opts)

			var pages [][]string

			for pager.More() {
				page, err := pager.NextPage(t.Context())
				require.NoError(t, err)

				names := make([]string, 0, len(page.ContainerItems))

				for _, item := range page.ContainerItems {
					names = append(names, *item.Name)

					if *item.Name != "sdkenv" {
						assert.Equal(t, *item.Name, *item.Metadata["owner"], "include=metadata returns metadata")
					}
				}

				pages = append(pages, names)

				if tt.maxResult > 0 {
					assert.Equal(t, tt.maxResult, *page.MaxResults)
				}

				if tt.prefix != "" {
					assert.Equal(t, tt.prefix, *page.Prefix)
				}
			}

			assert.Equal(t, tt.wantPages, pages)
		})
	}
}

func TestSDK_ListContainers_MetadataOmittedByDefault(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	seedContainers(t, env, "m1")

	pager := env.svc.NewListContainersPager(&azblob.ListContainersOptions{Prefix: new("m")})
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.ContainerItems, 1)
	assert.Empty(t, page.ContainerItems[0].Metadata)
}

func TestListContainers_InvalidParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "maxresults_zero", query: "maxresults=0", want: http.StatusBadRequest},
		{name: "maxresults_text", query: "maxresults=abc", want: http.StatusBadRequest},
		{name: "bad_marker", query: "marker=***", want: http.StatusBadRequest},
		{name: "maxresults_clamped", query: "maxresults=999999", want: http.StatusOK},
		{name: "include_system_deleted", query: "include=metadata,deleted,system", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				env.url+"?comp=list&"+tt.query, http.NoBody)
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}
