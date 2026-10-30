package azurearm_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIMConcurrentControlAndGateway(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<rate-limit calls="1000" renewal-period="60" />`, "", ""), http.StatusCreated)

	const workers, iters = 8, 15

	do := func(method, url, body string, hdr map[string]string) error {
		req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")

		for k, v := range hdr {
			req.Header.Set(k, v)
		}

		resp, err := e.hc.Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		_, err = io.Copy(io.Discard, resp.Body)

		return err
	}

	gwHdr := map[string]string{"Ocp-Apim-Subscription-Key": e.key}
	errs := make(chan error, workers*iters*7)

	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			for i := range iters {
				errs <- do(http.MethodGet, e.url+e.armPath("/apis"), "", nil)
				errs <- do(http.MethodGet, e.url+"/_apim/"+apimSvc+"/echo/items/1", "", gwHdr)

				body := fmt.Sprintf(`{"properties":{"displayName":"s%d-%d","scope":"/apis","state":"active"}}`, w, i)
				errs <- do(http.MethodPut, e.url+e.armPath(fmt.Sprintf("/subscriptions/sub-%d-%d", w, i)), body, nil)
			}
		})
	}

	wg.Wait()

	for range workers * 2 {
		wg.Go(func() {
			for range iters * 2 {
				errs <- do(http.MethodGet, e.url+e.armPath("/apis"), "", nil)
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	require.NotEmpty(t, e.key)
}
