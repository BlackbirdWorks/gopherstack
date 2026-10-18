// Command hello is a minimal gopherstack extension: it serves /hello and observes S3 requests.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

const (
	hookTimeoutMs = 500
	headerTimeout = 5 * time.Second
)

type hookRequest struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "hello from the %s extension\n", os.Getenv("GOPHERSTACK_EXTENSION_NAME"))
	})
	mux.HandleFunc("/hooks/startup", func(http.ResponseWriter, *http.Request) { log.Println("on_startup") })
	mux.HandleFunc("/hooks/shutdown", func(http.ResponseWriter, *http.Request) { log.Println("on_shutdown") })
	mux.HandleFunc("/hooks/s3/pre", func(_ http.ResponseWriter, r *http.Request) {
		var req hookRequest

		_ = json.NewDecoder(r.Body).Decode(&req)
		log.Printf("s3 request %q %q", req.Method, req.Path)
	})

	addr := "127.0.0.1:" + os.Getenv("GOPHERSTACK_EXTENSION_PORT")

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: headerTimeout}

	go func() { log.Fatal(srv.Serve(l)) }()

	if err = register(); err != nil {
		log.Fatal(err)
	}

	select {}
}

func register() error {
	body, err := json.Marshal(map[string]any{
		"version": "0.1.0",
		"routes":  map[string]any{"path_prefixes": []string{"/hello"}},
		"hooks": map[string]any{
			"on_startup":  "/hooks/startup",
			"on_shutdown": "/hooks/shutdown",
			"request": []map[string]any{
				{"services": []string{"S3"}, "pre": "/hooks/s3/pre", "timeout_ms": hookTimeoutMs},
			},
		},
	})
	if err != nil {
		return err
	}

	url := os.Getenv("GOPHERSTACK_CONTROL_URL") + "/_gopherstack/extensions/register"

	req, err := http.NewRequestWithContext( //nolint:gosec // URL from gopherstack env
		context.Background(),
		http.MethodPost,
		url,
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+os.Getenv("GOPHERSTACK_EXTENSION_TOKEN"))

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // loopback control API
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register failed: %s", resp.Status) //nolint:err113 // example
	}

	return nil
}
