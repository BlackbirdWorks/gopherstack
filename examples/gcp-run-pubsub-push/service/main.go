// Command push-receiver is a tiny Cloud Run style service that accepts
// Pub/Sub push deliveries on POST /push and logs every message.
package main

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
)

type pushEnvelope struct {
	Subscription string `json:"subscription"`
	Message      struct {
		MessageID  string            `json:"messageId"`
		Data       string            `json:"data"`
		Attributes map[string]string `json:"attributes"`
	} `json:"message"`
}

type received struct {
	ID         string            `json:"id"`
	Data       string            `json:"data"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type receiver struct {
	mu   sync.Mutex
	msgs []received
}

func (r *receiver) push(w http.ResponseWriter, req *http.Request) {
	var env pushEnvelope
	if err := json.NewDecoder(req.Body).Decode(&env); err != nil {
		http.Error(w, "bad push envelope", http.StatusBadRequest)

		return
	}

	data, err := base64.StdEncoding.DecodeString(env.Message.Data)
	if err != nil {
		http.Error(w, "bad base64 data", http.StatusBadRequest)

		return
	}

	m := received{ID: env.Message.MessageID, Data: string(data), Attributes: env.Message.Attributes}

	r.mu.Lock()
	r.msgs = append(r.msgs, m)
	r.mu.Unlock()

	log.Printf("received pubsub message id=%s data=%q subscription=%s", m.ID, m.Data, env.Subscription)
	w.WriteHeader(http.StatusNoContent)
}

func (r *receiver) list(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	out := append([]received{}, r.msgs...)
	r.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := &receiver{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /push", r.push)
	mux.HandleFunc("GET /messages", r.list)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	log.Printf("push-receiver listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
