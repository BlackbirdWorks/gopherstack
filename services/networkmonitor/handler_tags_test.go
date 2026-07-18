package networkmonitor_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doNMRequest(t, h, http.MethodPost, "/monitors", map[string]any{
		"monitorName": "tagged",
		"tags":        map[string]any{"env": "prod"},
	})

	if rr.Code != http.StatusOK {
		t.Fatalf("create: status %d", rr.Code)
	}

	var mon map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &mon); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	monARN, _ := mon["monitorArn"].(string)
	if monARN == "" {
		t.Fatal("expected monitorArn in response")
	}

	rr = doNMRequest(t, h, http.MethodGet, "/tags/"+monARN, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list tags: status %d", rr.Code)
	}

	var tagResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &tagResp); err != nil {
		t.Fatalf("unmarshal tags: %v", err)
	}

	tags, _ := tagResp["tags"].(map[string]any)
	if tags["env"] != "prod" {
		t.Errorf("tag env: got %v, want prod", tags["env"])
	}

	rr = doNMRequest(t, h, http.MethodPost, "/tags/"+monARN, map[string]any{
		"tags": map[string]any{"team": "sre"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("tag: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodDelete, "/tags/"+monARN+"?tagKeys=env", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("untag: status %d", rr.Code)
	}

	rr = doNMRequest(t, h, http.MethodGet, "/tags/"+monARN, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &tagResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	tags, _ = tagResp["tags"].(map[string]any)

	if _, ok := tags["env"]; ok {
		t.Error("env tag should be removed")
	}

	if tags["team"] != "sre" {
		t.Errorf("team tag: got %v, want sre", tags["team"])
	}
}
