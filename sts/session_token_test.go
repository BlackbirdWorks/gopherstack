package sts_test

import (
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/sts"
)

func TestGetSessionToken_DefaultDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	resp, err := backend.GetSessionToken(&sts.GetSessionTokenInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	creds := resp.GetSessionTokenResult.Credentials
	if !strings.HasPrefix(creds.AccessKeyID, "ASIA") {
		t.Errorf("expected ASIA prefix, got %q", creds.AccessKeyID)
	}
	if creds.SecretAccessKey == "" {
		t.Error("expected non-empty SecretAccessKey")
	}
	if creds.SessionToken == "" {
		t.Error("expected non-empty SessionToken")
	}

	exp, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		t.Fatalf("parse expiration: %v", err)
	}

	diff := time.Until(exp)
	if diff < 11*time.Hour || diff > 13*time.Hour {
		t.Errorf("expected ~12h expiration, got %v", diff)
	}
}

func TestGetSessionToken_CustomDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	resp, err := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 3600})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exp, err := time.Parse(time.RFC3339, resp.GetSessionTokenResult.Credentials.Expiration)
	if err != nil {
		t.Fatalf("parse expiration: %v", err)
	}

	diff := time.Until(exp)
	if diff < 59*time.Minute || diff > 61*time.Minute {
		t.Errorf("expected ~1h expiration, got %v", diff)
	}
}

func TestGetSessionToken_InvalidDuration(t *testing.T) {
	t.Parallel()
	backend := sts.NewInMemoryBackend()
	_, err := backend.GetSessionToken(&sts.GetSessionTokenInput{DurationSeconds: 100})
	if err == nil {
		t.Fatal("expected error for duration=100, got nil")
	}
	if !strings.Contains(err.Error(), "DurationSeconds") {
		t.Errorf("expected ErrInvalidDuration, got %v", err)
	}
}
