package token_test

import (
	"strings"
	"testing"
	"time"

	"github.com/rahul25012003/orderops/internal/platform/token"
)

func TestSignParseRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tok, err := token.Sign(token.Claims{Subject: "u1", Name: "Rahul",
		Role: "customer"}, "s3cr3t", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c, err := token.Parse(tok, "s3cr3t", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Subject != "u1" || c.Role != "customer" || c.Name != "Rahul" {
		t.Fatalf("claims round-tripped wrong: %+v", c)
	}
}

func TestWrongSecretRejected(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tok, _ := token.Sign(token.Claims{Subject: "u1"}, "right", now, time.Hour)
	if _, err := token.Parse(tok, "wrong", now); err != token.ErrSignature {
		t.Fatalf("want ErrSignature, got %v", err)
	}
}

func TestTamperedPayloadRejected(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tok, _ := token.Sign(token.Claims{Subject: "u1", Role: "customer"},
		"s3cr3t", now, time.Hour)
	parts := strings.Split(tok, ".")
	// forge an admin payload but keep the original signature
	forged, _ := token.Sign(token.Claims{Subject: "u1", Role: "admin"},
		"other", now, time.Hour)
	fp := strings.Split(forged, ".")
	tampered := parts[0] + "." + fp[1] + "." + parts[2]
	if _, err := token.Parse(tampered, "s3cr3t", now); err != token.ErrSignature {
		t.Fatalf("privilege-escalation tamper not caught: %v", err)
	}
}

func TestExpired(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tok, _ := token.Sign(token.Claims{Subject: "u1"}, "s", now, time.Minute)
	if _, err := token.Parse(tok, "s", now.Add(2*time.Minute)); err != token.ErrExpired {
		t.Fatalf("want ErrExpired, got %v", err)
	}
}

func TestMalformed(t *testing.T) {
	if _, err := token.Parse("not-a-jwt", "s", time.Now()); err != token.ErrMalformed {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestBearer(t *testing.T) {
	if got := token.Bearer("Bearer abc.def.ghi"); got != "abc.def.ghi" {
		t.Fatalf("bearer parse: %q", got)
	}
	if got := token.Bearer("bearer xyz"); got != "xyz" { // case-insensitive scheme
		t.Fatalf("bearer case: %q", got)
	}
	if got := token.Bearer("Basic abc"); got != "" {
		t.Fatalf("non-bearer should be empty: %q", got)
	}
}
