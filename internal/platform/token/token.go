// Package token: a dependency-free HS256 JSON Web Token. The auth service
// SIGNS tokens; the gateway VERIFIES them locally with the shared secret —
// no round-trip to auth on every request, which is how real API gateways
// do stateless auth. Deliberately small and readable rather than pulling a
// JWT library: the whole trust model fits on one screen.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Claims is the JWT payload OrderOps cares about.
type Claims struct {
	Subject  string `json:"sub"`  // user id
	Name     string `json:"name"` // display name
	Role     string `json:"role"` // customer | admin
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
}

var (
	// ErrMalformed means the token isn't three base64url segments.
	ErrMalformed = errors.New("malformed token")
	// ErrSignature means the HMAC didn't verify (tampered or wrong secret).
	ErrSignature = errors.New("bad signature")
	// ErrExpired means exp is in the past.
	ErrExpired = errors.New("token expired")
)

var b64 = base64.RawURLEncoding

// header is fixed — HS256 is the only algorithm we accept, which sidesteps
// the classic "alg: none" and algorithm-confusion attacks entirely.
var headerSegment = b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Sign mints a signed token for claims valid for ttl from now.
func Sign(c Claims, secret string, now time.Time, ttl time.Duration) (string, error) {
	c.IssuedAt = now.Unix()
	c.Expires = now.Add(ttl).Unix()
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	signing := headerSegment + "." + b64.EncodeToString(payload)
	return signing + "." + b64.EncodeToString(sign(signing, secret)), nil
}

// Parse verifies signature and expiry, returning the claims. Signature is
// checked in constant time before anything in the payload is trusted.
func Parse(tok, secret string, now time.Time) (Claims, error) {
	var c Claims
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return c, ErrMalformed
	}
	want := sign(parts[0]+"."+parts[1], secret)
	got, err := b64.DecodeString(parts[2])
	if err != nil {
		return c, ErrMalformed
	}
	if !hmac.Equal(want, got) {
		return c, ErrSignature
	}
	raw, err := b64.DecodeString(parts[1])
	if err != nil {
		return c, ErrMalformed
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, ErrMalformed
	}
	if now.Unix() >= c.Expires {
		return c, ErrExpired
	}
	return c, nil
}

func sign(signing, secret string) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(signing))
	return m.Sum(nil)
}

// Bearer pulls the token out of an "Authorization: Bearer <tok>" header.
func Bearer(header string) string {
	const p = "Bearer "
	if len(header) > len(p) && strings.EqualFold(header[:len(p)], p) {
		return strings.TrimSpace(header[len(p):])
	}
	return ""
}
