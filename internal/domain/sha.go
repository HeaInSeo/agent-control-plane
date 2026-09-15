package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrNotImmutableCommit is returned when a value that must be an exact,
// immutable commit identity is something else — a branch name, a symbolic
// ref, an abbreviated SHA, or empty.
var ErrNotImmutableCommit = errors.New("not an exact immutable commit sha")

// CommitSHA is a full 40-hex-character Git object name.
//
// The control plane never accepts an abbreviated or symbolic revision where a
// commit identity is required: "publish branch HEAD" is forbidden, "publish
// exact commit SHA" is required (CC1).
type CommitSHA string

// Validate reports whether the value is a full 40-hex-character commit name.
func (s CommitSHA) Validate() error {
	if s == "" {
		return fmt.Errorf("%w: empty", ErrNotImmutableCommit)
	}
	if len(s) != 40 {
		return fmt.Errorf("%w: %q has length %d, want 40", ErrNotImmutableCommit, string(s), len(s))
	}
	if _, err := hex.DecodeString(string(s)); err != nil {
		return fmt.Errorf("%w: %q is not hex", ErrNotImmutableCommit, string(s))
	}
	if strings.ToLower(string(s)) != string(s) {
		return fmt.Errorf("%w: %q must be lowercase hex", ErrNotImmutableCommit, string(s))
	}
	return nil
}

// Digest is a lowercase hex-encoded SHA-256 digest.
type Digest string

// Validate reports whether the value is a 64-hex-character digest.
func (d Digest) Validate() error {
	if d == "" {
		return errors.New("digest is empty")
	}
	if len(d) != 64 {
		return fmt.Errorf("digest %q has length %d, want 64", string(d), len(d))
	}
	if _, err := hex.DecodeString(string(d)); err != nil {
		return fmt.Errorf("digest %q is not hex", string(d))
	}
	if strings.ToLower(string(d)) != string(d) {
		return fmt.Errorf("digest %q must be lowercase hex", string(d))
	}
	return nil
}
