package objproto

import (
	"bytes"
	"testing"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

func TestAEADKeyLen(t *testing.T) {
	cases := []struct {
		kind packet.CommonKeyKind
		want int
	}{
		{packet.CommonKeyKind_Aes128Gcm, 16},
		{packet.CommonKeyKind_Aes192Gcm, 24},
		{packet.CommonKeyKind_Aes256Gcm, 32},
		{packet.CommonKeyKind_Chacha20Poly1305, 32},
	}
	for _, c := range cases {
		got, err := aeadKeyLen(c.kind)
		if err != nil {
			t.Fatalf("%v: %v", c.kind, err)
		}
		if got != c.want {
			t.Fatalf("%v: key length %d, want %d", c.kind, got, c.want)
		}
	}
	if _, err := aeadKeyLen(packet.CommonKeyKind(0)); err == nil {
		t.Fatal("unknown key kind must be an error")
	}
}

func TestDerivePhaseKeysUsesTheNegotiatedKeyLength(t *testing.T) {
	// Regression: the old NewAEADFromCommonKeyKind handed a 32-byte slice to
	// aes.NewCipher for every AES kind, so a negotiated aes128_gcm silently
	// ran AES-256. A 12-byte GCM nonce holds for all of them, so assert on the
	// derived key length via the constructor's own length check instead.
	secret := bytes.Repeat([]byte{0x11}, 32)
	for _, kind := range []packet.CommonKeyKind{
		packet.CommonKeyKind_Aes128Gcm,
		packet.CommonKeyKind_Aes192Gcm,
		packet.CommonKeyKind_Aes256Gcm,
		packet.CommonKeyKind_Chacha20Poly1305,
	} {
		pk, err := derivePhaseKeys(secret, kind)
		if err != nil {
			t.Fatalf("%v: %v", kind, err)
		}
		if len(pk.iv) != 12 {
			t.Fatalf("%v: iv is %d bytes, want 12", kind, len(pk.iv))
		}
		if pk.aead.NonceSize() != 12 {
			t.Fatalf("%v: nonce size %d, want 12", kind, pk.aead.NonceSize())
		}
	}
	if _, err := NewAEADFromCommonKeyKind(packet.CommonKeyKind_Aes128Gcm, bytes.Repeat([]byte{1}, 32)); err == nil {
		t.Fatal("aes128_gcm must reject a 32-byte key instead of silently running AES-256")
	}
}

func TestRatchetSecretMovesForward(t *testing.T) {
	s0 := bytes.Repeat([]byte{0x22}, 32)
	s1, err := ratchetSecret(s0)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := ratchetSecret(s1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(s0, s1) || bytes.Equal(s1, s2) || bytes.Equal(s0, s2) {
		t.Fatal("ratchet produced a repeated secret")
	}
	if len(s1) != 32 || len(s2) != 32 {
		t.Fatalf("ratchet changed the secret length: %d %d", len(s1), len(s2))
	}
	again, err := ratchetSecret(s0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1, again) {
		t.Fatal("ratchet is not deterministic")
	}
}
