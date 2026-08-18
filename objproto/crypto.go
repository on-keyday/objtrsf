package objproto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"io"
	"time"

	"github.com/on-keyday/objtrsf/objproto/packet"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const AES128GCM = packet.CommonKeyKind_Aes128Gcm
const AES192GCM = packet.CommonKeyKind_Aes192Gcm
const AES256GCM = packet.CommonKeyKind_Aes256Gcm
const ChaCha20Poly1305 = packet.CommonKeyKind_Chacha20Poly1305

func NewHandshake(key []byte, kind packet.KeyKind, commonKeyKind packet.CommonKeyKind, offset uint16) (*packet.Handshake, error) {
	probe := &packet.Handshake{
		KeyKind:       kind,
		CommonKeyKind: commonKeyKind,
		Len:           uint16(len(key)),
		KeyShare:      key,
	}
	if kind == packet.KeyKind_Offset {
		probe.SetOffset(offset)
	}
	return probe, nil
}

func NewECDHHandshake(curve ecdh.Curve, commonKeyKind packet.CommonKeyKind) ([]byte, *packet.Handshake, error) {
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	kind := packet.KeyKind_Offset
	switch curve {
	case ecdh.X25519():
		kind = packet.KeyKind_X25519
	case ecdh.P256():
		kind = packet.KeyKind_P256
	case ecdh.P384():
		kind = packet.KeyKind_P384
	case ecdh.P521():
		kind = packet.KeyKind_P521
	default:
		return nil, nil, fmt.Errorf("unsupported curve: %v", curve)
	}
	probeData, err := NewHandshake(priv.PublicKey().Bytes(), kind, commonKeyKind, 0)
	if err != nil {
		return nil, nil, err
	}
	return priv.Bytes(), probeData, nil
}

func CurveFromKeyKind(kind packet.KeyKind) (ecdh.Curve, error) {
	switch kind {
	case packet.KeyKind_X25519:
		return ecdh.X25519(), nil
	case packet.KeyKind_P256:
		return ecdh.P256(), nil
	case packet.KeyKind_P384:
		return ecdh.P384(), nil
	case packet.KeyKind_P521:
		return ecdh.P521(), nil
	default:
		return nil, fmt.Errorf("unsupported key kind: %v", kind)
	}
}

func DoECDHHandshake(ctx context.Context, sess Endpoint, cid ConnectionID, curve ecdh.Curve, commonKeyKind packet.CommonKeyKind) (Connection, error) {
	priv, probe, err := NewECDHHandshake(curve, commonKeyKind)
	if err != nil {
		return nil, err
	}
	ch, err := sess.SendHandshake(cid, priv, probe)
	if err != nil {
		return nil, err
	}
	active, err := ch.WaitWithTimeout(ctx, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return active, nil
}

func DeriveKey(secret []byte, context string, keyLen int) (key []byte, err error) {
	hash := sha512.New
	hkdf := hkdf.New(hash, secret, nil, []byte(context))
	key = make([]byte, keyLen)
	_, err = io.ReadFull(hkdf, key)
	return key, err
}

func aeadKeyLen(kind packet.CommonKeyKind) (int, error) {
	switch kind {
	case packet.CommonKeyKind_Aes128Gcm:
		return 16, nil
	case packet.CommonKeyKind_Aes192Gcm:
		return 24, nil
	case packet.CommonKeyKind_Aes256Gcm, packet.CommonKeyKind_Chacha20Poly1305:
		return 32, nil
	default:
		return 0, fmt.Errorf("unsupported common key kind: %v", kind)
	}
}

// NewAEADFromCommonKeyKind requires an exactly-sized key. It used to accept any
// key at least as long as the negotiated kind needed and hand the whole slice
// to aes.NewCipher, so a negotiated aes128_gcm silently ran AES-256.
func NewAEADFromCommonKeyKind(kind packet.CommonKeyKind, key []byte) (cipher.AEAD, error) {
	want, err := aeadKeyLen(kind)
	if err != nil {
		return nil, err
	}
	if len(key) != want {
		return nil, fmt.Errorf("invalid key length for %v: got %d, want %d", kind, len(key), want)
	}
	if kind == packet.CommonKeyKind_Chacha20Poly1305 {
		return chacha20poly1305.New(key)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create %v cipher: %w", kind, err)
	}
	return cipher.NewGCM(block)
}

// phaseKeys is one direction's traffic keying material for a single key phase.
// secret is what ratchets forward; key and iv are derived from it and are
// discarded wholesale when the phase advances.
type phaseKeys struct {
	secret []byte
	aead   cipher.AEAD
	iv     []byte
}

func derivePhaseKeys(secret []byte, kind packet.CommonKeyKind) (phaseKeys, error) {
	keyLen, err := aeadKeyLen(kind)
	if err != nil {
		return phaseKeys{}, err
	}
	key, err := DeriveKey(secret, "ksdk-protocol-key", keyLen)
	if err != nil {
		return phaseKeys{}, fmt.Errorf("failed to derive phase key: %w", err)
	}
	iv, err := DeriveKey(secret, "ksdk-protocol-nonce", 12)
	if err != nil {
		return phaseKeys{}, fmt.Errorf("failed to derive phase iv: %w", err)
	}
	aead, err := NewAEADFromCommonKeyKind(kind, key)
	if err != nil {
		return phaseKeys{}, err
	}
	clear(key)
	return phaseKeys{secret: secret, aead: aead, iv: iv}, nil
}

func ratchetSecret(secret []byte) ([]byte, error) {
	return DeriveKey(secret, "ksdk-protocol-ku", 32)
}

func ECDHFromHandshake(selfPrivate []byte, probe *packet.Handshake) ([]byte, packet.CommonKeyKind, error) {
	curve, err := CurveFromKeyKind(probe.KeyKind)
	if err != nil {
		return nil, 0, err
	}
	peerPub, err := curve.NewPublicKey(probe.KeyShare)
	if err != nil {
		return nil, 0, err
	}
	selfPriv, err := curve.NewPrivateKey(selfPrivate)
	if err != nil {
		return nil, 0, err
	}
	shared, err := selfPriv.ECDH(peerPub)
	if err != nil {
		return nil, 0, err
	}
	return shared, probe.CommonKeyKind, nil
}
