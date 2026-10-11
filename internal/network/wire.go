package network

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/crypto/hkdf"
)

const (
	AuthProtocol         protocol.ID = "/HappySync/auth/1.0.0"
	ManifestProtocol     protocol.ID = "/HappySync/manifest/1.0.0"
	BlockProtocol        protocol.ID = "/HappySync/blocks/1.0.0"
	AvailabilityProtocol protocol.ID = "/HappySync/availability/1.0.0"
	DiscoveryProtocol    protocol.ID = "/HappySync/discovery/1.0.0"
	ControlProtocol      protocol.ID = "/HappySync/control/1.0.0"
	maxFrame                         = 16 << 20
	maxPage                          = 128
)

func writeFrame(w io.Writer, value any, maximum int) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(b) > maximum {
		return errors.New("message exceeds protocol limit")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	if _, err = w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
func readFrame(r io.Reader, value any, maximum int) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > uint32(maximum) {
		return errors.New("invalid message length")
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid protocol message")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing protocol data")
	}
	return nil
}
func derive(secret, syncID []byte, domain string) []byte {
	out := make([]byte, 32)
	_, _ = io.ReadFull(hkdf.New(sha256.New, secret, syncID, []byte(domain)), out)
	return out
}

type authMessage struct {
	Version int    `json:"version"`
	Nonce   []byte `json:"nonce,omitempty"`
	MAC     []byte `json:"mac,omitempty"`
}

func authMAC(key []byte, s network.Stream, client bool, clientNonce, serverNonce []byte, role string) []byte {
	initiator, responder := s.Conn().RemotePeer().String(), s.Conn().LocalPeer().String()
	if client {
		initiator, responder = responder, initiator
	}
	mac := hmac.New(sha256.New, key)
	// Length-prefixed transcript fields prevent ambiguous concatenations.
	for _, v := range [][]byte{[]byte("HappySync/v1/auth"), []byte(s.Protocol()), []byte(initiator), []byte(responder), clientNonce, serverNonce, []byte(role)} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(v)))
		mac.Write(n[:])
		mac.Write(v)
	}
	return mac.Sum(nil)
}
func authenticate(s network.Stream, key []byte, client bool) error {
	_ = s.SetDeadline(time.Now().Add(15 * time.Second))
	defer s.SetDeadline(time.Time{})
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	if client {
		var hello authMessage
		if err := readFrame(s, &hello, 1024); err != nil {
			return err
		}
		if hello.Version != 1 || len(hello.Nonce) != 32 || len(hello.MAC) != 0 {
			return errors.New("incompatible authentication")
		}
		proof := authMessage{Version: 1, Nonce: nonce, MAC: authMAC(key, s, true, nonce, hello.Nonce, "initiator")}
		if err := writeFrame(s, proof, 1024); err != nil {
			return err
		}
		var reply authMessage
		if err := readFrame(s, &reply, 1024); err != nil {
			return err
		}
		if reply.Version != 1 || len(reply.Nonce) != 0 || !hmac.Equal(reply.MAC, authMAC(key, s, true, nonce, hello.Nonce, "responder")) {
			return errors.New("group authentication failed")
		}
		return nil
	}
	if err := writeFrame(s, authMessage{Version: 1, Nonce: nonce}, 1024); err != nil {
		return err
	}
	var proof authMessage
	if err := readFrame(s, &proof, 1024); err != nil {
		return err
	}
	if proof.Version != 1 || len(proof.Nonce) != 32 || !hmac.Equal(proof.MAC, authMAC(key, s, false, proof.Nonce, nonce, "initiator")) {
		return errors.New("group authentication failed")
	}
	return writeFrame(s, authMessage{Version: 1, MAC: authMAC(key, s, false, proof.Nonce, nonce, "responder")}, 1024)
}
