package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestRoundTripEvent(t *testing.T) {
	in := Envelope{
		V:       Version,
		Type:    TypeEvent,
		Event:   "chat.send",
		ID:      "evt-1",
		Corr:    "corr-1",
		Origin:  "ignored-by-server",
		Trace:   map[string]string{"traceparent": "00-abc"},
		Payload: json.RawMessage(`{"text":"hi"}`),
	}
	b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeClient(b, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if out.Event != "chat.send" || out.ID != "evt-1" || string(out.Payload) != `{"text":"hi"}` {
		t.Fatalf("%+v", out)
	}
}

func TestDecodeClientRejects(t *testing.T) {
	cases := []struct {
		name string
		body string
		max  int
		err  error
	}{
		{"empty", ``, 100, ErrPayload},
		{"junk", `{`, 100, ErrPayload},
		{"trailing", `{"v":1}{}`, 100, ErrPayload},
		{"version", `{"v":2,"type":"event","event":"a"}`, 100, ErrVersion},
		{"missing version", `{"type":"event","event":"a"}`, 100, ErrVersion},
		{"hello from client", `{"v":1,"type":"hello"}`, 100, ErrType},
		{"error from client", `{"v":1,"type":"error"}`, 100, ErrType},
		{"bye from client", `{"v":1,"type":"bye"}`, 100, ErrType},
		{"bad event", `{"v":1,"type":"event","event":"has space"}`, 100, ErrEvent},
		{"empty event", `{"v":1,"type":"event","event":""}`, 100, ErrEvent},
		{"bad id", `{"v":1,"type":"event","event":"a","id":"no spaces"}`, 100, ErrField},
		{"ack without corr", `{"v":1,"type":"ack"}`, 100, ErrField},
		{"payload junk", `{"v":1,"type":"event","event":"a","payload":"unterminated}`, 100, ErrPayload},
		{"payload not json", `{"v":1,"type":"event","event":"a","payload":not}`, 100, ErrPayload},
		{"trace control", `{"v":1,"type":"event","event":"a","trace":{"k":"bad\n"}}`, 100, ErrField},
		{"too many trace", `{"v":1,"type":"event","event":"a","trace":{"a":"1","b":"2","c":"3","d":"4","e":"5"}}`, 100, ErrField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tc.body), tc.max)
			if !errors.Is(err, tc.err) {
				t.Fatalf("err %v", err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
}

func TestPayloadLimit(t *testing.T) {
	body := `{"v":1,"type":"event","event":"a","payload":"` + strings.Repeat("x", 50) + `"}`
	if _, err := DecodeClient([]byte(body), 40); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err %v", err)
	}
	if _, err := DecodeClient([]byte(body), 80); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownFieldIgnored(t *testing.T) {
	env, err := DecodeClient([]byte(`{"v":1,"type":"event","event":"a.b","future":true}`), 100)
	if err != nil {
		t.Fatal(err)
	}
	if env.Event != "a.b" {
		t.Fatal(env.Event)
	}
}

func TestValidateEventName(t *testing.T) {
	if err := ValidateEventName("chat.send"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEventName(""); !errors.Is(err, ErrEvent) {
		t.Fatal(err)
	}
	if err := ValidateEventName(strings.Repeat("a", 129)); !errors.Is(err, ErrEvent) {
		t.Fatal(err)
	}
}

func TestAckRoundTrip(t *testing.T) {
	b, err := Encode(Envelope{Type: TypeAck, ID: "a1", Corr: "e1"})
	if err != nil {
		t.Fatal(err)
	}
	env, err := DecodeClient(b, 100)
	if err != nil {
		t.Fatal(err)
	}
	if env.V != Version || env.Corr != "e1" {
		t.Fatalf("%+v", env)
	}
}
