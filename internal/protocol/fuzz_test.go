package protocol

import (
	"encoding/json"
	"testing"
)

func FuzzDecodeClient(f *testing.F) {
	f.Add([]byte(`{"v":1,"type":"event","event":"ping","payload":{"n":1}}`))
	f.Add([]byte(`{"v":1,"type":"ack","corr":"abc"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte("null"))
	f.Fuzz(func(t *testing.T, data []byte) {
		env, err := DecodeClient(data, 32*1024)
		if err != nil {
			return
		}
		if env.V != Version {
			t.Fatalf("version %d", env.V)
		}
		out, err := Encode(env)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if !json.Valid(out) {
			t.Fatalf("invalid json %q", out)
		}
	})
}

func FuzzValidateEventName(f *testing.F) {
	f.Add("chat.message")
	f.Add("")
	f.Add("has space")
	f.Fuzz(func(t *testing.T, name string) {
		_ = ValidateEventName(name)
		_ = ValidateID(name)
	})
}
