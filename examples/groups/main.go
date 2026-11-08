// Command groups is a small room server.
//
// A client sends {"v":1,"type":"event","event":"room.join","payload":{"room":"general"}}
// and then {"v":1,"type":"event","event":"room.say","payload":{"room":"general","text":"hi"}}.
// Other members of the room receive room.message. The sender does not.
// Joining is unrestricted. A real application checks membership before Join.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/prashanth-yarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	srv.On("room.join", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		var body struct {
			Room string `json:"room"`
		}
		if err := json.Unmarshal(ev.Payload, &body); err != nil {
			return gorealtime.ErrInvalidPayload
		}
		if err := conn.Join(body.Room); err != nil {
			return err
		}
		return conn.Send(ctx, "room.joined", map[string]string{"room": body.Room})
	})
	srv.On("room.say", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		var body struct {
			Room string `json:"room"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(ev.Payload, &body); err != nil || body.Text == "" {
			return gorealtime.ErrInvalidPayload
		}
		return srv.SendToGroup(ctx, body.Room, "room.message", map[string]string{
			"from": conn.ID(),
			"text": body.Text,
		}, gorealtime.ExcludeConnections(conn.ID()))
	})

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
