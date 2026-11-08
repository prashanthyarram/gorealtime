// Command chat shows the boundary between an application and gorealtime.
//
// The process keeps an in-memory slice of lines. That slice is the stand-in
// for a database. gorealtime only routes events. A client presents a demo
// token the server already knows; the handler maps that token to an identity.
// It does not accept a user id from the payload. Unknown tokens are rejected.
// This is not a chat product and the tokens are fixtures, not a login system.
//
//	{"v":1,"type":"event","event":"room.hello","payload":{"token":"demo-alice","room":"general"}}
//	{"v":1,"type":"event","event":"room.say","payload":{"room":"general","text":"hello"}}
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/prashanthyarram/gorealtime"
)

// tokens are server-side fixtures. A production handler would read an
// authenticated session that middleware already established.
var tokens = map[string]string{
	"demo-alice": "alice",
	"demo-bob":   "bob",
}

type line struct {
	User string
	Text string
}

var (
	storeMu sync.Mutex
	store   []line
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	srv.On("room.hello", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		var body struct {
			Token string `json:"token"`
			Room  string `json:"room"`
		}
		if err := json.Unmarshal(ev.Payload, &body); err != nil {
			return gorealtime.ErrInvalidPayload
		}
		user, ok := tokens[body.Token]
		if !ok {
			return gorealtime.WithMessage(gorealtime.ErrUnauthorized, "unknown token")
		}
		if err := conn.SetIdentity(user); err != nil {
			return err
		}
		if err := conn.Join(body.Room); err != nil {
			return err
		}
		return conn.Send(ctx, "room.joined", map[string]string{"room": body.Room, "user": user})
	})
	srv.On("room.say", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		if conn.Identity() == "" {
			return gorealtime.WithMessage(gorealtime.ErrUnauthorized, "join first")
		}
		var body struct {
			Room string `json:"room"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(ev.Payload, &body); err != nil || body.Text == "" || len(body.Text) > 500 {
			return gorealtime.ErrInvalidPayload
		}
		storeMu.Lock()
		store = append(store, line{User: conn.Identity(), Text: body.Text})
		storeMu.Unlock()
		return srv.SendToGroup(ctx, body.Room, "room.message", map[string]string{
			"user": conn.Identity(),
			"text": body.Text,
		})
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
