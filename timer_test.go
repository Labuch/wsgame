package wsgame

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// timerEngine arme / annule le minuteur sur demande et diffuse chaque déclenchement.
type timerEngine struct{ fired atomic.Int32 }

// Enregistré une seule fois, comme dans main() : le registre n'est pas
// prévu pour être modifié pendant que des rooms tournent.
var testTimer = &timerEngine{}

func init() { RegisterEngine("test-timer", testTimer) }

func (e *timerEngine) OnRoomCreate(*RoomConfig)       {}
func (e *timerEngine) RoleFor(RoomConfig, int) string { return "" }
func (e *timerEngine) OnPlayerLeave(int, json.RawMessage) EngineResult {
	return EngineResult{}
}

func (e *timerEngine) HandleAction(ctx ActionContext) EngineResult {
	var a struct {
		ArmMs int  `json:"armMs"`
		Stop  bool `json:"stop"`
	}
	json.Unmarshal(ctx.RawPayload, &a)
	if a.Stop {
		return EngineResult{Timer: StopTimer}
	}
	return EngineResult{Timer: time.Duration(a.ArmMs) * time.Millisecond}
}

func (e *timerEngine) OnTimer(json.RawMessage) EngineResult {
	n := e.fired.Add(1)
	payload, _ := json.Marshal(map[string]int32{"fired": n})
	return EngineResult{
		NewState: payload,
		Messages: []Message{{Target: TargetAll, Payload: payload}},
	}
}

// dialTimerRoom ouvre une room et renvoie la connexion et le flux des
// déclenchements reçus (lus en tâche de fond : avec gorilla/websocket, une
// lecture expirée rend la connexion inutilisable).
func dialTimerRoom(t *testing.T, room string) (*websocket.Conn, <-chan int) {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/ws/{id}", wsHandler)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + room + "?token=t1&gamemode=test-timer"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	var welcome Envelope
	if err := conn.ReadJSON(&welcome); err != nil || welcome.Type != "welcome" {
		t.Fatalf("welcome attendu : %+v %v", welcome, err)
	}

	fires := make(chan int, 8)
	go func() {
		for {
			var env Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			var p struct{ Fired int }
			json.Unmarshal(env.Payload, &p)
			fires <- p.Fired
		}
	}()
	return conn, fires
}

// nextFire attend le prochain déclenchement, ou renvoie false après `within`.
func nextFire(fires <-chan int, within time.Duration) (int, bool) {
	select {
	case n := <-fires:
		return n, true
	case <-time.After(within):
		return 0, false
	}
}

func TestRoomTimer(t *testing.T) {
	conn, fires := dialTimerRoom(t, fmt.Sprintf("timer-%d", time.Now().UnixNano()))
	base := testTimer.fired.Load()

	// Armement simple.
	conn.WriteJSON(map[string]int{"armMs": 20})
	if n, ok := nextFire(fires, time.Second); !ok || n != int(base)+1 {
		t.Fatalf("déclenchement attendu (n=%d ok=%v)", n, ok)
	}

	// Un réarmement remplace le minuteur précédent : un seul déclenchement, tardif.
	conn.WriteJSON(map[string]int{"armMs": 20})
	conn.WriteJSON(map[string]int{"armMs": 250})
	if _, ok := nextFire(fires, 150*time.Millisecond); ok {
		t.Fatal("le premier minuteur aurait dû être remplacé")
	}
	if n, ok := nextFire(fires, time.Second); !ok || n != int(base)+2 {
		t.Fatalf("second déclenchement attendu (n=%d ok=%v)", n, ok)
	}

	// StopTimer annule.
	conn.WriteJSON(map[string]int{"armMs": 20})
	conn.WriteJSON(map[string]bool{"stop": true})
	if _, ok := nextFire(fires, 200*time.Millisecond); ok {
		t.Fatal("le minuteur aurait dû être annulé")
	}
	if got := testTimer.fired.Load() - base; got != 2 {
		t.Fatalf("%d déclenchements, attendu 2", got)
	}
}
