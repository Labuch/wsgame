package wsgame

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// tickEngine : « start » démarre la boucle, « stop » l'arrête. Chaque tick
// incrémente un compteur dans l'état et le diffuse. Les départs sont comptés
// par étiquette de room (action « tag »), pour vérifier qu'une connexion
// remplacée ne compte pas.
type tickEngine struct {
	ticks  atomic.Int32
	mu     sync.Mutex
	leaves map[string]int
}

var testTick = &tickEngine{leaves: map[string]int{}}

func init() { RegisterEngine("test-tick", testTick) }

func (e *tickEngine) OnRoomCreate(c *RoomConfig)     { c.MaxPlayers = 4 }
func (e *tickEngine) RoleFor(RoomConfig, int) string { return "" }
func (e *tickEngine) TickRate() time.Duration        { return 10 * time.Millisecond }
func (e *tickEngine) RoomLifetime() (time.Duration, time.Duration) {
	return 50 * time.Millisecond, 0
}

func (e *tickEngine) Welcome(json.RawMessage) json.RawMessage {
	return json.RawMessage(`{"hello":"config"}`)
}

func (e *tickEngine) OnPlayerLeave(_ int, state json.RawMessage) EngineResult {
	var s struct{ Tag string }
	json.Unmarshal(state, &s)
	e.mu.Lock()
	e.leaves[s.Tag]++
	e.mu.Unlock()
	return EngineResult{}
}

func (e *tickEngine) leaveCount(tag string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leaves[tag]
}

func (e *tickEngine) HandleAction(ctx ActionContext) EngineResult {
	var a struct{ Action, Tag string }
	json.Unmarshal(ctx.RawPayload, &a)
	switch a.Action {
	case "tag":
		raw, _ := json.Marshal(map[string]string{"Tag": a.Tag})
		return EngineResult{NewState: raw}
	case "start":
		return EngineResult{Ticking: TickStart}
	case "stop":
		return EngineResult{Ticking: TickStop}
	}
	return EngineResult{}
}

func (e *tickEngine) OnTick(state json.RawMessage, _ time.Time) EngineResult {
	var s struct{ N int }
	json.Unmarshal(state, &s)
	s.N++
	e.ticks.Add(1)
	raw, _ := json.Marshal(s)
	return EngineResult{
		NewState: raw,
		Persist:  s.N%5 == 0,
		Messages: []Message{{Target: TargetAll, Payload: raw}},
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/ws/{id}", wsHandler)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func uniqueRoom(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// dialTick ouvre une connexion et renvoie le welcome et le flux des
// enveloppes suivantes, lues en tâche de fond.
func dialTick(t *testing.T, srv *httptest.Server, room, token string) (*websocket.Conn, Envelope, <-chan Envelope) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + room + "?token=" + token + "&gamemode=test-tick"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	var welcome Envelope
	if err := conn.ReadJSON(&welcome); err != nil || welcome.Type != "welcome" {
		t.Fatalf("welcome attendu : %+v %v", welcome, err)
	}
	out := make(chan Envelope, 256)
	go func() {
		defer close(out)
		for {
			var env Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			select {
			case out <- env:
			default:
			}
		}
	}()
	return conn, welcome, out
}

func roomByID(id string) *Room {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return hub.rooms[id]
}

func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("délai dépassé : %s", what)
}

func TestTickLoopStartsStopsAndPausesWhenEmpty(t *testing.T) {
	srv := newTestServer(t)
	id := uniqueRoom("tick")
	conn, welcome, _ := dialTick(t, srv, id, "a")

	if string(welcome.Payload) != `{"hello":"config"}` {
		t.Fatalf("charge du welcome : %s", welcome.Payload)
	}
	room := roomByID(id)
	if room.Ticking() {
		t.Fatal("la boucle ne doit pas tourner avant la demande de l'engine")
	}

	conn.WriteJSON(map[string]string{"action": "start"})
	waitFor(t, time.Second, "démarrage de la boucle", room.Ticking)
	before := testTick.ticks.Load()
	waitFor(t, time.Second, "ticks", func() bool { return testTick.ticks.Load() >= before+5 })

	conn.WriteJSON(map[string]string{"action": "stop"})
	waitFor(t, time.Second, "arrêt de la boucle", func() bool { return !room.Ticking() })

	// Room vide : la boucle demandée reste en pause, puis reprend au retour d'un client.
	conn.WriteJSON(map[string]string{"action": "start"})
	waitFor(t, time.Second, "redémarrage", room.Ticking)
	conn.Close()
	waitFor(t, time.Second, "pause quand la room est vide", func() bool { return !room.Ticking() })

	dialTick(t, srv, id, "a")
	waitFor(t, time.Second, "reprise au retour d'un client", room.Ticking)
}

func TestTickPersistOnlyWhenAsked(t *testing.T) {
	room := &Room{config: RoomConfig{GameMode: "test-tick"}, clients: map[string]*Client{}}
	room.mu.Lock()
	defer room.mu.Unlock()

	if save := room.applyLocked(EngineResult{NewState: json.RawMessage(`{"N":1}`)}); save != nil {
		t.Fatal("un état de tick sans Persist ne doit pas partir en base")
	}
	if !room.dirty {
		t.Fatal("l'état non écrit doit être marqué à écrire")
	}
	// Persist sans nouvel état : l'état en mémoire part en base.
	if save := room.applyLocked(EngineResult{Persist: true}); string(save) != `{"N":1}` {
		t.Fatalf("état à écrire : %s", save)
	}
	if room.dirty {
		t.Fatal("l'état vient d'être écrit")
	}

	// Engine sans boucle : chaque nouvel état est écrit, comme avant.
	timed := &Room{config: RoomConfig{GameMode: "test-timer"}, clients: map[string]*Client{}}
	timed.mu.Lock()
	defer timed.mu.Unlock()
	if save := timed.applyLocked(EngineResult{NewState: json.RawMessage(`{}`)}); save == nil {
		t.Fatal("un engine sans boucle écrit chaque état")
	}
}

func TestPing(t *testing.T) {
	old := PingInterval
	PingInterval = 20 * time.Millisecond
	defer func() { PingInterval = old }()

	srv := newTestServer(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + uniqueRoom("ping") + "?token=p&gamemode=test-tick"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pinged := make(chan struct{}, 1)
	conn.SetPingHandler(func(string) error {
		select {
		case pinged <- struct{}{}:
		default:
		}
		return nil
	})
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	select {
	case <-pinged:
	case <-time.After(time.Second):
		t.Fatal("aucun ping reçu")
	}
}

func TestSameTokenReplacesOldConnection(t *testing.T) {
	srv := newTestServer(t)
	id := uniqueRoom("token")
	c1, w1, first := dialTick(t, srv, id, "same")
	c1.WriteJSON(map[string]string{"action": "tag", "Tag": id})
	room := roomByID(id)
	waitFor(t, time.Second, "étiquette", func() bool {
		room.mu.RLock()
		defer room.mu.RUnlock()
		return strings.Contains(string(room.state), id)
	})
	_, w2, _ := dialTick(t, srv, id, "same")

	if w1.PlayerNumber != w2.PlayerNumber {
		t.Fatalf("même jeton, même joueur : %d puis %d", w1.PlayerNumber, w2.PlayerNumber)
	}
	// L'ancienne connexion est fermée par le serveur.
	waitFor(t, time.Second, "fermeture de l'ancienne connexion", func() bool {
		select {
		case _, ok := <-first:
			return !ok
		default:
			return false
		}
	})
	waitFor(t, time.Second, "un seul client", func() bool {
		room.mu.RLock()
		defer room.mu.RUnlock()
		return len(room.clients) == 1
	})
	time.Sleep(20 * time.Millisecond)
	if n := testTick.leaveCount(id); n != 0 {
		t.Fatalf("une connexion remplacée ne doit pas déclencher OnPlayerLeave (%d)", n)
	}
}

func TestSweepRemovesIdleRooms(t *testing.T) {
	srv := newTestServer(t)
	id := uniqueRoom("sweep")
	conn, _, _ := dialTick(t, srv, id, "s")

	Sweep(time.Now().Add(time.Hour))
	if roomByID(id) == nil {
		t.Fatal("une room occupée ne doit pas être retirée")
	}

	conn.Close()
	room := roomByID(id)
	waitFor(t, time.Second, "room vide", func() bool {
		room.mu.RLock()
		defer room.mu.RUnlock()
		return len(room.clients) == 0
	})
	Sweep(time.Now())
	if roomByID(id) == nil {
		t.Fatal("une room vide depuis moins que son délai doit rester")
	}
	Sweep(time.Now().Add(time.Second))
	if roomByID(id) != nil {
		t.Fatal("une room vide depuis plus que son délai doit quitter la mémoire")
	}
}

func TestCheckOrigin(t *testing.T) {
	req := func(origin, host string) *http.Request {
		r := httptest.NewRequest("GET", "http://"+host+"/ws/x", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}

	t.Setenv("WS_ALLOWED_ORIGINS", "")
	if !checkOrigin(req("http://n-importe-ou.com", "api.labuch.fr")) {
		t.Fatal("sans liste, toutes les origines passent")
	}

	t.Setenv("WS_ALLOWED_ORIGINS", "https://labuch.fr, *.labuch.fr")
	cases := []struct {
		origin, host string
		ok           bool
	}{
		{"https://labuch.fr", "api.example", true},
		{"https://jeux.labuch.fr", "api.example", true},
		{"https://evil.com", "api.example", false},
		{"https://labuch.fr.evil.com", "api.example", false},
		{"http://localhost:8080", "localhost:8080", true}, // même hôte
		{"", "api.example", true},                         // client hors navigateur
	}
	for _, c := range cases {
		if got := checkOrigin(req(c.origin, c.host)); got != c.ok {
			t.Errorf("origine %q hôte %q : %v, attendu %v", c.origin, c.host, got, c.ok)
		}
	}
}
