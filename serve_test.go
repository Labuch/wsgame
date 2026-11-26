package wsgame

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	healthHandler("essai")(rec, httptest.NewRequest("GET", "/health", nil))
	var body struct {
		Status string `json:"status"`
		Game   string `json:"game"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("réponse illisible %q : %v", rec.Body.String(), err)
	}
	if rec.Code != 200 || body.Status != "ok" || body.Game != "essai" {
		t.Fatalf("health = %d %+v", rec.Code, body)
	}
}

// À l'arrêt, seules les rooms dont l'état n'est pas encore en base sont
// écrites, une seule fois.
func TestUnsavedAtShutdown(t *testing.T) {
	h := &Hub{rooms: map[string]*Room{
		"AAAAAA": {id: "AAAAAA", dirty: true, state: json.RawMessage(`{"tick":42}`)},
		"BBBBBB": {id: "BBBBBB", dirty: false, state: json.RawMessage(`{"tick":7}`)},
		"CCCCCC": {id: "CCCCCC", dirty: true},
	}}
	got := h.unsaved()
	if len(got) != 1 || string(got["AAAAAA"]) != `{"tick":42}` {
		t.Fatalf("unsaved = %v, attendu la seule room AAAAAA", got)
	}
	if h.rooms["AAAAAA"].dirty {
		t.Fatal("AAAAAA devrait être marquée écrite")
	}
	if again := h.unsaved(); len(again) != 0 {
		t.Fatalf("second passage = %v, attendu vide", again)
	}
}
