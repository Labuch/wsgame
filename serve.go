package wsgame

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Serve lance un serveur de jeu autonome, un processus par jeu :
//   - GET /health répond {"status":"ok","game":…,"rooms":…} (vérifié par
//     deploy/update.sh après chaque mise en ligne) ;
//   - les routes ws-game (/ws/{id}, /room/{id}) et, si mount n'est pas nil,
//     celles propres au jeu ;
//   - écoute sur BIND_ADDR:PORT. PORT vaut 8080 par défaut ; BIND_ADDR vide
//     écoute sur toutes les interfaces (développement, tablette du réseau
//     local), 127.0.0.1 en production derrière nginx.
//
// À l'arrêt (SIGTERM de systemd, Ctrl-C), Serve écrit en base l'état des
// rooms qui ne l'est pas encore : une mise en ligne ne fait perdre aux
// parties en cours que la coupure de connexion, que les fronts rattrapent.
// Serve ne rend la main qu'en cas d'erreur, ou après l'arrêt (nil).
func Serve(name string, mount func(chi.Router)) error {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/health", healthHandler(name))
	if mount != nil {
		mount(r)
	}
	Mount(r)

	addr := os.Getenv("BIND_ADDR") + ":" + envOr("PORT", "8080")
	srv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("[%s] à l'écoute sur %s", name, addr)

	select {
	case err := <-errc:
		return fmt.Errorf("[%s] %w", name, err)
	case <-ctx.Done():
	}

	log.Printf("[%s] arrêt : sauvegarde des rooms", name)
	// Les websockets, détournées de net/http, ne retardent pas Shutdown :
	// les clients voient la coupure et se reconnectent au redémarrage.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	n := hub.flush()
	log.Printf("[%s] %d room(s) écrite(s) en base, arrêt", name, n)
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("[%s] %w", name, err)
	}
	return nil
}

// healthHandler répond que le serveur du jeu name tourne, avec le nombre de
// rooms en mémoire.
func healthHandler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
			"game":   name,
			"rooms":  RoomCount(),
		})
	}
}

// flush écrit en base l'état des rooms modifiées depuis leur dernière
// sauvegarde et renvoie leur nombre. Sans base, il ne fait rien.
func (h *Hub) flush() int {
	saves := h.unsaved()
	if store == nil {
		return 0
	}
	for id, state := range saves {
		store.SaveState(id, state)
	}
	return len(saves)
}

// unsaved renvoie l'état des rooms pas encore écrit en base et les marque
// comme écrites.
func (h *Hub) unsaved() map[string]json.RawMessage {
	saves := make(map[string]json.RawMessage)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for id, room := range h.rooms {
		room.mu.Lock()
		if room.dirty && room.state != nil {
			saves[id] = room.state
			room.dirty = false
		}
		room.mu.Unlock()
	}
	return saves
}
