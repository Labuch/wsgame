package wsgame

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// ─── Room config & persistent state ──────────────────────────────────────────

type RoomConfig struct {
	GameMode   string `json:"gameMode"`
	MaxPlayers int    `json:"maxPlayers"`
	RoleSlot   int    `json:"-"`
}

type PlayerSlot struct {
	Token  string
	Number int
}

type payloadPeek struct {
	State json.RawMessage `json:"state,omitempty"`
}

// ─── Envelope ─────────────────────────────────────────────────────────────────

type Envelope struct {
	Type         string          `json:"type"`
	ClientID     string          `json:"clientId,omitempty"`
	PlayerNumber int             `json:"playerNumber,omitempty"`
	Role         string          `json:"role,omitempty"`
	Clients      []string        `json:"clients,omitempty"`
	Config       *RoomConfig     `json:"config,omitempty"`
	State        json.RawMessage `json:"state,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Message      string          `json:"message,omitempty"`
}

// ─── Client ───────────────────────────────────────────────────────────────────

type Client struct {
	id           string
	playerNumber int
	conn         *websocket.Conn
	send         chan Envelope
	room         *Room
	// replaced : une connexion plus récente du même joueur a pris la place
	// de celle-ci. Sa fermeture ne doit pas marquer le joueur hors ligne.
	// Protégé par room.mu.
	replaced bool
}

// Délais de la connexion : un ping toutes les PingInterval, et une connexion
// muette (ni message, ni pong) depuis ReadTimeout est fermée. Sans ping, une
// tablette en wifi peut garder une connexion morte sans le savoir.
var (
	PingInterval = 20 * time.Second
	ReadTimeout  = 45 * time.Second
	writeTimeout = 10 * time.Second
)

func (c *Client) writePump() {
	ping := time.NewTicker(PingInterval)
	defer func() {
		ping.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case env, ok := <-c.send:
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteJSON(env); err != nil {
				return
			}
		case <-ping.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		eng := engineFor(c.room.config.GameMode)
		c.room.mu.Lock()
		replaced := c.replaced
		var save json.RawMessage
		var result EngineResult
		if eng != nil && !replaced {
			result = eng.OnPlayerLeave(c.playerNumber, c.room.state)
			save = c.room.applyLocked(result)
		}
		c.room.mu.Unlock()
		c.room.publish(save, result.Messages, c)
		c.room.remove(c)
		close(c.send)
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(ReadTimeout))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(ReadTimeout))
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		c.conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if !json.Valid(raw) {
			continue
		}

		eng := engineFor(c.room.config.GameMode)

		if eng == nil {
			// Fallback : comportement legacy — peek state + relay raw
			var peek payloadPeek
			if json.Unmarshal(raw, &peek) == nil && len(peek.State) > 0 {
				c.room.mu.Lock()
				c.room.state = peek.State
				c.room.mu.Unlock()
				if store != nil {
					go store.SaveState(c.room.id, peek.State)
				}
			}
			c.room.relay(Envelope{
				Type:         "event",
				ClientID:     c.id,
				PlayerNumber: c.playerNumber,
				Payload:      json.RawMessage(raw),
			}, c)
			continue
		}

		c.room.mu.Lock()
		result := eng.HandleAction(ActionContext{
			PlayerNumber: c.playerNumber,
			RawPayload:   raw,
			CurrentState: c.room.state,
		})
		save := c.room.applyLocked(result)
		c.room.mu.Unlock()
		c.room.publish(save, result.Messages, c)
	}
}

// ─── Room ─────────────────────────────────────────────────────────────────────

type Room struct {
	id      string
	clients map[string]*Client
	mu      sync.RWMutex

	config  RoomConfig
	players []PlayerSlot
	state   json.RawMessage

	timer    *time.Timer
	timerGen uint64 // incrémenté à chaque (ré)armement : invalide les minuteurs périmés

	// Boucle de simulation (TickEngine) : tickWanted est la demande de
	// l'engine, tickStop ferme la goroutine en cours (nil = arrêtée).
	tickWanted bool
	tickStop   chan struct{}

	emptySince time.Time // dernier départ du dernier client (zéro = occupée)
	dirty      bool      // l'état en mémoire n'a pas encore été écrit en base
}

// applyLocked applique le state, le minuteur et la boucle d'un EngineResult,
// et renvoie l'état à écrire en base (nil = rien à écrire).
// room.mu doit être tenu.
func (r *Room) applyLocked(result EngineResult) json.RawMessage {
	eng := engineFor(r.config.GameMode)
	_, ticking := eng.(TickEngine)

	var save json.RawMessage
	if result.NewState != nil {
		r.state = result.NewState
		if !ticking || result.Persist {
			save = result.NewState
			r.dirty = false
		} else {
			r.dirty = true
		}
	} else if ticking && result.Persist && r.dirty {
		save = r.state
		r.dirty = false
	}

	switch result.Ticking {
	case TickStart:
		r.tickWanted = true
		r.syncTickerLocked()
	case TickStop:
		r.tickWanted = false
		r.syncTickerLocked()
	}

	if result.Timer == 0 {
		return save
	}
	r.timerGen++
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	if result.Timer > 0 {
		gen := r.timerGen
		r.timer = time.AfterFunc(result.Timer, func() { r.fireTimer(gen) })
	}
	return save
}

// publish écrit l'état en base (s'il y en a un) et distribue les messages.
// room.mu ne doit pas être tenu.
func (r *Room) publish(save json.RawMessage, msgs []Message, sender *Client) {
	if save != nil && store != nil {
		go store.SaveState(r.id, save)
	}
	r.dispatchMessages(msgs, sender)
}

func (r *Room) fireTimer(gen uint64) {
	r.mu.Lock()
	eng, ok := engineFor(r.config.GameMode).(TimedEngine)
	if !ok || gen != r.timerGen {
		r.mu.Unlock()
		return
	}
	r.timer = nil
	result := eng.OnTimer(r.state)
	save := r.applyLocked(result)
	r.mu.Unlock()
	r.publish(save, result.Messages, nil)
}

// syncTickerLocked démarre ou arrête la boucle de simulation : elle tourne
// quand l'engine l'a demandée et qu'au moins un client est là.
// room.mu doit être tenu.
func (r *Room) syncTickerLocked() {
	eng, ok := engineFor(r.config.GameMode).(TickEngine)
	run := ok && r.tickWanted && len(r.clients) > 0
	switch {
	case run && r.tickStop == nil:
		stop := make(chan struct{})
		r.tickStop = stop
		go r.runTicker(eng, stop)
	case !run && r.tickStop != nil:
		close(r.tickStop)
		r.tickStop = nil
	}
}

func (r *Room) runTicker(eng TickEngine, stop chan struct{}) {
	rate := eng.TickRate()
	if rate <= 0 {
		rate = 50 * time.Millisecond
	}
	t := time.NewTicker(rate)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			r.mu.Lock()
			select {
			case <-stop: // arrêtée pendant l'attente du verrou
				r.mu.Unlock()
				return
			default:
			}
			result := eng.OnTick(r.state, now)
			save := r.applyLocked(result)
			r.mu.Unlock()
			r.publish(save, result.Messages, nil)
		}
	}
}

// Ticking indique si la boucle de simulation tourne (pour les tests).
func (r *Room) Ticking() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tickStop != nil
}

func (r *Room) assignPlayer(token string) int {
	for i, slot := range r.players {
		if slot.Token == token {
			return i + 1
		}
	}
	max := r.config.MaxPlayers
	if max < 2 {
		max = 2
	}
	if len(r.players) < max {
		num := len(r.players) + 1
		r.players = append(r.players, PlayerSlot{Token: token, Number: num})
		return num
	}
	return 0
}

func (r *Room) clientIDs() []string {
	ids := make([]string, 0, len(r.clients))
	for id := range r.clients {
		ids = append(ids, id)
	}
	return ids
}

func (r *Room) add(c *Client) {
	r.mu.Lock()
	r.clients[c.id] = c
	r.emptySince = time.Time{}
	r.syncTickerLocked()
	r.mu.Unlock()
}

func (r *Room) remove(c *Client) {
	r.mu.Lock()
	delete(r.clients, c.id)
	remaining := r.clientIDs()
	if len(r.clients) == 0 {
		r.emptySince = time.Now()
	}
	r.syncTickerLocked()
	r.mu.Unlock()

	r.broadcast(Envelope{
		Type:         "leave",
		ClientID:     c.id,
		PlayerNumber: c.playerNumber,
		Clients:      remaining,
	}, nil)
}

func (r *Room) broadcast(env Envelope, skip *Client) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.clients {
		if c == skip {
			continue
		}
		select {
		case c.send <- env:
		default:
		}
	}
}

func (r *Room) relay(env Envelope, sender *Client) {
	r.broadcast(env, sender)
}

func (r *Room) sendTo(c *Client, env Envelope) {
	select {
	case c.send <- env:
	default:
	}
}

func (r *Room) clientByPlayerNumber(n int) *Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.clients {
		if c.playerNumber == n {
			return c
		}
	}
	return nil
}

func (r *Room) dispatchMessages(msgs []Message, sender *Client) {
	for _, m := range msgs {
		env := Envelope{Type: "event", Payload: m.Payload}
		switch m.Target {
		case TargetAll:
			r.broadcast(env, nil)
		case TargetOthers:
			r.broadcast(env, sender)
		default:
			if c := r.clientByPlayerNumber(m.Target); c != nil {
				r.sendTo(c, env)
			}
		}
	}
}

// ─── Hub ──────────────────────────────────────────────────────────────────────

type Hub struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

func (h *Hub) getOrCreate(id string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	if room, ok := h.rooms[id]; ok {
		// Repousse le nettoyage : un client est en train d'arriver.
		room.mu.Lock()
		if len(room.clients) == 0 {
			room.emptySince = time.Now()
		}
		room.mu.Unlock()
		return room
	}
	room := &Room{
		id:         id,
		clients:    make(map[string]*Client),
		players:    []PlayerSlot{},
		emptySince: time.Now(),
	}

	if store != nil {
		if row, err := store.LoadRoom(id); err == nil && row != nil {
			room.config.GameMode = row.GameMode
			room.config.MaxPlayers = row.MaxPlayers
			room.config.RoleSlot = row.RoleSlot
			room.state = row.State
		}
		if slots, err := store.LoadSlots(id); err == nil {
			for _, s := range slots {
				if s.PlayerNumber >= 1 {
					for len(room.players) < s.PlayerNumber {
						room.players = append(room.players, PlayerSlot{})
					}
					room.players[s.PlayerNumber-1] = PlayerSlot{Token: s.Token, Number: s.PlayerNumber}
				}
			}
		}
	}

	h.rooms[id] = room
	return room
}

// DefaultRoomIdle : une room vide depuis cette durée quitte la mémoire
// (voir LifetimeEngine). Son état reste en base et la room est reprise au
// retour d'un joueur.
const DefaultRoomIdle = 10 * time.Minute

func roomLifetime(gameMode string) (idle, retention time.Duration) {
	if eng, ok := engineFor(gameMode).(LifetimeEngine); ok {
		idle, retention = eng.RoomLifetime()
	}
	if idle <= 0 {
		idle = DefaultRoomIdle
	}
	return idle, retention
}

// sweep retire de la mémoire les rooms vides depuis trop longtemps, en
// écrivant d'abord leur dernier état s'il n'est pas en base.
func (h *Hub) sweep(now time.Time) {
	type pending struct {
		id    string
		state json.RawMessage
	}
	var saves []pending

	h.mu.Lock()
	for id, room := range h.rooms {
		room.mu.Lock()
		idle, _ := roomLifetime(room.config.GameMode)
		if len(room.clients) == 0 && !room.emptySince.IsZero() && now.Sub(room.emptySince) >= idle {
			if room.timer != nil {
				room.timer.Stop()
				room.timer = nil
			}
			room.timerGen++
			if room.tickStop != nil {
				close(room.tickStop)
				room.tickStop = nil
			}
			if room.dirty && room.state != nil {
				saves = append(saves, pending{id, room.state})
				room.dirty = false
			}
			delete(h.rooms, id)
		}
		room.mu.Unlock()
	}
	inMemory := make([]string, 0, len(h.rooms))
	for id := range h.rooms {
		inMemory = append(inMemory, id)
	}
	h.mu.Unlock()

	if store == nil {
		return
	}
	for _, p := range saves {
		store.SaveState(p.id, p.state)
	}
	for mode := range engines {
		if _, retention := roomLifetime(mode); retention > 0 {
			store.PurgeRooms(mode, retention, inMemory)
		}
	}
}

// RoomCount renvoie le nombre de rooms en mémoire (pour les tests).
func RoomCount() int {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	return len(hub.rooms)
}

// Sweep lance un nettoyage immédiat, comme si l'heure était now (pour les tests).
func Sweep(now time.Time) { hub.sweep(now) }

func (h *Hub) janitor(every time.Duration) {
	for now := range time.Tick(every) {
		h.sweep(now)
	}
}

var hub = &Hub{rooms: make(map[string]*Room)}
var store *Store

var upgrader = websocket.Upgrader{CheckOrigin: checkOrigin}

// checkOrigin : sans WS_ALLOWED_ORIGINS, toutes les origines sont acceptées
// (développement, tests depuis une tablette du réseau local). En production,
// WS_ALLOWED_ORIGINS liste les origines permises, séparées par des virgules ;
// « *.labuch.fr » accepte aussi les sous-domaines. Une page servie par le même
// hôte que le serveur est toujours acceptée.
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	raw := strings.TrimSpace(os.Getenv("WS_ALLOWED_ORIGINS"))
	if origin == "" || raw == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if u.Host == r.Host {
		return true
	}
	for _, a := range strings.Split(raw, ",") {
		a = strings.TrimSpace(a)
		switch {
		case a == "*" || a == origin:
			return true
		case strings.HasPrefix(a, "*."):
			if strings.HasSuffix(u.Hostname(), a[1:]) || u.Hostname() == a[2:] {
				return true
			}
		}
	}
	return false
}

func newClientID() string {
	return fmt.Sprintf("%08x", rand.Uint32())
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

// GET /room/{id}
func roomInfoHandler(w http.ResponseWriter, r *http.Request) {
	roomID := chi.URLParam(r, "id")
	if roomID == "" {
		http.Error(w, "room ID required", http.StatusBadRequest)
		return
	}

	hub.mu.RLock()
	room, exists := hub.rooms[roomID]
	hub.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if !exists {
		json.NewEncoder(w).Encode(map[string]any{
			"roomId":   roomID,
			"clients":  []string{},
			"config":   RoomConfig{},
			"hasState": false,
		})
		return
	}

	room.mu.RLock()
	clients := room.clientIDs()
	config := room.config
	hasState := len(room.state) > 0
	room.mu.RUnlock()

	json.NewEncoder(w).Encode(map[string]any{
		"roomId":   roomID,
		"clients":  clients,
		"config":   config,
		"hasState": hasState,
	})
}

// GET /ws/{id}?token=<token>&gamemode=<mode>&maxplayers=<n>
func wsHandler(w http.ResponseWriter, r *http.Request) {
	roomID := chi.URLParam(r, "id")
	if roomID == "" {
		http.Error(w, "room ID required", http.StatusBadRequest)
		return
	}

	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		http.Error(w, "token required", http.StatusBadRequest)
		return
	}
	gameMode := strings.TrimSpace(r.URL.Query().Get("gamemode"))
	maxPlayers, _ := strconv.Atoi(r.URL.Query().Get("maxplayers"))

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[ws-game] upgrade:", err)
		return
	}

	room := hub.getOrCreate(roomID)

	room.mu.Lock()
	isNewRoom := room.config.GameMode == "" && room.config.MaxPlayers == 0
	if isNewRoom {
		if gameMode != "" {
			room.config.GameMode = gameMode
		}
		if maxPlayers >= 2 && maxPlayers <= 8 {
			room.config.MaxPlayers = maxPlayers
		}
		if eng := engineFor(room.config.GameMode); eng != nil {
			eng.OnRoomCreate(&room.config)
		}
	}
	if room.config.MaxPlayers < 2 {
		room.config.MaxPlayers = 2
	}

	isExistingToken := false
	for _, slot := range room.players {
		if slot.Token == token {
			isExistingToken = true
			break
		}
	}

	playerNumber := room.assignPlayer(token)
	isNewSlot := !isExistingToken && playerNumber > 0
	config := room.config
	currentState := room.state

	var role string
	var welcome json.RawMessage
	if eng := engineFor(config.GameMode); eng != nil {
		role = eng.RoleFor(config, playerNumber)
		if we, ok := eng.(WelcomeEngine); ok {
			welcome = we.Welcome(currentState)
		}
	}

	// Une seule connexion par jeton : la nouvelle remplace l'ancienne (onglet
	// en double, tablette sortie de veille avant que le serveur ait vu la
	// coupure). L'ancienne est fermée sans marquer le joueur hors ligne.
	var stale []*Client
	if playerNumber > 0 {
		for _, c := range room.clients {
			if c.playerNumber == playerNumber {
				c.replaced = true
				stale = append(stale, c)
			}
		}
	}
	room.mu.Unlock()

	for _, c := range stale {
		c.conn.Close()
	}

	if playerNumber == 0 {
		conn.WriteJSON(Envelope{Type: "error", Message: "room is full"})
		conn.Close()
		return
	}

	if store != nil {
		if isNewRoom {
			go store.UpsertRoom(roomID, config.GameMode, config.MaxPlayers, config.RoleSlot)
		}
		if isNewSlot {
			go store.UpsertSlot(roomID, playerNumber, token)
		}
	}

	client := &Client{
		id:           newClientID(),
		playerNumber: playerNumber,
		conn:         conn,
		send:         make(chan Envelope, 256),
		room:         room,
	}
	room.add(client)

	room.mu.RLock()
	allIDs := room.clientIDs()
	room.mu.RUnlock()

	room.sendTo(client, Envelope{
		Type:         "welcome",
		ClientID:     client.id,
		PlayerNumber: playerNumber,
		Role:         role,
		Clients:      allIDs,
		Config:       &config,
		State:        currentState,
		Payload:      welcome,
	})

	room.broadcast(Envelope{
		Type:         "join",
		ClientID:     client.id,
		PlayerNumber: playerNumber,
		Clients:      allIDs,
	}, client)

	go client.writePump()
	client.readPump()
}

// ─── Mount ────────────────────────────────────────────────────────────────────

// Mount initialise le store et enregistre les routes ws-game sur r.
func Mount(r chi.Router) {
	var err error
	store, err = NewStore()
	if err != nil {
		log.Printf("[ws-game] ⚠  DB unavailable, running in-memory only: %v", err)
	} else {
		log.Printf("[ws-game] ✓  Connected to PostgreSQL (schema: %s)", schema)
	}

	r.Get("/ws/{id}", wsHandler)
	r.Get("/room/{id}", roomInfoHandler)

	go hub.janitor(time.Minute)
}

// MountMemory enregistre les routes sans base de données ni nettoyage
// périodique (tests d'intégration des engines).
func MountMemory(r chi.Router) {
	r.Get("/ws/{id}", wsHandler)
	r.Get("/room/{id}", roomInfoHandler)
}
