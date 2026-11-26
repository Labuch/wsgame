package wsgame

import (
	"encoding/json"
	"time"
)

// Targeting constants for Message.Target.
const (
	TargetAll    = 0  // broadcast à tous les clients de la room
	TargetOthers = -1 // broadcast à tous sauf le sender
	// n > 0 → envoyer uniquement au joueur n
)

// Message est un message sortant produit par un engine.
type Message struct {
	Target  int             // TargetAll, TargetOthers, ou un playerNumber
	Payload json.RawMessage // placé dans Envelope.Payload
}

// ActionContext contient tout ce dont l'engine a besoin pour traiter une action.
type ActionContext struct {
	PlayerNumber int
	RawPayload   json.RawMessage // le message JSON brut reçu du client
	CurrentState json.RawMessage // state persisté de la room (peut être nil)
}

// StopTimer, placé dans EngineResult.Timer, annule le minuteur de la room.
const StopTimer time.Duration = -1

// EngineResult est l'ensemble des effets que l'engine veut appliquer.
// NewState remplace le state de la room (et est persisté).
// Si NewState est nil, le state est inchangé.
type EngineResult struct {
	NewState json.RawMessage
	Messages []Message
	// Timer pilote le minuteur unique de la room (voir TimedEngine) :
	// 0 = inchangé, > 0 = (ré)armé pour cette durée, StopTimer = annulé.
	Timer time.Duration
	// Ticking pilote la boucle de simulation de la room (voir TickEngine) :
	// TickKeep = inchangé, TickStart = démarrer, TickStop = arrêter.
	Ticking int
	// Persist demande l'écriture de NewState en base. Il n'est lu que pour
	// les TickEngine : pour les autres engines, chaque nouvel état est écrit,
	// comme avant.
	Persist bool
}

// Valeurs de EngineResult.Ticking.
const (
	TickKeep  = 0
	TickStart = 1
	TickStop  = -1
)

// GameEngine est l'interface que chaque jeu doit implémenter.
// Toutes les méthodes sont appelées avec room.mu.Lock() tenu :
// les engines sont des fonctions pures, sans lock interne ni appel retour dans room.
type GameEngine interface {
	// OnRoomCreate est appelé une seule fois à la création de la room.
	// Peut modifier config (ex: assigner RoleSlot).
	OnRoomCreate(config *RoomConfig)

	// RoleFor retourne le rôle du joueur playerNumber.
	// Retourner "" si le jeu n'a pas de rôles asymétriques.
	RoleFor(config RoomConfig, playerNumber int) string

	// HandleAction traite un message entrant et retourne le nouveau state + messages.
	HandleAction(ctx ActionContext) EngineResult

	// OnPlayerLeave est appelé quand un client se déconnecte.
	// Retourner Messages vides pour ne rien envoyer.
	OnPlayerLeave(playerNumber int, currentState json.RawMessage) EngineResult
}

// TimedEngine est implémentée par les engines qui ont besoin d'échéances
// côté serveur (ex : temps de réponse limité). Le minuteur est armé via
// EngineResult.Timer ; à expiration, OnTimer est appelé avec room.mu tenu.
type TimedEngine interface {
	GameEngine
	OnTimer(currentState json.RawMessage) EngineResult
}

// TickEngine est implémentée par les jeux en temps réel. La room appelle
// OnTick à intervalle fixe (TickRate) tant que l'engine l'a demandé
// (EngineResult.Ticking = TickStart) et qu'au moins un client est connecté.
// Une room vide met sa boucle en pause, et la reprend au retour d'un client.
// OnTick est appelé avec room.mu tenu, comme les autres méthodes.
//
// Pour un TickEngine, l'état n'est écrit en base que si EngineResult.Persist
// est vrai : à 20 ticks par seconde, écrire chaque état serait trop coûteux.
type TickEngine interface {
	GameEngine
	TickRate() time.Duration
	OnTick(currentState json.RawMessage, now time.Time) EngineResult
}

// WelcomeEngine ajoute une charge au message « welcome » envoyé à chaque
// connexion (Envelope.Payload), par exemple la configuration de la room.
type WelcomeEngine interface {
	GameEngine
	Welcome(currentState json.RawMessage) json.RawMessage
}

// LifetimeEngine règle la durée de vie des rooms de ce jeu : une room vide
// depuis idle quitte la mémoire (son état reste en base), et une room dont
// l'état n'a pas changé depuis retention est supprimée de la base
// (retention = 0 : jamais). Sans cette interface : DefaultRoomIdle, et aucune
// suppression en base.
type LifetimeEngine interface {
	GameEngine
	RoomLifetime() (idle, retention time.Duration)
}
