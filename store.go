package wsgame

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"time"

	"github.com/lib/pq"
)

// Store wraps the PostgreSQL connection.
// The server falls back to in-memory-only mode when Store is nil.
type Store struct {
	db *sql.DB
}

// NewStore opens the DB connection and runs the migration.
// Returns (nil, err) when the DB is unreachable so the caller can degrade gracefully.
func NewStore() (*Store, error) {
	dsn := buildDSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{db: db}, nil
}

// schema is the PostgreSQL schema used for all tables.
var schema = envOr("PGSCHEMA", "ws-game")

func tbl(name string) string {
	return fmt.Sprintf(`"%s".%s`, schema, name)
}

func buildDSN() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	host := envOr("PGHOST", "localhost")
	port := envOr("PGPORT", "5432")
	user := envOr("PGUSER", "portFolio")
	password := os.Getenv("PGPASSWORD")
	dbname := envOr("PGDATABASE", "portfolio")
	sslmode := envOr("PGSSLMODE", "disable")
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode,
	)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Read ─────────────────────────────────────────────────────────────────────

type roomRow struct {
	GameMode   string
	MaxPlayers int
	RoleSlot   int
	State      json.RawMessage
}

type slotRow struct {
	PlayerNumber int
	Token        string
}

func (s *Store) LoadRoom(id string) (*roomRow, error) {
	var gm string
	var mp int
	var ps int
	var raw []byte
	err := s.db.QueryRow(
		fmt.Sprintf(`SELECT game_mode, max_players, role_slot, state FROM %s WHERE id = $1`, tbl("rooms")), id,
	).Scan(&gm, &mp, &ps, &raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &roomRow{GameMode: gm, MaxPlayers: mp, RoleSlot: ps, State: json.RawMessage(raw)}, nil
}

func (s *Store) LoadSlots(roomID string) ([]slotRow, error) {
	rows, err := s.db.Query(
		fmt.Sprintf(`SELECT player_number, token FROM %s WHERE room_id = $1 ORDER BY player_number`, tbl("player_slots")), roomID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var slots []slotRow
	for rows.Next() {
		var r slotRow
		if err := rows.Scan(&r.PlayerNumber, &r.Token); err != nil {
			return nil, err
		}
		slots = append(slots, r)
	}
	return slots, rows.Err()
}

// ─── Write ────────────────────────────────────────────────────────────────────

func (s *Store) UpsertRoom(id, gameMode string, maxPlayers, roleSlot int) {
	_, err := s.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (id, game_mode, max_players, role_slot)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE
		  SET game_mode   = EXCLUDED.game_mode,
		      max_players = EXCLUDED.max_players,
		      role_slot   = EXCLUDED.role_slot,
		      updated_at  = NOW()
	`, tbl("rooms")), id, gameMode, maxPlayers, roleSlot)
	if err != nil {
		log.Printf("[ws-game] upsert room %s: %v", id, err)
	}
}

func (s *Store) SaveState(roomID string, state json.RawMessage) {
	_, err := s.db.Exec(fmt.Sprintf(`
		UPDATE %s SET state = $2, updated_at = NOW() WHERE id = $1
	`, tbl("rooms")), roomID, []byte(state))
	if err != nil {
		log.Printf("[ws-game] save state room %s: %v", roomID, err)
	}
}

func (s *Store) UpsertSlot(roomID string, playerNumber int, token string) {
	_, err := s.db.Exec(fmt.Sprintf(`
		INSERT INTO %s (room_id, player_number, token)
		VALUES ($1, $2, $3)
		ON CONFLICT (room_id, player_number) DO NOTHING
	`, tbl("player_slots")), roomID, playerNumber, token)
	if err != nil {
		log.Printf("[ws-game] upsert slot room %s player %d: %v", roomID, playerNumber, err)
	}
}

// PurgeRooms supprime de la base les rooms du jeu gameMode dont l'état n'a
// pas changé depuis olderThan, sauf celles encore en mémoire (keep).
func (s *Store) PurgeRooms(gameMode string, olderThan time.Duration, keep []string) {
	res, err := s.db.Exec(fmt.Sprintf(`
		DELETE FROM %s
		WHERE game_mode = $1
		  AND updated_at < NOW() - make_interval(secs => $2)
		  AND NOT (id = ANY($3))
	`, tbl("rooms")), gameMode, olderThan.Seconds(), pq.Array(keep))
	if err != nil {
		log.Printf("[ws-game] purge rooms %s: %v", gameMode, err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("[ws-game] purge rooms %s: %d supprimée(s)", gameMode, n)
	}
}
