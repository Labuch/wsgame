CREATE SCHEMA IF NOT EXISTS "ws-game";

CREATE TABLE IF NOT EXISTS "ws-game".rooms (
    id          VARCHAR(16)  PRIMARY KEY,
    game_mode   VARCHAR(16)  NOT NULL DEFAULT '',
    max_players SMALLINT     NOT NULL DEFAULT 2,
    role_slot   SMALLINT     NOT NULL DEFAULT 0,
    state       JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "ws-game".player_slots (
    room_id       VARCHAR(16)  NOT NULL REFERENCES "ws-game".rooms(id) ON DELETE CASCADE,
    player_number SMALLINT     NOT NULL CHECK (player_number >= 1 AND player_number <= 8),
    token         VARCHAR(128) NOT NULL,
    PRIMARY KEY (room_id, player_number)
);
