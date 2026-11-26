package wsgame

// engines associe un gameMode à son GameEngine.
// Toutes les registrations doivent se faire avant wsgame.Mount().
var engines = map[string]GameEngine{}

// RegisterEngine associe un GameEngine à un gameMode.
// Doit être appelé avant wsgame.Mount().
func RegisterEngine(gameMode string, engine GameEngine) {
	engines[gameMode] = engine
}

func engineFor(gameMode string) GameEngine {
	return engines[gameMode] // nil si non enregistré
}
