# wsgame

Module Go partagé par les jeux en temps réel : rooms identifiées par un code, joueurs reconnus par un jeton sans compte, reconnexion, boucle de simulation à cadence fixe, sauvegarde des parties dans PostgreSQL. Chaque jeu fournit son engine (ses règles) et lance son propre serveur avec `wsgame.Serve` : **un processus par jeu**, que l'on met en ligne sans toucher aux autres.

Ce dépôt ne se déploie pas : il est compilé dans le serveur de chaque jeu, à la version écrite dans le `go.mod` du jeu.

| Jeu | Engine |
| --- | --- |
| `co-leader` | oui, avec boucle de simulation (`TickEngine`) |
| `buzzer`, `cible` | oui |
| `desamorcage`, `tic-tac-tron` | non : ws-game relaie les messages et garde l'état que le front lui confie |

## Contenu

```text
engine.go       contrat GameEngine et interfaces facultatives (TickEngine, WelcomeEngine, LifetimeEngine, TimedEngine)
handler.go      rooms, jetons, websocket, minuteurs, boucle de ticks, nettoyage des rooms vides
registry.go     enregistrement des engines par mode de jeu
store.go        sauvegarde PostgreSQL (schéma PGSCHEMA)
serve.go        Serve : le serveur complet d'un jeu, /health, arrêt propre
migrations/     création du schéma ws-game et de ses tables
```

## Écrire le serveur d'un jeu

```go
package main

import (
	"log"

	"github.com/Labuch/wsgame"

	"monjeu/internal/monjeu"
)

func main() {
	wsgame.RegisterEngine(monjeu.GameMode, monjeu.NewEngine())
	if err := wsgame.Serve("monjeu", nil); err != nil {
		log.Fatal(err)
	}
}
```

Le second argument de `Serve` ajoute des routes propres au jeu (Co-leader y monte le rechargement de sa configuration en développement).

| Route | Rôle |
| --- | --- |
| `GET /ws/{id}?token=…&gamemode=…` | websocket de la room `id` |
| `GET /room/{id}` | état public de la room (joueurs, configuration) |
| `GET /health` | `{"status":"ok","game":…,"rooms":…}`, vérifié après chaque mise en ligne |

Derrière nginx, chaque jeu expose ces routes sous `/api/` de son sous-domaine (`/api/ws/{id}`…) : voir le README d'un jeu.

| Variable | Rôle | Par défaut |
| --- | --- | --- |
| `PORT` | port d'écoute | `8080` |
| `BIND_ADDR` | adresse d'écoute ; `127.0.0.1` en production | toutes |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE` (ou `DATABASE_URL`) | base PostgreSQL ; injoignable, le serveur garde les rooms en mémoire seule | `localhost:5432/portfolio` |
| `PGSCHEMA` | schéma des tables | `ws-game` |
| `WS_ALLOWED_ORIGINS` | origines acceptées, séparées par des virgules (`*.labuch.fr` accepte les sous-domaines) ; une page du même hôte l'est toujours | toutes |

À l'arrêt (SIGTERM de systemd, Ctrl-C), `Serve` écrit en base l'état des rooms qui ne l'est pas encore : une mise en ligne ne coûte aux joueurs qu'une reconnexion.

**La base.** Tous les jeux partagent le schéma `ws-game` (`migrations/001_ws-game.sql`, celui qu'utilisait make) ; chacun ne nettoie que les rooms de son jeu. Pour isoler un jeu dans son propre schéma, il suffit de créer ce schéma avec la même migration, le nom changé, et de régler `PGSCHEMA` dans ses réglages.

## Faire évoluer wsgame

Le cas courant ne passe pas par ici : changer les règles d'un jeu ne touche que le dépôt du jeu. On ne vient dans `wsgame` que pour une capacité générique (une nouvelle interface d'engine, un réglage du transport).

**Règle de compatibilité.** Ajouter une capacité par une **interface facultative**, comme `TickEngine` ou `WelcomeEngine` : un jeu qui ne l'implémente pas ne voit aucune différence, et c'est une version mineure (`v0.4.0` → `v0.5.0`). Changer le contrat de base (l'interface `GameEngine`, le format des messages `welcome`, `event`… que les fronts lisent aussi) casse les jeux : c'est une version majeure, à éviter.

**Travailler sur wsgame et un jeu à la fois**, sans rien publier : un `go.work` dans le dossier parent des dépôts, jamais commité.

```bash
cd ~/Developement/labuch
```

```bash
go work init ./wsgame ./co-leader/server
```

Le jeu compile alors avec ta copie locale de `wsgame`. Avant de pousser le jeu, vérifier qu'il compile avec la version publiée, comme sur le VPS :

```bash
GOWORK=off go build ./...
```

**Publier une version**, une fois les tests de `wsgame` et des jeux concernés au vert :

```bash
go test -race ./...
```

```bash
git tag v0.5.0 && git push origin v0.5.0
```

Puis, dans chaque jeu qui en a besoin, et seulement ceux-là :

```bash
cd server && go get github.com/Labuch/wsgame@v0.5.0 && go mod tidy
```

Les autres jeux restent sur leur version tant qu'on ne fait pas la même chose chez eux.

## Dépôt public ou privé

- **Public (le plus simple)** : le code ne contient aucun secret ; `go get` et le VPS le récupèrent sans réglage.
- **Privé** : sur ton poste, une fois, `go env -w GOPRIVATE=github.com/Labuch/*` avec ton accès Git habituel. Le VPS, lui, ne peut pas le télécharger ; deux solutions :
  - `go mod vendor` dans le `server/` de chaque jeu, à commiter et à refaire après chaque `go get` : le VPS compile sans réseau ;
  - une clé de déploiement GitHub en lecture seule pour `wsgame` sur le VPS, avec `GOPRIVATE` réglé pour l'utilisateur qui lance `update.sh`.

## Tests

```bash
go test -race ./...
```
