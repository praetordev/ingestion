package main

import (
	"crypto/subtle"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/praetordev/db"
	"github.com/praetordev/env"
	"github.com/praetordev/eventbus"
	"github.com/praetordev/ingestion/core"
	"github.com/praetordev/ingestion/handler"
	"github.com/praetordev/metrics"
	"github.com/praetordev/objectstore"
	"github.com/praetordev/plog"
	"github.com/praetordev/runtoken"
)

// internalAuth guards the in-cluster endpoints (credential resolution, runnable
// pre-flight, inventory-sync upsert, log-read proxy) with the shared bearer
// token, compared in constant time. An unset token disables the route entirely
// (fail closed) rather than allowing unauthenticated access. Callers are other
// Praetor services (executor, API), which hold the full secret.
func internalAuth(token string) func(http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(r.Header.Get("Authorization"))
			if token == "" || subtle.ConstantTimeCompare(got, want) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// runTokenAuth guards the host-runner-facing, run-scoped write endpoints
// (events/logs/heartbeat/facts). It accepts EITHER:
//
//   - the full internal secret — for in-cluster callers such as the executor,
//     which publishes lifecycle events for any run; or
//   - the per-run token minted for the {run_id} in the request path — what the
//     host-runner on a managed target presents (see pkg/runtoken).
//
// Both comparisons are constant-time. Because the per-run token is an HMAC of the
// run id, a token for one run does not validate on another: the run id is taken
// from the routed URL, so an attacker cannot present run A's token against run B.
// An unset secret fails every request closed.
func runTokenAuth(secret string) func(http.Handler) http.Handler {
	internalWant := []byte("Bearer " + secret)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if secret == "" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			got := []byte(r.Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, internalWant) == 1 {
				next.ServeHTTP(w, r)
				return
			}
			if runID := chi.URLParam(r, "run_id"); runID != "" {
				runWant := []byte("Bearer " + runtoken.Mint(secret, runID))
				if subtle.ConstantTimeCompare(got, runWant) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		})
	}
}

func main() {
	plog.Configure("ingestion")
	port := env.String("INGESTION_PORT", "8081") // Distinct port from API (8080)

	log.Println("Starting Ingestion Service...")

	// 1. DB
	database, err := db.Connect(env.String("DATABASE_URL", db.DefaultDSN))
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// 2. NATS Infrastructure
	bus, err := eventbus.NewBus(env.String("NATS_URL", eventbus.DefaultURL))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer bus.Close()

	// 3. Object store for bulk log output (JetStream Object Store)
	logStore, err := objectstore.NewJetStreamLogStore(bus.JS, "")
	if err != nil {
		log.Fatalf("Failed to init log object store: %v", err)
	}

	// 4. Service & Handler
	svc := core.NewIngestionService(database, bus, logStore)
	h := handler.NewIngestionHandler(svc)

	// 4. Router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(handler.Metrics)

	r.Handle("/metrics", metrics.Handler())

	// Liveness probe for the container healthcheck (compose depends_on:
	// service_healthy). Intentionally cheap — it does not touch Postgres or NATS,
	// so it reports process liveness, not downstream readiness.
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Auth. Every run-scoped and cross-service endpoint requires the shared secret;
	// an unset secret fails all of them closed.
	//   - internal:   in-cluster callers (executor, API) presenting the full token.
	//   - run-scoped: host-runner calls, accepting the full token OR the run's
	//                 per-run token bound to the {run_id} in the path.
	internalToken := env.String("PRAETOR_INTERNAL_TOKEN", "")
	internal := internalAuth(internalToken)
	runScoped := runTokenAuth(internalToken)

	// In-cluster only (executor / API): full internal token.
	r.With(internal).Get("/internal/v1/runs/{run_id}/credentials", h.ResolveCredentials)
	r.With(internal).Get("/api/v1/runs/{run_id}/runnable", h.Runnable)                 // executor pre-flight
	r.With(internal).Get("/api/v1/runs/{run_id}/logs", h.StreamLog)                    // API log-read proxy
	r.With(internal).Post("/api/v1/inventories/{id}/sync-data", h.IngestInventorySync) // executor upsert
	r.With(internal).Post("/api/v1/inventories/{id}/sync-failure", h.IngestInventorySyncFailure)
	r.With(internal).Get("/internal/v1/inventories/{id}/rendered", h.InventoryRendered) // executor fetches INI (#13)
	r.With(internal).Get("/internal/v1/inventories/{id}/facts", h.InventoryFacts)       // executor fetches fact cache (#48)

	// Host-runner-facing writes: full internal token OR the run's per-run token.
	r.With(runScoped).Post("/api/v1/runs/{run_id}/events", h.Ingest)
	r.With(runScoped).Post("/api/v1/runs/{run_id}/logs", h.IngestLog)
	r.With(runScoped).Get("/api/v1/runs/{run_id}/logs/cursor", h.LogCursor) // stdout resume point (#9)
	r.With(runScoped).Post("/api/v1/runs/{run_id}/heartbeat", h.Heartbeat)
	r.With(runScoped).Post("/api/v1/runs/{run_id}/facts", h.IngestFacts)

	// 5. Start
	log.Printf("Ingestion listening on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
