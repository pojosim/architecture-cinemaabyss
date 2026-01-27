package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type Config struct {
	MonolithURL            string `json:"monolith_url"`
	MoviesServiceURL       string `json:"movies_service_url"`
	EventsServiceURL       string `json:"events_service_url"`
	HttpPort               string `json:"port"`
	GradualMigration       bool   `json:"gradual_migration"`
	MoviesMigrationPercent int    `json:"movies_migration_percent"`
}

var (
	config     Config
	configLock sync.RWMutex
	randSource = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("[CINEMAABYSS-PROXY-SERVICE] ")

	loadConfig()

	log.Printf("Starting Cinemaabyss proxy service")

	router := mux.NewRouter()

	router.HandleFunc("/admin/config", configHandler).Methods("GET")
	router.HandleFunc("/admin/config", updateConfigHandler).Methods("PUT")
	router.HandleFunc("/health", healthHandler).Methods("GET")

	router.PathPrefix("/").HandlerFunc(routerHandler)

	server := &http.Server{
		Addr:         ":" + config.HttpPort,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Service listening on port %s", config.HttpPort)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Service failed to start: %v", err)
	}
}

func loadConfig() {
	config = Config{
		MonolithURL:            "http://localhost:8080",
		MoviesServiceURL:       "http://localhost:8081",
		EventsServiceURL:       "http://localhost:8082",
		HttpPort:               "8000",
		GradualMigration:       false,
		MoviesMigrationPercent: 0,
	}
	loadFromEnv()
}

func configHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"monolith_url":             config.MonolithURL,
		"movies_service_url":       config.MoviesServiceURL,
		"events_service_url":       config.EventsServiceURL,
		"port":                     config.HttpPort,
		"gradual_migration":        config.GradualMigration,
		"movies_migration_percent": config.MoviesMigrationPercent,
	})
}

func updateConfigHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		GradualMigration       *bool `json:"gradual_migration"`
		MoviesMigrationPercent *int  `json:"movies_migration_percent"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	configLock.Lock()
	if req.GradualMigration != nil {
		config.GradualMigration = *req.GradualMigration
	}
	if req.MoviesMigrationPercent != nil {
		if *req.MoviesMigrationPercent < 0 {
			*req.MoviesMigrationPercent = 0
		} else if *req.MoviesMigrationPercent > 100 {
			*req.MoviesMigrationPercent = 100
		}
		config.MoviesMigrationPercent = *req.MoviesMigrationPercent
	}
	configLock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
	})
}

func routerHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method

	if isMoviesPath(path) {
		useMoviesService := shouldRouteToMoviesService()
		logRoutingDecision(path, useMoviesService)

		if useMoviesService {
			w.Header().Set("X-Routed-To", "movies-service")
			moviesServiceProxy().ServeHTTP(w, r)
		} else {
			w.Header().Set("X-Routed-To", "monolith")
			monolithProxy().ServeHTTP(w, r)
		}
		return
	}

	log.Printf("[%s %s] Routing to Monolith", method, path)
	w.Header().Set("X-Routed-To", "monolith")
	monolithProxy().ServeHTTP(w, r)
}

func monolithProxy() *httputil.ReverseProxy {
	monolithURL, _ := url.Parse(config.MonolithURL)
	proxy := httputil.NewSingleHostReverseProxy(monolithURL)
	return proxy
}

func moviesServiceProxy() *httputil.ReverseProxy {
	moviesURL, _ := url.Parse(config.MoviesServiceURL)
	proxy := httputil.NewSingleHostReverseProxy(moviesURL)
	return proxy
}

func isMoviesPath(path string) bool {
	if strings.HasPrefix(path, "/api/movies") {
		return true
	}
	return false
}

func shouldRouteToMoviesService() bool {
	configLock.RLock()
	defer configLock.RUnlock()

	if !config.GradualMigration {
		return false
	}

	if config.MoviesMigrationPercent == 0 {
		return false
	} else if config.MoviesMigrationPercent == 100 {
		return true
	}

	return randSource.Intn(100) < config.MoviesMigrationPercent
}

func logRoutingDecision(path string, useMoviesService bool) {
	if useMoviesService {
		log.Printf("Routing %s to Movies Service", path)
	} else {
		log.Printf("Routing %s to Monolith", path)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "up",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func loadFromEnv() {
	if port := os.Getenv("PORT"); port != "" {
		config.HttpPort = port
	}

	if monolith := os.Getenv("MONOLITH_URL"); monolith != "" {
		config.MonolithURL = monolith
	}

	if movies := os.Getenv("MOVIES_SERVICE_URL"); movies != "" {
		config.MoviesServiceURL = movies
	}

	if events := os.Getenv("EVENTS_SERVICE_URL"); events != "" {
		config.EventsServiceURL = events
	}

	if gradual := os.Getenv("GRADUAL_MIGRATION"); gradual != "" {
		if gradual == "true" {
			config.GradualMigration = true
		} else {
			config.GradualMigration = false
		}
	}

	if percentStr := os.Getenv("MOVIES_MIGRATION_PERCENT"); percentStr != "" {
		if percent, err := strconv.Atoi(percentStr); err == nil {
			if percent < 0 {
				percent = 0
			} else if percent > 100 {
				percent = 100
			}
			config.MoviesMigrationPercent = percent
		}
	}
}
