package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/AstraBert/quache/quache-go/core"
)

type SetRequest struct {
	Key   string   `json:"key"`
	Value any      `json:"value"`
	Ttl   *float64 `json:"ttl"`
}

type GetResponse struct {
	Value any `json:"value"`
}

func handlePost(kvStore *core.KVStore, w http.ResponseWriter, r *http.Request) {
	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&req)
	if err != nil {
		http.Error(
			w,
			fmt.Sprintf("An error occurred while reading your request: %s", err.Error()),
			http.StatusBadRequest,
		)
		return
	}
	kvStore.Put(req.Key, req.Value, req.Ttl)
	w.WriteHeader(http.StatusCreated)
}

func handleGet(kvStore *core.KVStore, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if strings.TrimSpace(key) == "" {
		http.Error(
			w,
			"Provided key was empty, please provide a non-empty key",
			http.StatusBadRequest,
		)
		return
	}
	val, err := kvStore.Get(key)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusNotFound,
		)
		return
	}
	apiResponse := GetResponse{Value: val}
	w.Header().Set("Content-Type", "application/json")
	j, err := json.Marshal(apiResponse)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(j)
}

func handleDelete(kvStore *core.KVStore, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(
			w,
			"Provided key was empty, please provide a non-empty key",
			http.StatusBadRequest,
		)
		return
	}
	kvStore.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}

func CreateServerMux(kvStore *core.KVStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /kv", func(w http.ResponseWriter, r *http.Request) {
		handlePost(kvStore, w, r)
	})

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		handleGet(kvStore, w, r)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		handleDelete(kvStore, w, r)
	})

	return mux
}
