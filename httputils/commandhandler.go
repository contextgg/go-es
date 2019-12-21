package httputils

import (
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/contextgg/go-es/es"
)

// CommandHandler parses commands and sends them
func CommandHandler(bus es.CommandBus) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unsuported method: "+r.Method, http.StatusMethodNotAllowed)
			return
		}

		name := path.Base(r.URL.Path)
		cmd, err := bus.NewCommand(name)
		if err != nil {
			http.Error(w, "could not create command: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, "could not decode command: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx := context.Background()

		if err := bus.HandleCommand(ctx, cmd); err != nil {
			http.Error(w, "could not handle command: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(&cmd); err != nil {
			http.Error(w, "could not encode command: "+err.Error(), http.StatusBadRequest)
			return
		}
	})
}
