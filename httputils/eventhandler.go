package httputils

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/contextgg/go-es/es"
)

// EventJSON for parsing the object differently
type EventJSON struct {
	*es.Event

	Data json.RawMessage `json:"data"`
}

// EventHandler parses events and sends them
func EventHandler(registry es.EventRegistry, eventHandler es.EventHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unsuported method: "+r.Method, http.StatusMethodNotAllowed)
			return
		}

		var wrap EventJSON
		if err := json.NewDecoder(r.Body).Decode(&wrap); err != nil {
			http.Error(w, "could not decode event: "+err.Error(), http.StatusBadRequest)
			return
		}

		data, err := registry.Get(wrap.Type)
		if err != nil {
			http.Error(w, "could not find event: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(wrap.Data, &data); err != nil {
			http.Error(w, "could not decode event data: "+err.Error(), http.StatusBadRequest)
			return
		}

		evt := wrap.Event
		evt.Data = data

		ctx := context.Background()
		if err := eventHandler.HandleEvent(ctx, evt); err != nil {
			http.Error(w, "could not handle event: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}
