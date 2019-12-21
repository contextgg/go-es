package httputils

import (
	"net/http"

	"github.com/contextgg/go-es/es"
)

// CombinedHandler parses commands and sends them
func CombinedHandler(registry es.EventRegistry, eventHandler es.EventHandler, bus es.CommandBus) http.Handler {
	ch := CommandHandler(bus)
	eh := EventHandler(registry, eventHandler)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unsuported method: "+r.Method, http.StatusMethodNotAllowed)
			return
		}

		p := r.URL.Path
		if p == "" || p == "/" {
			eh.ServeHTTP(w, r)
			return
		}
		ch.ServeHTTP(w, r)
	})
}
