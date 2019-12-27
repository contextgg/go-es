package httputils

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/contextgg/go-es/es"
)

const testevent = `
{
    "type": "TestEventData",
    "timestamp": "2019-12-27T14:32:08.415654156Z",
    "aggregate_id": "",
    "aggregate_type": "",
    "version": 0,
    "data": {
        "Message": "Hello guy!"
    },
    "metadata": null
}
`

type TestEventData struct {
	Message string
}
type TestEventHandler struct {
	Count int
}

func (t *TestEventHandler) HandleEvent(context.Context, *es.Event) error {
	t.Count = t.Count + 1
	return nil
}

func TestHttpEventHandler(t *testing.T) {
	registry := es.NewEventRegistry()
	registry.Set(&TestEventData{}, false)

	eh := &TestEventHandler{}

	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("POST", "/", strings.NewReader(testevent))
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := EventHandler(registry, eh)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	// Check the response body is what we expect.
	expected := 1
	if eh.Count != expected {
		t.Errorf("handler didn't handle event: got %v want %v", eh.Count, expected)
	}
}
