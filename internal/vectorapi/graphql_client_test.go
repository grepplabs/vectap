package vectorapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type subscribePayload struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

type tapTestServer struct {
	server        *httptest.Server
	seenVariables chan map[string]interface{}
}

func newTapTestServer(t *testing.T, afterStart func(*websocket.Conn, graphqlWSMessage)) *tapTestServer {
	t.Helper()

	upgrader := websocket.Upgrader{}
	seenVariables := make(chan map[string]interface{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()

		var initMsg graphqlWSMessage
		require.NoError(t, conn.ReadJSON(&initMsg))
		require.Equal(t, "connection_init", initMsg.Type)

		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "connection_ack"}))

		var startMsg graphqlWSMessage
		require.NoError(t, conn.ReadJSON(&startMsg))
		require.Equal(t, "start", startMsg.Type)

		var payload subscribePayload
		require.NoError(t, json.Unmarshal(startMsg.Payload, &payload))
		seenVariables <- payload.Variables

		if afterStart != nil {
			afterStart(conn, startMsg)
		}
	}))
	t.Cleanup(server.Close)

	return &tapTestServer{
		server:        server,
		seenVariables: seenVariables,
	}
}

func startTap(t *testing.T, srv *tapTestServer, req TapRequest) (<-chan TapEvent, <-chan error, context.CancelFunc) {
	t.Helper()

	client := NewGraphQLWSClient()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	events, errs := client.Tap(ctx, srv.server.URL+"/graphql", req)
	return events, errs, cancel
}

func requireTapVariables(t *testing.T, srv *tapTestServer) map[string]interface{} {
	t.Helper()

	select {
	case vars := <-srv.seenVariables:
		return vars
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for start payload")
		return nil
	}
}

func requireEventsClosedAfterCancel(t *testing.T, cancel context.CancelFunc, events <-chan TapEvent) {
	t.Helper()

	cancel()
	select {
	case _, ok := <-events:
		require.False(t, ok, "expected events channel to close after cancellation")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for stream close")
	}
}

func writeTapData(t *testing.T, conn *websocket.Conn, payload map[string]any) {
	t.Helper()

	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "data", Payload: body}))
}

func TestGraphQLWSClientTapReceivesAndDecodesEvent(t *testing.T) {
	ts := "2025-01-02T03:04:05Z"
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		writeTapData(t, conn, map[string]any{
			"data": map[string]any{
				"outputEventsByComponentIdPatterns": []map[string]any{
					{
						"__typename":    "Log",
						"componentId":   "source.my_logs",
						"componentType": "file",
						"componentKind": "source",
						"timestamp":     ts,
						"string":        "hello from vector",
						"metadata": map[string]any{
							"host": "vector-0",
						},
					},
				},
			},
		})
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "complete"}))
	})

	events, errs, cancel := startTap(t, srv, TapRequest{OutputsOf: []string{"source.my_logs"}})
	defer cancel()

	select {
	case err := <-errs:
		require.NoError(t, err)
	case ev, ok := <-events:
		require.True(t, ok, "events channel closed before event")
		require.Equal(t, "source.my_logs", ev.ComponentID)
		require.Equal(t, "source", ev.Kind)
		require.Equal(t, "hello from vector", ev.Message)
		require.Equal(t, ts, ev.Timestamp.Format(time.RFC3339))
		require.Equal(t, "vector-0", ev.Meta["host"])
	}

	requireEventsClosedAfterCancel(t, cancel, events)
}

func TestGraphQLWSClientTapReportsGraphQLError(t *testing.T) {
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "error", Payload: json.RawMessage(`[{"message":"boom"}]`)}))
	})

	events, errs, cancel := startTap(t, srv, TapRequest{})
	defer cancel()
	select {
	case <-events:
	case <-time.After(time.Second):
	}

	select {
	case err := <-errs:
		require.Error(t, err)
		require.Contains(t, err.Error(), "graphql subscription error")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for error")
	}
}

func TestGraphQLWSClientTapSendsWildcardOutputsPatternsWhenNoFilters(t *testing.T) {
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "complete"}))
	})

	events, _, cancel := startTap(t, srv, TapRequest{})
	vars := requireTapVariables(t, srv)
	got, ok := vars["outputsPatterns"]
	require.True(t, ok, "outputsPatterns variable missing")
	patterns, ok := got.([]interface{})
	require.True(t, ok, "outputsPatterns has unexpected type: %T", got)
	require.Equal(t, []interface{}{"*"}, patterns)
	require.Equal(t, float64(defaultTapInterval), vars["interval"])
	require.Equal(t, float64(defaultTapLimit), vars["limit"])

	requireEventsClosedAfterCancel(t, cancel, events)
}

func TestGraphQLWSClientTapUsesConfiguredIntervalAndLimit(t *testing.T) {
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "complete"}))
	})

	events, _, cancel := startTap(t, srv, TapRequest{Interval: 750, Limit: 250})
	vars := requireTapVariables(t, srv)
	require.Equal(t, float64(750), vars["interval"])
	require.Equal(t, float64(250), vars["limit"])

	requireEventsClosedAfterCancel(t, cancel, events)
}

func TestGraphQLWSClientTapUsesInputsAndOutputsPatterns(t *testing.T) {
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "complete"}))
	})

	events, _, cancel := startTap(t, srv, TapRequest{
		OutputsOf: []string{"source.*", "transform.*"},
		InputsOf:  []string{"sink.*"},
	})
	vars := requireTapVariables(t, srv)
	require.Equal(t, []interface{}{"source.*", "transform.*"}, vars["outputsPatterns"])
	require.Equal(t, []interface{}{"sink.*"}, vars["inputsPatterns"])

	requireEventsClosedAfterCancel(t, cancel, events)
}

func TestGraphQLWSClientTapDoesNotForceWildcardOutputsWhenOnlyInputsOfIsSet(t *testing.T) {
	srv := newTapTestServer(t, func(conn *websocket.Conn, _ graphqlWSMessage) {
		require.NoError(t, conn.WriteJSON(graphqlWSMessage{Type: "complete"}))
	})

	events, _, cancel := startTap(t, srv, TapRequest{InputsOf: []string{"sink.*"}})
	vars := requireTapVariables(t, srv)
	require.Equal(t, []interface{}{}, vars["outputsPatterns"])
	require.Equal(t, []interface{}{"sink.*"}, vars["inputsPatterns"])

	requireEventsClosedAfterCancel(t, cancel, events)
}

func TestToWebSocketURL(t *testing.T) {
	got, err := toWebSocketURL("http://127.0.0.1:8686/graphql")
	require.NoError(t, err)
	require.Equal(t, "ws://127.0.0.1:8686/graphql", got)
}

func TestGraphQLWSClientComponentsPaginatesAndMapsKinds(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/graphql", r.URL.Path)

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)

		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		assert.NoError(t, json.Unmarshal(body, &req))
		assert.Contains(t, req.Query, "query Components")

		callCount++
		switch callCount {
		case 1:
			assert.Equal(t, float64(defaultComponentsPageSize), req.Variables["first"])
			assert.Nil(t, req.Variables["after"])
			_, _ = w.Write([]byte(`{"data":{"components":{"edges":[{"node":{"__typename":"Source","componentId":"generate_syslog","componentType":"demo_logs"}}],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}`))
		case 2:
			assert.Equal(t, "cursor-1", req.Variables["after"])
			_, _ = w.Write([]byte(`{"data":{"components":{"edges":[{"node":{"__typename":"Transform","componentId":"remap_syslog","componentType":"remap"}},{"node":{"__typename":"Sink","componentId":"console_out","componentType":"console"}}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`))
		default:
			t.Fatalf("unexpected extra request %d", callCount)
		}
	}))
	defer srv.Close()

	client := NewGraphQLWSClient()
	got, err := client.Components(t.Context(), srv.URL+"/graphql", ComponentsRequest{})
	require.NoError(t, err)
	require.Equal(t, []Component{
		{ComponentID: "generate_syslog", ComponentKind: "source", ComponentType: "demo_logs"},
		{ComponentID: "remap_syslog", ComponentKind: "transform", ComponentType: "remap"},
		{ComponentID: "console_out", ComponentKind: "sink", ComponentType: "console"},
	}, got)
}

func TestGraphQLWSClientComponentsReturnsGraphQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"message":"boom"}]}`))
	}))
	defer srv.Close()

	client := NewGraphQLWSClient()
	_, err := client.Components(t.Context(), srv.URL+"/graphql", ComponentsRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "graphql query error: boom")
}
