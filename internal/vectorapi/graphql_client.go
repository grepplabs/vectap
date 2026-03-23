//nolint:tagliatelle
package vectorapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultTapLimit    = 100
	defaultTapInterval = 500
	defaultHTTPTimeout = 15 * time.Second
	tapRetryDelay      = time.Second
	tapReadDeadline    = 60 * time.Second
)

// https://github.com/vectordotdev/vector/blob/master/lib/vector-api-client/graphql/schema.json
// https://github.com/vectordotdev/vector/blob/master/lib/vector-api-client/graphql/subscriptions/output_events_by_component_id_patterns.graphql
const tapSubscriptionQuery = `subscription Tap($outputsPatterns: [String!]!, $inputsPatterns: [String!], $limit: Int!, $interval: Int!, $encoding: EventEncodingType!) {
  outputEventsByComponentIdPatterns(outputsPatterns: $outputsPatterns, inputsPatterns: $inputsPatterns, limit: $limit, interval: $interval) {
    __typename
    ... on Log {
      componentId
      componentType
      componentKind
      message
      timestamp
      string(encoding: $encoding)
    }
    ... on Metric {
      componentId
      componentType
      componentKind
      timestamp
      string(encoding: $encoding)
    }
    ... on Trace {
      componentId
      componentType
      componentKind
      string(encoding: $encoding)
    }
    ... on EventNotification {
      message
    }
  }
}`

const componentsQuery = `query Components($first: Int!, $after: String) {
  components(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        __typename
        componentId
        componentType
      }
    }
  }
}`

const topologyQuery = `query Topology($first: Int!, $after: String) {
  components(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        __typename
        componentId
        componentType
        ... on Source {
          transforms {
            componentId
            componentType
          }
          sinks {
            componentId
            componentType
          }
          outputs {
            outputId
          }
          metrics {
            receivedBytesTotal {
              receivedBytesTotal
            }
            receivedEventsTotal {
              receivedEventsTotal
            }
            sentEventsTotal {
              sentEventsTotal
            }
          }
        }
        ... on Transform {
          sources {
            componentId
            componentType
          }
          transforms {
            componentId
            componentType
          }
          sinks {
            componentId
            componentType
          }
          outputs {
            outputId
          }
          metrics {
            receivedEventsTotal {
              receivedEventsTotal
            }
            sentEventsTotal {
              sentEventsTotal
            }
          }
        }
        ... on Sink {
          sources {
            componentId
            componentType
          }
          transforms {
            componentId
            componentType
          }
          metrics {
            receivedEventsTotal {
              receivedEventsTotal
            }
            sentBytesTotal {
              sentBytesTotal
            }
            sentEventsTotal {
              sentEventsTotal
            }
          }
        }
      }
    }
  }
}`

const defaultComponentsPageSize = 100

type GraphQLWSClient struct {
	dialer     *websocket.Dialer
	httpClient *http.Client
}

func NewGraphQLWSClient() *GraphQLWSClient {
	return &GraphQLWSClient{
		dialer: websocket.DefaultDialer,
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

func (c *GraphQLWSClient) Tap(ctx context.Context, endpointURL string, req TapRequest) (<-chan TapEvent, <-chan error) {
	events := make(chan TapEvent)
	errCh := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errCh)

		for {
			err := c.tap(ctx, endpointURL, req, events)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}

			timer := time.NewTimer(tapRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()

	return events, errCh
}

func (c *GraphQLWSClient) Components(ctx context.Context, endpointURL string, _ ComponentsRequest) ([]Component, error) {
	var (
		after string
		out   []Component
	)

	for {
		payload, err := c.query(ctx, endpointURL, componentsQuery, map[string]any{
			"first": defaultComponentsPageSize,
			"after": nullableCursor(after),
		})
		if err != nil {
			return nil, err
		}

		components, pageInfo, err := decodeComponentsPayload(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, components...)
		if !pageInfo.HasNextPage {
			return out, nil
		}
		after = pageInfo.EndCursor
	}
}

func (c *GraphQLWSClient) Topology(ctx context.Context, endpointURL string, _ TopologyRequest) ([]TopologyComponent, error) {
	var (
		after string
		out   []TopologyComponent
	)

	for {
		payload, err := c.query(ctx, endpointURL, topologyQuery, map[string]any{
			"first": defaultComponentsPageSize,
			"after": nullableCursor(after),
		})
		if err != nil {
			return nil, err
		}

		components, pageInfo, err := decodeTopologyPayload(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, components...)
		if !pageInfo.HasNextPage {
			return out, nil
		}
		after = pageInfo.EndCursor
	}
}

func (c *GraphQLWSClient) tap(ctx context.Context, endpointURL string, req TapRequest, events chan<- TapEvent) error {
	conn, err := c.openTapConnection(ctx, endpointURL)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	if err := c.startTapSubscription(conn, req); err != nil {
		return err
	}

	for {
		msg, done, err := readGraphQLWSMessage(ctx, conn)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		stop, err := c.handleTapMessage(ctx, conn, msg, events)
		if err != nil || stop {
			return err
		}
	}
}

func (c *GraphQLWSClient) openTapConnection(ctx context.Context, endpointURL string) (*websocket.Conn, error) {
	wsURL, err := toWebSocketURL(endpointURL)
	if err != nil {
		return nil, err
	}

	conn, resp, err := c.dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		status := ""
		if resp != nil {
			status = fmt.Sprintf(" (status %s)", resp.Status)
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		return nil, fmt.Errorf("connect vector graphql websocket: %w%s", err, status)
	}
	return conn, nil
}

func (c *GraphQLWSClient) startTapSubscription(conn *websocket.Conn, req TapRequest) error {
	if err := conn.WriteJSON(graphqlWSMessage{Type: "connection_init"}); err != nil {
		return fmt.Errorf("send connection_init: %w", err)
	}

	subscribePayload, err := tapSubscribePayload(req)
	if err != nil {
		return err
	}

	if err := conn.WriteJSON(graphqlWSMessage{
		ID:      "tap",
		Type:    "start",
		Payload: subscribePayload,
	}); err != nil {
		return fmt.Errorf("send start subscription: %w", err)
	}
	return nil
}

func tapSubscribePayload(req TapRequest) (json.RawMessage, error) {
	payload, err := json.Marshal(map[string]any{
		"query":     tapSubscriptionQuery,
		"variables": tapSubscribeVariables(req),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal subscribe payload: %w", err)
	}
	return payload, nil
}

func tapSubscribeVariables(req TapRequest) map[string]any {
	inputsPatterns := append([]string{}, req.InputsOf...)
	outputsPatterns := append([]string{}, req.OutputsOf...)
	if len(outputsPatterns) == 0 && len(inputsPatterns) == 0 {
		outputsPatterns = []string{"*"}
	}

	return map[string]any{
		"outputsPatterns": outputsPatterns,
		"inputsPatterns":  inputsPatterns,
		"limit":           tapLimit(req),
		"interval":        tapInterval(req),
		"encoding":        "JSON",
	}
}

func readGraphQLWSMessage(ctx context.Context, conn *websocket.Conn) (graphqlWSMessage, bool, error) {
	if err := conn.SetReadDeadline(time.Now().Add(tapReadDeadline)); err != nil {
		return graphqlWSMessage{}, false, fmt.Errorf("set read deadline: %w", err)
	}

	var msg graphqlWSMessage
	readErr := conn.ReadJSON(&msg)
	if readErr != nil {
		if !websocket.IsCloseError(readErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) && ctx.Err() == nil {
			return graphqlWSMessage{}, false, fmt.Errorf("read graphql websocket message: %w", readErr)
		}
		return graphqlWSMessage{}, true, nil
	}
	return msg, false, nil
}

func (c *GraphQLWSClient) handleTapMessage(ctx context.Context, conn *websocket.Conn, msg graphqlWSMessage, events chan<- TapEvent) (bool, error) {
	switch msg.Type {
	case "data":
		return false, publishTapEvents(ctx, msg.Payload, events)
	case "error":
		return false, fmt.Errorf("graphql subscription error: %s", string(msg.Payload))
	case "complete":
		return true, nil
	case "connection_ack", "ka":
		return false, nil
	case "ping":
		if err := conn.WriteJSON(graphqlWSMessage{Type: "pong"}); err != nil {
			return false, fmt.Errorf("send pong: %w", err)
		}
		return false, nil
	default:
		return false, nil
	}
}

func publishTapEvents(ctx context.Context, payload json.RawMessage, events chan<- TapEvent) error {
	evs, ok, err := decodeTapMessage(payload)
	if err != nil || !ok {
		return err
	}

	for _, ev := range evs {
		select {
		case <-ctx.Done():
			return nil
		case events <- ev:
		}
	}
	return nil
}

func tapLimit(req TapRequest) int {
	if req.Limit > 0 {
		return req.Limit
	}
	return defaultTapLimit
}

func tapInterval(req TapRequest) int {
	if req.Interval > 0 {
		return req.Interval
	}
	return defaultTapInterval
}

func toWebSocketURL(endpointURL string) (string, error) {
	u, err := url.Parse(endpointURL)
	if err != nil {
		return "", fmt.Errorf("parse endpoint url: %w", err)
	}

	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q", u.Scheme)
	}
	return u.String(), nil
}

type graphqlWSMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type graphQLNextPayload struct {
	Data struct {
		OutputEventsByComponentIDPatterns []tapEventPayload `json:"outputEventsByComponentIdPatterns"`
	} `json:"data"`
}

type tapEventPayload struct {
	Typename      string         `json:"__typename"`
	ComponentID   string         `json:"componentId"`
	ComponentType string         `json:"componentType"`
	ComponentKind string         `json:"componentKind"`
	Message       string         `json:"message"`
	Timestamp     string         `json:"timestamp"`
	String        string         `json:"string"`
	Metadata      map[string]any `json:"metadata"`
}

func decodeTapMessage(payload json.RawMessage) ([]TapEvent, bool, error) {
	var next graphQLNextPayload
	if err := json.Unmarshal(payload, &next); err != nil {
		return nil, false, fmt.Errorf("decode data payload: %w", err)
	}

	if len(next.Data.OutputEventsByComponentIDPatterns) == 0 {
		return nil, false, nil
	}

	result := make([]TapEvent, 0, len(next.Data.OutputEventsByComponentIDPatterns))
	for _, tapEvent := range next.Data.OutputEventsByComponentIDPatterns {
		if tapEvent.Typename == "EventNotification" {
			continue
		}

		event, err := newTapEvent(tapEvent)
		if err != nil {
			return nil, false, err
		}
		result = append(result, event)
	}

	if len(result) == 0 {
		return nil, false, nil
	}
	return result, true, nil
}

func newTapEvent(payload tapEventPayload) (TapEvent, error) {
	timestamp, err := tapEventTimestamp(payload.Timestamp)
	if err != nil {
		return TapEvent{}, err
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return TapEvent{}, fmt.Errorf("marshal raw tap event: %w", err)
	}

	return TapEvent{
		ComponentID: payload.ComponentID,
		Kind:        tapEventKind(payload),
		Timestamp:   timestamp,
		Message:     tapEventMessage(payload),
		Raw:         raw,
		Meta:        tapEventMeta(payload),
	}, nil
}

func tapEventTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Now().UTC(), nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse tap timestamp %q: %w", raw, err)
	}
	return parsed.UTC(), nil
}

func tapEventMessage(payload tapEventPayload) string {
	if payload.String != "" {
		return payload.String
	}
	return payload.Message
}

func tapEventMeta(payload tapEventPayload) map[string]string {
	meta := make(map[string]string, len(payload.Metadata)+2)
	for k, v := range payload.Metadata {
		meta[k] = fmt.Sprint(v)
	}
	if payload.ComponentType != "" {
		meta["component_type"] = payload.ComponentType
	}
	if payload.ComponentKind != "" {
		meta["component_kind"] = payload.ComponentKind
	}
	return meta
}

func tapEventKind(payload tapEventPayload) string {
	if payload.ComponentKind != "" {
		return payload.ComponentKind
	}

	kind := strings.ToLower(payload.Typename)
	if kind == "" {
		return "log"
	}
	return kind
}

type graphQLQueryResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type graphQLComponentsPayload struct {
	Components struct {
		Edges []struct {
			Node struct {
				Typename      string `json:"__typename"`
				ComponentID   string `json:"componentId"`
				ComponentType string `json:"componentType"`
			} `json:"node"`
		} `json:"edges"`
		PageInfo struct {
			HasNextPage bool   `json:"hasNextPage"`
			EndCursor   string `json:"endCursor"`
		} `json:"pageInfo"`
	} `json:"components"`
}

type graphQLTopologyPayload struct {
	Components struct {
		Edges []struct {
			Node struct {
				Typename      string `json:"__typename"`
				ComponentID   string `json:"componentId"`
				ComponentType string `json:"componentType"`
				Sources       []struct {
					ComponentID   string `json:"componentId"`
					ComponentType string `json:"componentType"`
				} `json:"sources"`
				Transforms []struct {
					ComponentID   string `json:"componentId"`
					ComponentType string `json:"componentType"`
				} `json:"transforms"`
				Sinks []struct {
					ComponentID   string `json:"componentId"`
					ComponentType string `json:"componentType"`
				} `json:"sinks"`
				Outputs []struct {
					OutputID string `json:"outputId"`
				} `json:"outputs"`
				Metrics struct {
					ReceivedBytesTotal *struct {
						ReceivedBytesTotal float64 `json:"receivedBytesTotal"`
					} `json:"receivedBytesTotal"`
					ReceivedEventsTotal *struct {
						ReceivedEventsTotal float64 `json:"receivedEventsTotal"`
					} `json:"receivedEventsTotal"`
					SentBytesTotal *struct {
						SentBytesTotal float64 `json:"sentBytesTotal"`
					} `json:"sentBytesTotal"`
					SentEventsTotal *struct {
						SentEventsTotal float64 `json:"sentEventsTotal"`
					} `json:"sentEventsTotal"`
				} `json:"metrics"`
			} `json:"node"`
		} `json:"edges"`
		PageInfo struct {
			HasNextPage bool   `json:"hasNextPage"`
			EndCursor   string `json:"endCursor"`
		} `json:"pageInfo"`
	} `json:"components"`
}

type componentsPageInfo struct {
	HasNextPage bool
	EndCursor   string
}

func (c *GraphQLWSClient) query(ctx context.Context, endpointURL, query string, variables map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal graphql request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("build graphql request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query vector graphql: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query vector graphql: unexpected status %s", resp.Status)
	}

	var gqlResp graphQLQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql query error: %s", gqlResp.Errors[0].Message)
	}
	return gqlResp.Data, nil
}

func decodeComponentsPayload(payload json.RawMessage) ([]Component, componentsPageInfo, error) {
	var resp graphQLComponentsPayload
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, componentsPageInfo{}, fmt.Errorf("decode components payload: %w", err)
	}

	out := make([]Component, 0, len(resp.Components.Edges))
	for _, edge := range resp.Components.Edges {
		kind := strings.ToLower(edge.Node.Typename)
		out = append(out, Component{
			ComponentID:   edge.Node.ComponentID,
			ComponentKind: kind,
			ComponentType: edge.Node.ComponentType,
		})
	}
	return out, componentsPageInfo{
		HasNextPage: resp.Components.PageInfo.HasNextPage,
		EndCursor:   resp.Components.PageInfo.EndCursor,
	}, nil
}

func decodeTopologyPayload(payload json.RawMessage) ([]TopologyComponent, componentsPageInfo, error) {
	var resp graphQLTopologyPayload
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, componentsPageInfo{}, fmt.Errorf("decode topology payload: %w", err)
	}

	out := make([]TopologyComponent, 0, len(resp.Components.Edges))
	for _, edge := range resp.Components.Edges {
		out = append(out, topologyComponentFromGraphQL(
			edge.Node.ComponentID,
			edge.Node.Typename,
			edge.Node.ComponentType,
			edge.Node.Metrics.ReceivedEventsTotal,
			edge.Node.Metrics.ReceivedBytesTotal,
			edge.Node.Metrics.SentEventsTotal,
			edge.Node.Metrics.SentBytesTotal,
			edge.Node.Sources,
			edge.Node.Transforms,
			edge.Node.Sinks,
			edge.Node.Outputs,
		))
	}

	return out, componentsPageInfo{
		HasNextPage: resp.Components.PageInfo.HasNextPage,
		EndCursor:   resp.Components.PageInfo.EndCursor,
	}, nil
}

func topologyComponentFromGraphQL(
	componentID, typename, componentType string,
	receivedEventsTotal *struct {
		ReceivedEventsTotal float64 `json:"receivedEventsTotal"`
	},
	receivedBytesTotal *struct {
		ReceivedBytesTotal float64 `json:"receivedBytesTotal"`
	},
	sentEventsTotal *struct {
		SentEventsTotal float64 `json:"sentEventsTotal"`
	},
	sentBytesTotal *struct {
		SentBytesTotal float64 `json:"sentBytesTotal"`
	},
	sources, transforms, sinks []struct {
		ComponentID   string `json:"componentId"`
		ComponentType string `json:"componentType"`
	},
	outputs []struct {
		OutputID string `json:"outputId"`
	},
) TopologyComponent {
	return TopologyComponent{
		ComponentID:   componentID,
		ComponentKind: strings.ToLower(typename),
		ComponentType: componentType,
		ReceivedEventsTotal: metricValue(receivedEventsTotal, func(v struct {
			ReceivedEventsTotal float64 `json:"receivedEventsTotal"`
		}) float64 {
			return v.ReceivedEventsTotal
		}),
		ReceivedBytesTotal: metricValue(receivedBytesTotal, func(v struct {
			ReceivedBytesTotal float64 `json:"receivedBytesTotal"`
		}) float64 {
			return v.ReceivedBytesTotal
		}),
		SentEventsTotal: metricValue(sentEventsTotal, func(v struct {
			SentEventsTotal float64 `json:"sentEventsTotal"`
		}) float64 {
			return v.SentEventsTotal
		}),
		SentBytesTotal: metricValue(sentBytesTotal, func(v struct {
			SentBytesTotal float64 `json:"sentBytesTotal"`
		}) float64 {
			return v.SentBytesTotal
		}),
		Sources:    topologyRefsFromGraphQL(sources),
		Transforms: topologyRefsFromGraphQL(transforms),
		Sinks:      topologyRefsFromGraphQL(sinks),
		Outputs:    topologyOutputsFromGraphQL(outputs),
	}
}

func metricValue[T any](metric *T, pick func(T) float64) *float64 {
	if metric == nil {
		return nil
	}
	v := pick(*metric)
	return &v
}

func topologyRefsFromGraphQL(items []struct {
	ComponentID   string `json:"componentId"`
	ComponentType string `json:"componentType"`
}) []TopologyComponentRef {
	if len(items) == 0 {
		return nil
	}
	out := make([]TopologyComponentRef, 0, len(items))
	for _, item := range items {
		out = append(out, TopologyComponentRef{
			ComponentID:   item.ComponentID,
			ComponentType: item.ComponentType,
		})
	}
	return out
}

func topologyOutputsFromGraphQL(items []struct {
	OutputID string `json:"outputId"`
}) []TopologyOutput {
	if len(items) == 0 {
		return nil
	}
	out := make([]TopologyOutput, 0, len(items))
	for _, item := range items {
		out = append(out, TopologyOutput{OutputID: item.OutputID})
	}
	return out
}

func nullableCursor(cursor string) any {
	if cursor == "" {
		return nil
	}
	return cursor
}
