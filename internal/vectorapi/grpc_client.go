package vectorapi

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	vectorpb "github.com/grepplabs/vectap/internal/vectorapi/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type GRPCClient struct{}

func NewGRPCClient() *GRPCClient {
	return &GRPCClient{}
}

func (c *GRPCClient) Tap(ctx context.Context, endpointURL string, req TapRequest) (<-chan TapEvent, <-chan error) {
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

func (c *GRPCClient) Components(ctx context.Context, endpointURL string, _ ComponentsRequest) ([]Component, error) {
	client, conn, err := c.newObservabilityClient(endpointURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck

	resp, err := client.GetComponents(ctx, &vectorpb.GetComponentsRequest{Limit: 0})
	if err != nil {
		return nil, fmt.Errorf("grpc GetComponents: %w", err)
	}

	out := make([]Component, 0, len(resp.GetComponents()))
	for _, item := range resp.GetComponents() {
		out = append(out, Component{
			ComponentID:   item.GetComponentId(),
			ComponentKind: componentKindFromProto(item.GetComponentType()),
			ComponentType: item.GetOnType(),
		})
	}
	return out, nil
}

func (c *GRPCClient) Topology(ctx context.Context, endpointURL string, _ TopologyRequest) ([]TopologyComponent, error) {
	client, conn, err := c.newObservabilityClient(endpointURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck

	resp, err := client.GetComponents(ctx, &vectorpb.GetComponentsRequest{Limit: 0})
	if err != nil {
		return nil, fmt.Errorf("grpc GetComponents for topology: %w", err)
	}

	out := make([]TopologyComponent, 0, len(resp.GetComponents()))
	for _, item := range resp.GetComponents() {
		topology := TopologyComponent{
			ComponentID:   item.GetComponentId(),
			ComponentKind: componentKindFromProto(item.GetComponentType()),
			ComponentType: item.GetOnType(),
		}

		for _, output := range item.GetOutputs() {
			topology.Outputs = append(topology.Outputs, TopologyOutput{OutputID: output.GetOutputId()})
		}

		if metrics := item.GetMetrics(); metrics != nil {
			if v := metrics.ReceivedBytesTotal; v != nil {
				topology.ReceivedBytesTotal = new(float64(*v))
			}
			if v := metrics.SentBytesTotal; v != nil {
				topology.SentBytesTotal = new(float64(*v))
			}
			if v := metrics.ReceivedEventsTotal; v != nil {
				topology.ReceivedEventsTotal = new(float64(*v))
			}
			if v := metrics.SentEventsTotal; v != nil {
				topology.SentEventsTotal = new(float64(*v))
			}
		}

		out = append(out, topology)
	}

	return out, nil
}

//nolint:cyclop
func (c *GRPCClient) tap(ctx context.Context, endpointURL string, req TapRequest, events chan<- TapEvent) error {
	client, conn, err := c.newObservabilityClient(endpointURL)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	outputsPatterns := append([]string{}, req.OutputsOf...)
	inputsPatterns := append([]string{}, req.InputsOf...)
	if len(outputsPatterns) == 0 && len(inputsPatterns) == 0 {
		outputsPatterns = []string{"*"}
	}

	stream, err := client.StreamOutputEvents(ctx, &vectorpb.StreamOutputEventsRequest{
		OutputsPatterns: outputsPatterns,
		InputsPatterns:  inputsPatterns,
		Limit:           int32(tapLimit(req)),    //nolint:gosec
		IntervalMs:      int32(tapInterval(req)), //nolint:gosec
	})
	if err != nil {
		return fmt.Errorf("grpc StreamOutputEvents: %w", err)
	}

	for {
		msg, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("recv StreamOutputEvents: %w", recvErr)
		}

		ev := msg.GetTappedEvent()
		if ev == nil {
			continue
		}
		if !allowGRPCTapEvent(req.EventKinds, ev.GetEvent()) {
			continue
		}

		raw, err := protojson.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshal raw tap event: %w", err)
		}

		tapEvent := TapEvent{
			ComponentID: ev.GetComponentId(),
			Kind:        tapEventKindFromProto(ev.GetComponentKind(), ev.GetEvent()),
			Timestamp:   tapEventTimestampFromProto(ev.GetEvent()),
			Message:     tapEventPayloadFromProto(ev, req.RawFormat),
			Raw:         raw,
			Meta:        tapEventMetaFromProto(ev),
		}

		select {
		case <-ctx.Done():
			return nil
		case events <- tapEvent:
		}
	}
}

func tapEventPayloadFromProto(ev *vectorpb.TappedEvent, rawFormat bool) string {
	if ev == nil {
		return ""
	}

	payload := map[string]any{
		"eventType":     grpcTypename(ev.GetEvent()),
		"componentId":   ev.GetComponentId(),
		"componentType": ev.GetComponentType(),
		"componentKind": ev.GetComponentKind(),
		"timestamp":     tapEventTimestampFromProto(ev.GetEvent()).Format(time.RFC3339Nano),
		"message":       tapEventMessageAnyFromProto(ev.GetEvent(), rawFormat),
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return tapEventMessageFromProto(ev.GetEvent(), rawFormat)
	}
	return string(b)
}

func grpcTypename(event *vectorpb.EventWrapper) string {
	switch event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		return "Log"
	case *vectorpb.EventWrapper_Metric:
		return "Metric"
	case *vectorpb.EventWrapper_Trace:
		return "Trace"
	default:
		return ""
	}
}

//nolint:cyclop
func tapEventMessageAnyFromProto(event *vectorpb.EventWrapper, rawFormat bool) any {
	if event == nil {
		return ""
	}
	if rawFormat {
		if raw := tapEventRawAnyFromProto(event); raw != nil {
			return raw
		}
		return tapEventMessageFromProto(event, true)
	}
	logEvent, ok := event.GetEvent().(*vectorpb.EventWrapper_Log)
	if ok && logEvent.Log != nil {
		if b := logEvent.Log.GetValue().GetRawBytes(); len(b) > 0 {
			return string(b)
		}
		if full := logEventToNative(logEvent.Log); full != nil {
			if obj, ok := full["log"].(map[string]any); ok {
				return obj
			}
			return full
		}
	}
	return tapEventMessageFromProto(event, rawFormat)
}

func tapEventRawAnyFromProto(event *vectorpb.EventWrapper) any {
	if event == nil {
		return nil
	}
	raw, err := protojson.Marshal(event)
	if err != nil {
		return nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func allowGRPCTapEvent(allowedKinds []string, event *vectorpb.EventWrapper) bool {
	return isAllowedEventKind(allowedKinds, grpcTapEventKind(event))
}

func isAllowedEventKind(allowedKinds []string, eventKind string) bool {
	kind := eventKind
	if kind == "" {
		return false
	}

	allowed := allowedKinds
	if len(allowed) == 0 {
		allowed = []string{EventKindLog}
	}
	for _, candidate := range allowed {
		if candidate == kind {
			return true
		}
	}
	return false
}

func grpcTapEventKind(event *vectorpb.EventWrapper) string {
	if event == nil {
		return ""
	}

	switch event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		return EventKindLog
	case *vectorpb.EventWrapper_Metric:
		return EventKindMetric
	case *vectorpb.EventWrapper_Trace:
		return EventKindTrace
	default:
		return ""
	}
}

func (c *GRPCClient) newObservabilityClient(endpointURL string) (vectorpb.ObservabilityServiceClient, *grpc.ClientConn, error) {
	target, useTLS, err := grpcDialTarget(endpointURL)
	if err != nil {
		return nil, nil, err
	}

	transportCreds := insecure.NewCredentials()
	if useTLS {
		transportCreds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}) //nolint:gosec
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		return nil, nil, fmt.Errorf("create grpc client connection: %w", err)
	}

	return vectorpb.NewObservabilityServiceClient(conn), conn, nil
}

func grpcDialTarget(endpointURL string) (string, bool, error) {
	if strings.Contains(endpointURL, "://") {
		u, err := url.Parse(endpointURL)
		if err != nil {
			return "", false, fmt.Errorf("parse endpoint url: %w", err)
		}
		if u.Host == "" {
			return "", false, fmt.Errorf("endpoint url %q has empty host", endpointURL)
		}
		return u.Host, strings.EqualFold(u.Scheme, "https"), nil
	}

	target := endpointURL
	if i := strings.IndexRune(target, '/'); i >= 0 {
		target = target[:i]
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false, fmt.Errorf("empty grpc target from endpoint %q", endpointURL)
	}
	return target, false, nil
}

func componentKindFromProto(v vectorpb.ComponentType) string {
	switch v {
	case vectorpb.ComponentType_COMPONENT_TYPE_UNSPECIFIED:
		return "unknown"
	case vectorpb.ComponentType_COMPONENT_TYPE_SOURCE:
		return "source"
	case vectorpb.ComponentType_COMPONENT_TYPE_TRANSFORM:
		return "transform"
	case vectorpb.ComponentType_COMPONENT_TYPE_SINK:
		return "sink"
	default:
		return "unknown"
	}
}

func tapEventKindFromProto(componentKind string, event *vectorpb.EventWrapper) string {
	if componentKind != "" {
		return componentKind
	}
	if event == nil {
		return EventKindLog
	}

	switch event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		return EventKindLog
	case *vectorpb.EventWrapper_Metric:
		return EventKindMetric
	case *vectorpb.EventWrapper_Trace:
		return EventKindTrace
	default:
		return EventKindLog
	}
}

func tapEventTimestampFromProto(event *vectorpb.EventWrapper) time.Time {
	if event == nil {
		return time.Now().UTC()
	}

	switch ev := event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Metric:
		if ts := ev.Metric.GetTimestamp(); ts != nil {
			return ts.AsTime().UTC()
		}
	case *vectorpb.EventWrapper_Log:
		if ts := ev.Log.GetValue().GetTimestamp(); ts != nil {
			return ts.AsTime().UTC()
		}
	case *vectorpb.EventWrapper_Trace:
		// best effort
		if v, ok := ev.Trace.GetFields()["timestamp"]; ok && v != nil {
			if ts := v.GetTimestamp(); ts != nil {
				return ts.AsTime().UTC()
			}
		}
	}
	return time.Now().UTC()
}

//nolint:cyclop
func tapEventMessageFromProto(event *vectorpb.EventWrapper, rawFormat bool) string {
	if event == nil {
		return ""
	}
	if rawFormat {
		raw, err := protojson.Marshal(event)
		if err != nil {
			return ""
		}
		return string(raw)
	}

	switch ev := event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		if b := ev.Log.GetValue().GetRawBytes(); len(b) > 0 {
			return string(b)
		}
		if full := logEventToNative(ev.Log); full != nil {
			b, err := json.Marshal(full)
			if err == nil {
				return string(b)
			}
		}
	case *vectorpb.EventWrapper_Metric:
		if ev.Metric.GetName() != "" {
			return ev.Metric.GetName()
		}
	case *vectorpb.EventWrapper_Trace:
		// best effort
		if v, ok := ev.Trace.GetFields()["message"]; ok && v != nil {
			if b := v.GetRawBytes(); len(b) > 0 {
				return string(b)
			}
		}
	}
	raw, err := protojson.Marshal(event)
	if err != nil {
		return ""
	}
	return string(raw)
}

func logEventToNative(log *vectorpb.Log) map[string]any {
	if log == nil {
		return nil
	}

	out := make(map[string]any, 4)
	if fields := valueFieldsToNative(log.GetFields()); len(fields) > 0 {
		out["fields"] = fields
	}
	if v, ok := valueToNative(log.GetValue()); ok {
		out["value"] = v
	}
	if md := metadataToNative(log.GetMetadataFull()); md != nil {
		out["metadata"] = md
	}
	if len(out) == 0 {
		return nil
	}
	return map[string]any{"log": out}
}

//nolint:cyclop
func metadataToNative(md *vectorpb.Metadata) map[string]any {
	if md == nil {
		return nil
	}
	out := make(map[string]any)
	if v, ok := valueToNative(md.GetValue()); ok {
		out["value"] = v
	}
	if v := md.GetSourceId(); v != "" {
		out["sourceId"] = v
	}
	if v := md.GetSourceType(); v != "" {
		out["sourceType"] = v
	}
	if upstream := md.GetUpstreamId(); upstream != nil {
		upstreamOut := make(map[string]any)
		if v := upstream.GetComponent(); v != "" {
			upstreamOut["component"] = v
		}
		if v := upstream.GetPort(); v != "" {
			upstreamOut["port"] = v
		}
		if len(upstreamOut) > 0 {
			out["upstreamId"] = upstreamOut
		}
	}
	if b := md.GetSourceEventId(); len(b) > 0 {
		out["sourceEventId"] = base64.StdEncoding.EncodeToString(b)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func tapEventMetaFromProto(ev *vectorpb.TappedEvent) map[string]string {
	meta := map[string]string{}

	if wrapper := ev.GetEvent(); wrapper != nil {
		switch e := wrapper.GetEvent().(type) {
		case *vectorpb.EventWrapper_Log:
			mergeMetadata(meta, e.Log.GetMetadataFull())
		case *vectorpb.EventWrapper_Metric:
			for k, v := range e.Metric.GetTagsV1() {
				meta["tag_"+k] = v
			}
			mergeMetadata(meta, e.Metric.GetMetadataFull())
		case *vectorpb.EventWrapper_Trace:
			mergeMetadata(meta, e.Trace.GetMetadataFull())
		}
	}

	if ev.GetComponentType() != "" {
		meta["component_type"] = ev.GetComponentType()
	}
	if ev.GetComponentKind() != "" {
		meta["component_kind"] = ev.GetComponentKind()
	}

	return meta
}

//nolint:cyclop
func mergeMetadata(meta map[string]string, md *vectorpb.Metadata) {
	if md == nil {
		return
	}
	if v := md.GetSourceId(); v != "" {
		meta["source_id"] = v
	}
	if v := md.GetSourceType(); v != "" {
		meta["source_type"] = v
	}
	if upstream := md.GetUpstreamId(); upstream != nil {
		if v := upstream.GetComponent(); v != "" {
			meta["upstream_component"] = v
		}
		if v := upstream.GetPort(); v != "" {
			meta["upstream_port"] = v
		}
	}
	if val := md.GetValue(); val != nil {
		if m := val.GetMap(); m != nil {
			for k, v := range m.GetFields() {
				if s, ok := valueToString(v); ok {
					meta[k] = s
				}
			}
		}
	}
}

func valueFieldsToNative(fields map[string]*vectorpb.Value) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		if native, ok := valueToNative(v); ok {
			out[k] = native
		}
	}
	return out
}

//nolint:cyclop
func valueToNative(v *vectorpb.Value) (any, bool) {
	if v == nil {
		return nil, false
	}

	switch x := v.GetKind().(type) {
	case *vectorpb.Value_RawBytes:
		return string(x.RawBytes), true
	case *vectorpb.Value_Integer:
		return x.Integer, true
	case *vectorpb.Value_Float:
		return x.Float, true
	case *vectorpb.Value_Boolean:
		return x.Boolean, true
	case *vectorpb.Value_Timestamp:
		if x.Timestamp == nil {
			return nil, false
		}
		return x.Timestamp.AsTime().UTC().Format(time.RFC3339), true
	case *vectorpb.Value_Null:
		return nil, true
	case *vectorpb.Value_Map:
		out := make(map[string]any, len(x.Map.GetFields()))
		for k, vv := range x.Map.GetFields() {
			if native, ok := valueToNative(vv); ok {
				out[k] = native
			}
		}
		return out, true
	case *vectorpb.Value_Array:
		out := make([]any, 0, len(x.Array.GetItems()))
		for _, vv := range x.Array.GetItems() {
			if native, ok := valueToNative(vv); ok {
				out = append(out, native)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

//nolint:cyclop
func valueToString(v *vectorpb.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	switch x := v.GetKind().(type) {
	case *vectorpb.Value_RawBytes:
		return string(x.RawBytes), true
	case *vectorpb.Value_Integer:
		return strconv.FormatInt(x.Integer, 10), true
	case *vectorpb.Value_Float:
		return fmt.Sprintf("%g", x.Float), true
	case *vectorpb.Value_Boolean:
		return strconv.FormatBool(x.Boolean), true
	case *vectorpb.Value_Timestamp:
		if x.Timestamp == nil {
			return "", false
		}
		return x.Timestamp.AsTime().UTC().Format(time.RFC3339), true
	case *vectorpb.Value_Null:
		return "null", true
	case *vectorpb.Value_Map:
		b, err := protojson.Marshal(x.Map)
		if err != nil {
			return "", false
		}
		return string(b), true
	case *vectorpb.Value_Array:
		b, err := protojson.Marshal(x.Array)
		if err != nil {
			return "", false
		}
		return string(b), true
	default:
		return "", false
	}
}
