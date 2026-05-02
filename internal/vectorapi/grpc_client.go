package vectorapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	vectorpb "github.com/grepplabs/vectap/internal/vectorapi/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type GRPCClient struct{}

const (
	eventKindLog    = "log"
	eventKindMetric = "metric"
	eventKindTrace  = "trace"
)

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

		raw, err := protojson.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshal raw tap event: %w", err)
		}

		tapEvent := TapEvent{
			ComponentID: ev.GetComponentId(),
			Kind:        tapEventKindFromProto(ev.GetComponentKind(), ev.GetEvent()),
			Timestamp:   tapEventTimestampFromProto(ev.GetEvent()),
			Message:     tapEventMessageFromProto(ev.GetEvent()),
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

func (c *GRPCClient) newObservabilityClient(endpointURL string) (vectorpb.ObservabilityServiceClient, *grpc.ClientConn, error) {
	target, err := grpcTarget(endpointURL)
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create grpc client connection: %w", err)
	}

	return vectorpb.NewObservabilityServiceClient(conn), conn, nil
}

func grpcTarget(endpointURL string) (string, error) {
	if strings.Contains(endpointURL, "://") {
		u, err := url.Parse(endpointURL)
		if err != nil {
			return "", fmt.Errorf("parse endpoint url: %w", err)
		}
		if u.Host == "" {
			return "", fmt.Errorf("endpoint url %q has empty host", endpointURL)
		}
		return u.Host, nil
	}

	target := endpointURL
	if i := strings.IndexRune(target, '/'); i >= 0 {
		target = target[:i]
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("empty grpc target from endpoint %q", endpointURL)
	}
	return target, nil
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
		return eventKindLog
	}

	switch event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		return eventKindLog
	case *vectorpb.EventWrapper_Metric:
		return eventKindMetric
	case *vectorpb.EventWrapper_Trace:
		return eventKindTrace
	default:
		return eventKindLog
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

func tapEventMessageFromProto(event *vectorpb.EventWrapper) string {
	if event == nil {
		return ""
	}

	switch ev := event.GetEvent().(type) {
	case *vectorpb.EventWrapper_Log:
		if b := ev.Log.GetValue().GetRawBytes(); len(b) > 0 {
			return string(b)
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

func valueToString(v *vectorpb.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	switch x := v.GetKind().(type) {
	case *vectorpb.Value_RawBytes:
		return string(x.RawBytes), true
	case *vectorpb.Value_Integer:
		return fmt.Sprintf("%d", x.Integer), true
	case *vectorpb.Value_Float:
		return fmt.Sprintf("%g", x.Float), true
	case *vectorpb.Value_Boolean:
		return fmt.Sprintf("%t", x.Boolean), true
	case *vectorpb.Value_Timestamp:
		if x.Timestamp == nil {
			return "", false
		}
		return x.Timestamp.AsTime().UTC().Format(time.RFC3339Nano), true
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
