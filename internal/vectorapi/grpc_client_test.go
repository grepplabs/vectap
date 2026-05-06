package vectorapi

import (
	"encoding/json"
	"testing"
	"time"

	vectorpb "github.com/grepplabs/vectap/internal/vectorapi/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGRPCDialTarget(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantTLS bool
		wantErr bool
	}{
		{name: "host-port", input: "127.0.0.1:6000", want: "127.0.0.1:6000", wantTLS: false},
		{name: "http-url", input: "http://127.0.0.1:8686/graphql", want: "127.0.0.1:8686", wantTLS: false},
		{name: "https-url", input: "https://vector.example:9443/observability", want: "vector.example:9443", wantTLS: true},
		{name: "target-with-path", input: "127.0.0.1:7000/some/path", want: "127.0.0.1:7000", wantTLS: false},
		{name: "invalid-empty", input: "   ", wantErr: true},
		{name: "invalid-url-no-host", input: "http:///graphql", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotTLS, err := grpcDialTarget(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantTLS, gotTLS)
		})
	}
}

func TestComponentKindFromProto(t *testing.T) {
	require.Equal(t, "source", componentKindFromProto(vectorpb.ComponentType_COMPONENT_TYPE_SOURCE))
	require.Equal(t, "transform", componentKindFromProto(vectorpb.ComponentType_COMPONENT_TYPE_TRANSFORM))
	require.Equal(t, "sink", componentKindFromProto(vectorpb.ComponentType_COMPONENT_TYPE_SINK))
	require.Equal(t, "unknown", componentKindFromProto(vectorpb.ComponentType_COMPONENT_TYPE_UNSPECIFIED))
}

func TestTapEventKindFromProto(t *testing.T) {
	require.Equal(t, "source", tapEventKindFromProto("source", nil))
	require.Equal(t, "log", tapEventKindFromProto("", nil))
	require.Equal(t, "log", tapEventKindFromProto("", &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{Log: &vectorpb.Log{}},
	}))
	require.Equal(t, "metric", tapEventKindFromProto("", &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{Metric: &vectorpb.Metric{}},
	}))
	require.Equal(t, "trace", tapEventKindFromProto("", &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Trace{Trace: &vectorpb.Trace{}},
	}))
}

func TestAllowGRPCTapEvent(t *testing.T) {
	require.False(t, allowGRPCTapEvent(nil, nil))
	require.True(t, allowGRPCTapEvent(nil, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{Log: &vectorpb.Log{}},
	}))
	require.False(t, allowGRPCTapEvent(nil, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{Metric: &vectorpb.Metric{}},
	}))
	require.False(t, allowGRPCTapEvent(nil, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Trace{Trace: &vectorpb.Trace{}},
	}))
	require.True(t, allowGRPCTapEvent([]string{"metric"}, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{Metric: &vectorpb.Metric{}},
	}))
	require.True(t, allowGRPCTapEvent([]string{"trace"}, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Trace{Trace: &vectorpb.Trace{}},
	}))
	require.False(t, allowGRPCTapEvent([]string{"metric"}, &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{Log: &vectorpb.Log{}},
	}))
}

func TestTapEventTimestampFromProto(t *testing.T) {
	logTs := timestamppb.New(time.Date(2025, 5, 1, 10, 11, 12, 0, time.UTC))
	metricTs := timestamppb.New(time.Date(2025, 5, 1, 11, 12, 13, 0, time.UTC))

	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Value: &vectorpb.Value{
					Kind: &vectorpb.Value_Timestamp{Timestamp: logTs},
				},
			},
		},
	}
	metricEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{
			Metric: &vectorpb.Metric{
				Timestamp: metricTs,
			},
		},
	}

	require.Equal(t, logTs.AsTime().UTC(), tapEventTimestampFromProto(logEvent))
	require.Equal(t, metricTs.AsTime().UTC(), tapEventTimestampFromProto(metricEvent))

	now := time.Now().UTC()
	got := tapEventTimestampFromProto(nil)
	require.WithinDuration(t, now, got, 2*time.Second)
}

func TestTapEventMessageFromProto(t *testing.T) {
	require.Equal(t, "", tapEventMessageFromProto(nil, false))

	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Value: &vectorpb.Value{
					Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("hello")},
				},
			},
		},
	}
	require.Equal(t, "hello", tapEventMessageFromProto(logEvent, false))

	metricEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{
			Metric: &vectorpb.Metric{Name: "requests_total"},
		},
	}
	require.Equal(t, "requests_total", tapEventMessageFromProto(metricEvent, false))
}

func TestTapEventMessageFromProtoLogMapDecodesRawBytes(t *testing.T) {
	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Value: &vectorpb.Value{
					Kind: &vectorpb.Value_Map{
						Map: &vectorpb.ValueMap{
							Fields: map[string]*vectorpb.Value{
								"host": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("vector-0")}},
								"kind": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("absolute")}},
								"name": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("component_sent_bytes_total")}},
								"counter": {Kind: &vectorpb.Value_Map{Map: &vectorpb.ValueMap{Fields: map[string]*vectorpb.Value{
									"value": {Kind: &vectorpb.Value_Float{Float: 1387424273}},
								}}}},
								"timestamp": {Kind: &vectorpb.Value_Timestamp{Timestamp: timestamppb.New(time.Date(2026, 5, 6, 15, 24, 37, 26551950, time.UTC))}},
								"tags": {Kind: &vectorpb.Value_Map{Map: &vectorpb.ValueMap{Fields: map[string]*vectorpb.Value{
									"component_id":   {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("prom_exporter")}},
									"component_kind": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("sink")}},
									"component_type": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("prometheus_exporter")}},
									"protocol":       {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("http")}},
								}}}},
							},
						},
					},
				},
			},
		},
	}

	msg := tapEventMessageFromProto(logEvent, false)
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(msg), &got))
	logObj, ok := got["log"].(map[string]any)
	require.True(t, ok)
	valueObj, ok := logObj["value"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "vector-0", valueObj["host"])
	require.Equal(t, "absolute", valueObj["kind"])
	require.Equal(t, "component_sent_bytes_total", valueObj["name"])
	require.Equal(t, "2026-05-06T15:24:37Z", valueObj["timestamp"])

	counter, ok := valueObj["counter"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 1387424273.0, counter["value"])

	tags, ok := valueObj["tags"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "prom_exporter", tags["component_id"])
	require.Equal(t, "sink", tags["component_kind"])
	require.Equal(t, "prometheus_exporter", tags["component_type"])
	require.Equal(t, "http", tags["protocol"])
}

func TestTapEventMessageFromProtoLogFieldsDecodesRawBytes(t *testing.T) {
	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Fields: map[string]*vectorpb.Value{
					"host": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("vector-0")}},
					"kind": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("absolute")}},
					"name": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("component_sent_bytes_total")}},
					"counter": {Kind: &vectorpb.Value_Map{Map: &vectorpb.ValueMap{Fields: map[string]*vectorpb.Value{
						"value": {Kind: &vectorpb.Value_Float{Float: 1412861968}},
					}}}},
				},
				MetadataFull: &vectorpb.Metadata{
					SourceId:      strPtr("internal_metrics"),
					SourceType:    strPtr("internal_metrics"),
					UpstreamId:    &vectorpb.OutputId{Component: "metrics-remap"},
					SourceEventId: []byte{0x01, 0x02, 0x03},
				},
			},
		},
	}

	msg := tapEventMessageFromProto(logEvent, false)
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(msg), &got))
	logObj, ok := got["log"].(map[string]any)
	require.True(t, ok)
	fieldsObj, ok := logObj["fields"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "vector-0", fieldsObj["host"])
	require.Equal(t, "absolute", fieldsObj["kind"])
	require.Equal(t, "component_sent_bytes_total", fieldsObj["name"])
	counter, ok := fieldsObj["counter"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 1412861968.0, counter["value"])
	metadata, ok := logObj["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "internal_metrics", metadata["sourceId"])
	require.Equal(t, "internal_metrics", metadata["sourceType"])
	upstream, ok := metadata["upstreamId"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "metrics-remap", upstream["component"])
	require.Equal(t, "AQID", metadata["sourceEventId"])
}

func strPtr(v string) *string { return &v }

func TestTapEventMessageFromProtoRawFormat(t *testing.T) {
	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Fields: map[string]*vectorpb.Value{
					"host": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("vector-0")}},
				},
			},
		},
	}

	msg := tapEventMessageFromProto(logEvent, true)
	require.Contains(t, msg, `"log"`)
	require.Contains(t, msg, `"rawBytes"`)
}

func TestTapEventPayloadFromProtoUsesFlattenedShape(t *testing.T) {
	ev := &vectorpb.TappedEvent{
		ComponentId:   "comp-1",
		ComponentType: "remap",
		ComponentKind: "transform",
		Event: &vectorpb.EventWrapper{
			Event: &vectorpb.EventWrapper_Log{
				Log: &vectorpb.Log{
					Fields: map[string]*vectorpb.Value{
						"host": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("vector-0")}},
					},
					MetadataFull: &vectorpb.Metadata{
						SourceId:   strPtr("internal_metrics"),
						SourceType: strPtr("internal_metrics"),
					},
				},
			},
		},
	}

	msg := tapEventPayloadFromProto(ev, false)
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(msg), &got))
	require.Equal(t, "Log", got["eventType"])
	require.Equal(t, "comp-1", got["componentId"])
	require.Equal(t, "remap", got["componentType"])
	require.Equal(t, "transform", got["componentKind"])
	require.NotEmpty(t, got["timestamp"])
	_, hasString := got["string"]
	require.False(t, hasString)
	message, ok := got["message"].(map[string]any)
	require.True(t, ok)
	fields, ok := message["fields"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "vector-0", fields["host"])
	metadata, ok := message["metadata"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "internal_metrics", metadata["sourceId"])
	require.Equal(t, "internal_metrics", metadata["sourceType"])
	_, hasTopLevelMetadata := got["metadata"]
	require.False(t, hasTopLevelMetadata)
}

func TestTapEventPayloadFromProtoRawFormatUsesRawEventWrapper(t *testing.T) {
	ev := &vectorpb.TappedEvent{
		ComponentId:   "comp-1",
		ComponentType: "remap",
		ComponentKind: "transform",
		Event: &vectorpb.EventWrapper{
			Event: &vectorpb.EventWrapper_Log{
				Log: &vectorpb.Log{
					Fields: map[string]*vectorpb.Value{
						"host": {Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("vector-0")}},
					},
				},
			},
		},
	}

	msg := tapEventPayloadFromProto(ev, true)
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(msg), &got))
	require.Equal(t, "Log", got["eventType"])
	message, ok := got["message"].(map[string]any)
	require.True(t, ok)
	_, hasLog := message["log"]
	require.True(t, hasLog)
}
