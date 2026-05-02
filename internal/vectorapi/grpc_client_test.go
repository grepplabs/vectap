package vectorapi

import (
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
	require.Equal(t, "", tapEventMessageFromProto(nil))

	logEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Log{
			Log: &vectorpb.Log{
				Value: &vectorpb.Value{
					Kind: &vectorpb.Value_RawBytes{RawBytes: []byte("hello")},
				},
			},
		},
	}
	require.Equal(t, "hello", tapEventMessageFromProto(logEvent))

	metricEvent := &vectorpb.EventWrapper{
		Event: &vectorpb.EventWrapper_Metric{
			Metric: &vectorpb.Metric{Name: "requests_total"},
		},
	}
	require.Equal(t, "requests_total", tapEventMessageFromProto(metricEvent))
}
