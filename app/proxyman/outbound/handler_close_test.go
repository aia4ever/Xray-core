package outbound

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

const closeTestXrayKey core.XrayKey = 1

func TestHandlerCloseClosesStreamSettings(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
	}

	v, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), closeTestXrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})

	h, err := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag: "tag",
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
			StreamSettings: &internet.StreamConfig{
				ProtocolName: "splithttp",
				TransportSettings: []*internet.TransportConfig{{
					ProtocolName: "splithttp",
					Settings:     serial.ToTypedMessage(&splithttp.Config{}),
				}},
			},
		}),
		ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	handler := h.(*Handler)
	var closed atomic.Int32
	if !handler.streamSettings.RegisterOnClose(func() { closed.Add(1) }) {
		t.Fatal("expected the stream settings of a new handler to be open")
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatal("expected Handler.Close to close the stream settings")
	}
}
