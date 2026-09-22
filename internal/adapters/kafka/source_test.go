package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDecode(t *testing.T) {
	ts := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		rec     *kgo.Record
		wantErr bool
		src     string
		ref     string
	}{
		{
			name: "full record",
			rec:  &kgo.Record{Topic: "rail.legs", Value: []byte(`{"source":"card_network","txn_ref":"A","amount_minor":100,"currency":"usd","value_date":"2026-09-15","direction":"credit","counterparty":"VISA"}`), Timestamp: ts},
			src:  "card_network", ref: "A",
		},
		{
			name: "defaults from topic and key",
			rec:  &kgo.Record{Topic: "ledger.legs", Key: []byte("K-1"), Value: []byte(`{"amount_minor":100,"currency":"USD","value_date":"2026-09-15","direction":"debit","counterparty":"VISA"}`), Timestamp: ts},
			src:  "ledger", ref: "K-1",
		},
		{name: "bad json", rec: &kgo.Record{Topic: "ledger.legs", Value: []byte(`{`)}, wantErr: true},
		{name: "invalid leg", rec: &kgo.Record{Topic: "ledger.legs", Value: []byte(`{"txn_ref":"x"}`)}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := decode(tc.rec, topicSource(tc.rec.Topic))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if l.Source != tc.src || l.TxnRef != tc.ref || !l.IngestedAt.Equal(ts) {
				t.Fatalf("leg=%+v", l)
			}
		})
	}
	if topicSource("plain") != "plain" || topicSource("a.b.c") != "a" {
		t.Fatal("topicSource wrong")
	}
	if _, err := New(Config{}, nil, nil); err == nil {
		t.Fatal("empty config must fail")
	}
}
