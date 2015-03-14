package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"

	"github.com/mattn/go-scan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
)

type MetricType string

func (t *MetricType) UnmarshalText(b []byte) error {
	s := string(b)
	switch s {
	case "gauge", "counter":
		*t = MetricType(s)
		return nil
	}
	return fmt.Errorf("Unknown metric type: %s", s)
}

type CountMode string

func (m *CountMode) UnmarshalText(b []byte) error {
	s := string(b)
	switch s {
	case "value", "exist", "non_exist":
		*m = CountMode(s)
		return nil
	}
	return fmt.Errorf("Unknown count mode: %s", s)
}

type Handler interface {
	HandleEvent(*message.Event) error
}

type Config struct {
	Listen  string
	Metrics map[string]Metric
}

type Metric struct {
	Type      MetricType
	Help      string
	Value     string
	CountMode CountMode `toml:"count_mode"`
	Labels    map[string]string
	labelKeys []string
}

func (m *Metric) New(name string) (Handler, error) {
	if m.CountMode == "" {
		m.CountMode = "value"
	}
	for key := range m.Labels {
		m.labelKeys = append(m.labelKeys, key)
	}
	sort.Strings(m.labelKeys)

	switch m.Type {
	case "gauge":
		return newGauge(name, m)
	case "counter":
		return newCounter(name, m)
	}
	return nil, fmt.Errorf("Unknown metric type: %v", m.Type)
}

func (m *Metric) labelValues(ev *message.Event) ([]string, error) {
	var vals []string
	for _, key := range m.labelKeys {
		var s string
		if err := m.scan(ev, m.Labels[key], &s); err != nil {
			return nil, err
		}
		vals = append(vals, s)
	}
	return vals, nil
}

func (m *Metric) scan(ev *message.Event, p string, t interface{}) error {
	err := scan.ScanTree(ev.Record, p, t)
	if err == nil || strings.HasPrefix(err.Error(), "invalid path") {
		err = nil
	}
	return err
}

type Gauge struct {
	Metric
	*prometheus.GaugeVec
}

func newGauge(name string, m *Metric) (*Gauge, error) {
	v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: m.Help}, m.labelKeys)
	if err := prometheus.Register(v); err != nil {
		return nil, err
	}
	return &Gauge{Metric: *m, GaugeVec: v}, nil
}

func (g *Gauge) HandleEvent(ev *message.Event) error {
	var v float64
	if err := g.scan(ev, g.Value, &v); err != nil {
		return err
	}
	lvals, err := g.labelValues(ev)
	if err != nil {
		return err
	}
	g.WithLabelValues(lvals...).Set(v)
	return nil
}

type Counter struct {
	Metric
	*prometheus.CounterVec
}

func newCounter(name string, m *Metric) (*Counter, error) {
	v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: m.Help}, m.labelKeys)
	if err := prometheus.Register(v); err != nil {
		return nil, err
	}
	return &Counter{Metric: *m, CounterVec: v}, nil
}

func (g *Counter) HandleEvent(ev *message.Event) error {
	lvals, err := g.labelValues(ev)
	if err != nil {
		return err
	}
	c := g.WithLabelValues(lvals...)
	if g.Value == "" {
		c.Inc()
		return nil
	}
	if g.CountMode == "value" {
		var v float64
		if err := g.scan(ev, g.Value, &v); err != nil {
			return err
		}
		if v < 0 {
			return errors.New("Counter value must be >=0")
		}
		c.Add(v)
		return nil
	}
	var v interface{}
	err = scan.ScanTree(ev.Record, g.Value, &v)
	if g.CountMode == "exist" && err == nil || g.CountMode == "non_exist" && err != nil {
		c.Inc()
	}
	return nil
}

type OutPrometheus struct {
	env      *plugin.Env
	conf     Config
	ln       net.Listener
	handlers []Handler
}

func (p *OutPrometheus) Init(env *plugin.Env) error {
	p.env = env
	return env.ReadConfig(&p.conf)
}

func (p *OutPrometheus) Start() (err error) {
	for name, metric := range p.conf.Metrics {
		var h Handler
		if h, err = metric.New(name); err != nil {
			return
		}
		p.handlers = append(p.handlers, h)
	}
	http.Handle("/metrics", prometheus.Handler())
	if p.ln, err = net.Listen("tcp", p.conf.Listen); err != nil {
		return
	}
	go new(http.Server).Serve(p.ln)
	return nil
}

func (p *OutPrometheus) Encode(ev *message.Event) (buffer.Sizer, error) {
	for _, h := range p.handlers {
		if err := h.HandleEvent(ev); err != nil {
			p.env.Log.Error(err)
		}
	}
	return buffer.StringItem(""), nil
}

func (p *OutPrometheus) Write(l []buffer.Sizer) (int, error) {
	return len(l), nil
}

func (p *OutPrometheus) Close() error {
	return p.ln.Close()
}

func main() {
	plugin.New("out-prometheus", func() plugin.Plugin { return &OutPrometheus{} }).Run()
}
