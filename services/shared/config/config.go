package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSchedulerArtifactStoreCapacity = 1_000_000

type Node struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type SchedulerDiscoveryKubernetesConfig struct {
	Namespace             string `json:"namespace"`
	ServiceName           string `json:"service_name"`
	Port                  int32  `json:"port"`
	Scheme                string `json:"scheme"`
	IgnorePodSelector     string `json:"ignore_pod_selector"`
	NoSchedulePodSelector string `json:"no_schedule_pod_selector"`
}

type SchedulerDiscoveryConfig struct {
	Mode       string                             `json:"mode"`
	Kubernetes SchedulerDiscoveryKubernetesConfig `json:"kubernetes"`
}

// NodeResourceLimit defines per-node resource thresholds for scheduling
// eligibility. A node exceeding any configured limit is excluded from
// scheduling candidates. Nil (absent) fields impose no limit.
//
// Allocated-percent limits (CPU and memory) can legitimately exceed 100%
// because allocated resources reflect the sum of all sandbox reservations,
// which may overcommit the physical capacity of the node.
type NodeResourceLimit struct {
	MaxSandboxCount           *uint32 `json:"max_sandbox_count"`
	MaxSandboxStartingCount   *uint32 `json:"max_sandbox_starting_count"`
	MaxCPUUsedPercent         *uint32 `json:"max_cpu_used_percent"`
	MaxCPUAllocatedPercent    *uint32 `json:"max_cpu_allocated_percent"` // can exceed 100 (overcommit)
	MaxMemoryUsedPercent      *uint32 `json:"max_memory_used_percent"`
	MaxMemoryAllocatedPercent *uint32 `json:"max_memory_allocated_percent"` // can exceed 100 (overcommit)

	// AdmissionMode selects which per-sandbox memory figure feeds
	// max_memory_allocated_percent:
	//   "nominal" (default/absent): sum of VM sizes (today's behavior).
	//   "request": sum of memory requests (memory_request_mb, defaulting to the
	//              VM size), plus the MaxMemoryLimitOvercommitRatio cap on the
	//              sum of nominal limits.
	//   "shadow":  decide exactly like "nominal" but evaluate "request" in
	//              parallel, log divergences and count them in
	//              agentenv_scheduler_admission_shadow_total.
	// MaxMemoryUsedPercent (real host usage) is evaluated in every mode and is
	// the backstop against host OOM when VMs grow towards their limits.
	AdmissionMode *string `json:"admission_mode"`
	// MaxMemoryLimitOvercommitRatio caps (sum of nominal VM sizes) /
	// memory_total_bytes in "request" mode (and in the hypothetical "request"
	// decision of "shadow" mode). Default 1.5. Ignored in "nominal" mode so
	// that the default behavior is unchanged.
	MaxMemoryLimitOvercommitRatio *float64 `json:"max_memory_limit_overcommit_ratio"`

	// MaxDiskUsedPercent caps the real filesystem usage of DiskMountPoint.
	// It is a usage ratio, so unlike the allocated limits it can never
	// legitimately exceed 100. The check runs in every admission mode;
	// Pausing a sandbox writes a chain of about one VM's worth of state to
	// this filesystem, so a node near full loses VMs when a pause fails.
	MaxDiskUsedPercent *uint32 `json:"max_disk_used_percent"`
	// DiskMountPoint selects the heartbeat disk entry the disk limit reads,
	// matched exactly against DiskMetric.mount_point. Defaults to
	// DefaultDiskMountPoint when absent.
	DiskMountPoint *string `json:"disk_mount_point"`

	// Limits that apply to the sum of the active running set plus paused
	// sandboxes. Paused sandboxes have released their VM-side CPU / memory
	// but still occupy persisted state on the node, so operators may want a
	// separate ceiling on total node footprint (including paused) on top of
	// the active-only ceilings above.
	MaxSandboxCountIncludingPaused         *uint32 `json:"max_sandbox_count_including_paused"`
	MaxAllocatedCPUIncludingPaused         *uint32 `json:"max_allocated_cpu_including_paused"`
	MaxAllocatedMemoryBytesIncludingPaused *uint64 `json:"max_allocated_memory_bytes_including_paused"`

	// AdmissionLayers are the VM shapes whose admission is published per node
	// by the scheduler and read by the gateway's /nodes response. When absent
	// the default layers (8, 16, 32 and 64 GiB) apply. CPUCount is optional:
	// the scheduler does not know the layer-to-vCPU map, so an absent value
	// projects no extra CPU and the cpu reason reflects only the node's
	// current CPU state.
	AdmissionLayers []AdmissionLayer `json:"admission_layers"`
	// MemoryCapacitySource selects the memory base the admission ceiling is
	// computed over: "allocatable" (default) uses the Kubernetes node's
	// status.allocatable.memory minus the runtime overhead; "total" is the
	// kill switch that restores the historical memory_total math.
	MemoryCapacitySource *string `json:"memory_capacity_source"`
	// ShapeAdmission enables the published per-layer verdict and the resume
	// gating that projects the paused sandbox shape. Default "on"; "off" is
	// the kill switch that restores the pre-OPSAI-6200 routing.
	ShapeAdmission *string `json:"shape_admission"`
	// CrossNodeResume allows a paused sandbox rejected on its owner to resume
	// on another admitted node. Off by default: moving a resume before the
	// paused artifacts are cluster-readable loses the sandbox.
	CrossNodeResume *bool `json:"cross_node_resume"`
	// AdmissionReservation reserves, at decision time, the shape of every
	// admitted create and resume until the gateway confirms its outcome.
	// Default "on": absence of the key must not disable the correction.
	AdmissionReservation *string `json:"admission_reservation"`
	// AdmissionReservationTTL is how long a reservation keeps occupying node
	// capacity: until the gateway settles it, a confirmed one is released on
	// the first node snapshot produced after the confirmation, and the TTL is
	// the backstop when that snapshot never arrives. Deployments should set it
	// to gateway.request_timeout + AdmissionReservationTTLMargin; the default
	// is AdmissionReservationTTLFor(DefaultGatewayRequestTimeout).
	AdmissionReservationTTL *string `json:"admission_reservation_ttl"`
}

// AdmissionLayer is one VM shape whose admission the scheduler publishes.
type AdmissionLayer struct {
	MemoryGB uint64  `json:"memory_gb"`
	CPUCount *uint32 `json:"cpu_count"`
}

// Admission modes accepted by NodeResourceLimit.AdmissionMode.
const (
	AdmissionModeNominal = "nominal"
	AdmissionModeRequest = "request"
	AdmissionModeShadow  = "shadow"

	// DefaultMaxMemoryLimitOvercommitRatio applies when
	// MaxMemoryLimitOvercommitRatio is unset.
	DefaultMaxMemoryLimitOvercommitRatio = 1.5

	// DefaultDiskMountPoint is the filesystem the disk limit reads when
	// disk_mount_point is unset. Sandbox state, persisted paused sandboxes and
	// the image cache all live on the node's /workspace volume.
	DefaultDiskMountPoint = "/workspace"

	// Memory capacity sources accepted by
	// NodeResourceLimit.MemoryCapacitySource.
	MemoryCapacitySourceAllocatable = "allocatable"
	MemoryCapacitySourceTotal       = "total"

	// Shape admission modes accepted by NodeResourceLimit.ShapeAdmission.
	ShapeAdmissionOn  = "on"
	ShapeAdmissionOff = "off"

	// DefaultGatewayRequestTimeout is the gateway request timeout applied when
	// gateway.request_timeout is absent.
	DefaultGatewayRequestTimeout = 30 * time.Second

	// AdmissionReservationTTLMargin is how much longer than the gateway
	// request timeout a reservation is held. It covers the settlement RPC,
	// the heartbeat that reports the new sandbox, and a cold create whose
	// image pull runs to the edge of the request timeout.
	AdmissionReservationTTLMargin = 60 * time.Second

	// DefaultAdmissionReservationTTL applies when admission_reservation_ttl is
	// absent. It is derived from the default request timeout plus the margin,
	// so a slow create is not released while it still runs, and short enough
	// that a lost gateway response stops consuming node capacity. A deployment
	// that raises gateway.request_timeout must raise this key with it; the
	// deploy parity test enforces that for the shipped configs.
	DefaultAdmissionReservationTTL = DefaultGatewayRequestTimeout + AdmissionReservationTTLMargin
)

// AdmissionReservationTTLFor returns the reservation TTL that outlives a
// gateway request timeout by AdmissionReservationTTLMargin.
func AdmissionReservationTTLFor(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		requestTimeout = DefaultGatewayRequestTimeout
	}
	return requestTimeout + AdmissionReservationTTLMargin
}

// DefaultAdmissionLayerMemoryGB is the set of VM shapes published per node
// when admission_layers is absent.
var DefaultAdmissionLayerMemoryGB = []uint64{8, 16, 32, 64}

// ResolvedMemoryCapacitySource returns the configured memory capacity source,
// defaulting to allocatable. Unknown values resolve to allocatable so a typo
// cannot silently restore the total math; Validate rejects them at startup.
func (l *NodeResourceLimit) ResolvedMemoryCapacitySource() string {
	if l == nil || l.MemoryCapacitySource == nil {
		return MemoryCapacitySourceAllocatable
	}
	switch source := strings.ToLower(strings.TrimSpace(*l.MemoryCapacitySource)); source {
	case MemoryCapacitySourceTotal:
		return MemoryCapacitySourceTotal
	default:
		return MemoryCapacitySourceAllocatable
	}
}

// ResolvedShapeAdmission returns "on" or "off", defaulting to "on".
func (l *NodeResourceLimit) ResolvedShapeAdmission() string {
	if l == nil || l.ShapeAdmission == nil {
		return ShapeAdmissionOn
	}
	if strings.ToLower(strings.TrimSpace(*l.ShapeAdmission)) == ShapeAdmissionOff {
		return ShapeAdmissionOff
	}
	return ShapeAdmissionOn
}

// ShapeAdmissionEnabled reports whether the per-layer verdict and resume
// projection are active.
func (l *NodeResourceLimit) ShapeAdmissionEnabled() bool {
	return l.ResolvedShapeAdmission() == ShapeAdmissionOn
}

// ResolvedAdmissionLayers returns the configured layers, or the 8/16/32/64 GiB
// defaults when admission_layers is absent.
func (l *NodeResourceLimit) ResolvedAdmissionLayers() []AdmissionLayer {
	if l == nil || len(l.AdmissionLayers) == 0 {
		layers := make([]AdmissionLayer, 0, len(DefaultAdmissionLayerMemoryGB))
		for _, memoryGB := range DefaultAdmissionLayerMemoryGB {
			layers = append(layers, AdmissionLayer{MemoryGB: memoryGB})
		}
		return layers
	}
	layers := make([]AdmissionLayer, len(l.AdmissionLayers))
	copy(layers, l.AdmissionLayers)
	return layers
}

// CrossNodeResumeEnabled reports whether a resume may move off its owner.
func (l *NodeResourceLimit) CrossNodeResumeEnabled() bool {
	return l != nil && l.CrossNodeResume != nil && *l.CrossNodeResume
}

// ResolvedAdmissionReservation returns "on" or "off", defaulting to "on".
func (l *NodeResourceLimit) ResolvedAdmissionReservation() string {
	if l == nil || l.AdmissionReservation == nil {
		return ShapeAdmissionOn
	}
	if strings.ToLower(strings.TrimSpace(*l.AdmissionReservation)) == ShapeAdmissionOff {
		return ShapeAdmissionOff
	}
	return ShapeAdmissionOn
}

// AdmissionReservationEnabled reports whether the in-flight reservation
// ledger is on.
func (l *NodeResourceLimit) AdmissionReservationEnabled() bool {
	return l.ResolvedAdmissionReservation() == ShapeAdmissionOn
}

// ResolvedAdmissionReservationTTL returns the configured reservation TTL,
// defaulting to DefaultAdmissionReservationTTL. An unparsable or non-positive
// value also falls back to the default; Validate rejects it at startup so a
// typo cannot go unnoticed.
func (l *NodeResourceLimit) ResolvedAdmissionReservationTTL() time.Duration {
	if l == nil || l.AdmissionReservationTTL == nil {
		return DefaultAdmissionReservationTTL
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(*l.AdmissionReservationTTL))
	if err != nil || ttl <= 0 {
		return DefaultAdmissionReservationTTL
	}
	return ttl
}

// ResolvedAdmissionMode returns the configured mode, defaulting to nominal.
// Unknown values resolve to nominal (fail-safe); Validate rejects them at
// startup so a typo cannot go unnoticed.
func (l *NodeResourceLimit) ResolvedAdmissionMode() string {
	if l == nil || l.AdmissionMode == nil {
		return AdmissionModeNominal
	}
	switch mode := strings.ToLower(strings.TrimSpace(*l.AdmissionMode)); mode {
	case AdmissionModeRequest, AdmissionModeShadow:
		return mode
	default:
		return AdmissionModeNominal
	}
}

// ResolvedMaxMemoryLimitOvercommitRatio returns the configured ratio or the default.
func (l *NodeResourceLimit) ResolvedMaxMemoryLimitOvercommitRatio() float64 {
	if l == nil || l.MaxMemoryLimitOvercommitRatio == nil {
		return DefaultMaxMemoryLimitOvercommitRatio
	}
	return *l.MaxMemoryLimitOvercommitRatio
}

// ResolvedDiskMountPoint returns the mount point the disk limit reads,
// defaulting to DefaultDiskMountPoint when it is unset or blank.
func (l *NodeResourceLimit) ResolvedDiskMountPoint() string {
	if l == nil || l.DiskMountPoint == nil || strings.TrimSpace(*l.DiskMountPoint) == "" {
		return DefaultDiskMountPoint
	}
	return strings.TrimSpace(*l.DiskMountPoint)
}

// Validate rejects unknown admission modes, non-positive ratios and disk
// percentages above 100.
func (l *NodeResourceLimit) Validate() error {
	if l == nil {
		return nil
	}
	if l.AdmissionMode != nil {
		switch strings.ToLower(strings.TrimSpace(*l.AdmissionMode)) {
		case AdmissionModeNominal, AdmissionModeRequest, AdmissionModeShadow:
		default:
			return fmt.Errorf("scheduler.node_resource_limit.admission_mode must be one of nominal, request, shadow, got %q", *l.AdmissionMode)
		}
	}
	if l.MaxMemoryLimitOvercommitRatio != nil && !(*l.MaxMemoryLimitOvercommitRatio > 0) {
		return errors.New("scheduler.node_resource_limit.max_memory_limit_overcommit_ratio must be greater than zero")
	}
	if l.MaxDiskUsedPercent != nil && *l.MaxDiskUsedPercent > 100 {
		return fmt.Errorf("scheduler.node_resource_limit.max_disk_used_percent must be at most 100, got %d", *l.MaxDiskUsedPercent)
	}
	if l.MemoryCapacitySource != nil {
		switch strings.ToLower(strings.TrimSpace(*l.MemoryCapacitySource)) {
		case MemoryCapacitySourceAllocatable, MemoryCapacitySourceTotal:
		default:
			return fmt.Errorf("scheduler.node_resource_limit.memory_capacity_source must be one of allocatable, total, got %q", *l.MemoryCapacitySource)
		}
	}
	if l.ShapeAdmission != nil {
		switch strings.ToLower(strings.TrimSpace(*l.ShapeAdmission)) {
		case ShapeAdmissionOn, ShapeAdmissionOff:
		default:
			return fmt.Errorf("scheduler.node_resource_limit.shape_admission must be one of on, off, got %q", *l.ShapeAdmission)
		}
	}
	if l.AdmissionReservation != nil {
		switch strings.ToLower(strings.TrimSpace(*l.AdmissionReservation)) {
		case ShapeAdmissionOn, ShapeAdmissionOff:
		default:
			return fmt.Errorf("scheduler.node_resource_limit.admission_reservation must be one of on, off, got %q", *l.AdmissionReservation)
		}
	}
	if l.AdmissionReservationTTL != nil {
		ttl, err := time.ParseDuration(strings.TrimSpace(*l.AdmissionReservationTTL))
		if err != nil {
			return fmt.Errorf("scheduler.node_resource_limit.admission_reservation_ttl must be a duration, got %q", *l.AdmissionReservationTTL)
		}
		if ttl <= 0 {
			return fmt.Errorf("scheduler.node_resource_limit.admission_reservation_ttl must be greater than zero, got %q", *l.AdmissionReservationTTL)
		}
	}
	seenLayers := make(map[uint64]struct{}, len(l.AdmissionLayers))
	for _, layer := range l.AdmissionLayers {
		if layer.MemoryGB == 0 {
			return errors.New("scheduler.node_resource_limit.admission_layers memory_gb must be greater than zero")
		}
		if _, duplicate := seenLayers[layer.MemoryGB]; duplicate {
			return fmt.Errorf("scheduler.node_resource_limit.admission_layers has duplicate memory_gb %d", layer.MemoryGB)
		}
		seenLayers[layer.MemoryGB] = struct{}{}
	}
	return nil
}

type SchedulerConfig struct {
	GRPCListenAddr          string                   `json:"grpc_listen_addr"`
	MetricsListenAddr       string                   `json:"metrics_listen_addr"`
	Strategy                string                   `json:"strategy"`
	ReportTTL               time.Duration            `json:"report_ttl"`
	BindingTTL              time.Duration            `json:"binding_ttl"`
	RedisAddr               string                   `json:"redis_addr"`
	ArtifactStoreCapacity   int                      `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                      `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                   `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit       `json:"node_resource_limit"`
}

func (s *SchedulerConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		GRPCListenAddr          *string                   `json:"grpc_listen_addr"`
		MetricsListenAddr       *string                   `json:"metrics_listen_addr"`
		Strategy                *string                   `json:"strategy"`
		ReportTTL               json.RawMessage           `json:"report_ttl"`
		BindingTTL              json.RawMessage           `json:"binding_ttl"`
		RedisAddr               *string                   `json:"redis_addr"`
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.GRPCListenAddr != nil {
		s.GRPCListenAddr = *parsed.GRPCListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		s.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.Strategy != nil {
		s.Strategy = *parsed.Strategy
	}
	if parsed.Nodes != nil {
		s.Nodes = *parsed.Nodes
	}
	if parsed.Discovery != nil {
		s.Discovery = *parsed.Discovery
	}
	if parsed.NodeResourceLimit != nil {
		s.NodeResourceLimit = parsed.NodeResourceLimit
	}
	if parsed.RedisAddr != nil {
		s.RedisAddr = *parsed.RedisAddr
	}
	if parsed.ArtifactStoreCapacity != nil {
		s.ArtifactStoreCapacity = *parsed.ArtifactStoreCapacity
	}
	if parsed.ArtifactLookupNodeLimit != nil {
		s.ArtifactLookupNodeLimit = *parsed.ArtifactLookupNodeLimit
	}

	if len(bytes.TrimSpace(parsed.ReportTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReportTTL, "scheduler.report_ttl")
		if err != nil {
			return err
		}
		s.ReportTTL = d
	}
	if len(bytes.TrimSpace(parsed.BindingTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingTTL, "scheduler.binding_ttl")
		if err != nil {
			return err
		}
		s.BindingTTL = d
	}

	return nil
}

func parseSchedulerDuration(raw json.RawMessage, field string) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

type GatewayConfig struct {
	HTTPListenAddr         string        `json:"http_listen_addr"`
	MetricsListenAddr      string        `json:"metrics_listen_addr"`
	SchedulerAddr          string        `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string        `json:"query_only_scheduler_addr"`
	RequestTimeout         time.Duration `json:"request_timeout"`
	ForwardResponseSize    int64         `json:"forward_response_size"`
	SandboxProxyDomains    []string      `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool `json:"debug_mode"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr         *string         `json:"http_listen_addr"`
		MetricsListenAddr      *string         `json:"metrics_listen_addr"`
		SchedulerAddr          *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string         `json:"query_only_scheduler_addr"`
		RequestTimeout         json.RawMessage `json:"request_timeout"`
		ForwardResponseSize    *int64          `json:"forward_response_size"`
		SandboxProxyDomains    *[]string       `json:"sandbox_proxy_domains"`
		DebugMode              *bool           `json:"debug_mode"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.HTTPListenAddr != nil {
		g.HTTPListenAddr = *parsed.HTTPListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		g.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.SchedulerAddr != nil {
		g.SchedulerAddr = *parsed.SchedulerAddr
	}
	if parsed.QueryOnlySchedulerAddr != nil {
		g.QueryOnlySchedulerAddr = *parsed.QueryOnlySchedulerAddr
	}
	if parsed.ForwardResponseSize != nil {
		g.ForwardResponseSize = *parsed.ForwardResponseSize
	}
	if parsed.SandboxProxyDomains != nil {
		g.SandboxProxyDomains = *parsed.SandboxProxyDomains
	}
	if parsed.DebugMode != nil {
		g.DebugMode = *parsed.DebugMode
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}

	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\": %w", parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\", got numeric value %s", asNumber.String())
	}

	return 0, errors.New("gateway.request_timeout must be a duration string like \"30s\"")
}

type Config struct {
	Service   string          `json:"service"`
	LogLevel  string          `json:"log_level"`
	LogFormat string          `json:"log_format"`
	Scheduler SchedulerConfig `json:"scheduler"`
	Gateway   GatewayConfig   `json:"gateway"`
}

func Load(path string, service string) (Config, error) {
	return load(path, service, false)
}

func LoadScheduler(path string, queryOnly bool) (Config, error) {
	return load(path, "scheduler", queryOnly)
}

func load(path string, service string, schedulerQueryOnly bool) (Config, error) {
	cfg := defaultConfig(service)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("unmarshal config json: %w", err)
		}
	}
	if err := overrideWithEnv(&cfg); err != nil {
		return Config{}, err
	}
	cfg.Service = service
	cfg.applyDefaults()
	if err := cfg.validate(schedulerQueryOnly); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig(service string) Config {
	return Config{
		Service:   service,
		LogLevel:  "info",
		LogFormat: "auto",
		Scheduler: SchedulerConfig{
			GRPCListenAddr:          ":9090",
			MetricsListenAddr:       ":9101",
			Strategy:                "round_robin",
			ReportTTL:               30 * time.Second,
			BindingTTL:              30 * time.Second,
			ArtifactStoreCapacity:   defaultSchedulerArtifactStoreCapacity,
			ArtifactLookupNodeLimit: 0,
			Nodes: []Node{
				{ID: "local-node", Endpoint: "http://127.0.0.1:8000"},
			},
			Discovery: SchedulerDiscoveryConfig{
				Mode: "static",
				Kubernetes: SchedulerDiscoveryKubernetesConfig{
					Scheme: "http",
				},
			},
		},
		Gateway: GatewayConfig{
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      DefaultGatewayRequestTimeout,
			ForwardResponseSize: 4 << 20,
			SandboxProxyDomains: []string{},
		},
	}
}

func overrideWithEnv(cfg *Config) error {
	set := func(key string, target *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*target = v
		}
	}
	set("LOG_LEVEL", &cfg.LogLevel)
	set("LOG_FORMAT", &cfg.LogFormat)
	set("SCHEDULER_GRPC_LISTEN_ADDR", &cfg.Scheduler.GRPCListenAddr)
	set("SCHEDULER_METRICS_LISTEN_ADDR", &cfg.Scheduler.MetricsListenAddr)
	set("SCHEDULER_STRATEGY", &cfg.Scheduler.Strategy)
	set("SCHEDULER_REDIS_ADDR", &cfg.Scheduler.RedisAddr)
	set("GATEWAY_HTTP_LISTEN_ADDR", &cfg.Gateway.HTTPListenAddr)
	set("GATEWAY_METRICS_LISTEN_ADDR", &cfg.Gateway.MetricsListenAddr)
	set("GATEWAY_SCHEDULER_ADDR", &cfg.Gateway.SchedulerAddr)
	set("GATEWAY_QUERY_ONLY_SCHEDULER_ADDR", &cfg.Gateway.QueryOnlySchedulerAddr)

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SANDBOX_PROXY_DOMAINS")); v != "" {
		cfg.Gateway.SandboxProxyDomains = splitCommaSeparated(v)
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_BINDING_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_BINDING_TTL %q: %w", v, err)
		}
		cfg.Scheduler.BindingTTL = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_STORE_CAPACITY")); v != "" {
		capacity, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_STORE_CAPACITY %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactStoreCapacity = capacity
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactLookupNodeLimit = limit
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_REQUEST_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_REQUEST_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.RequestTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_DEBUG_MODE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_DEBUG_MODE %q: %w", v, err)
		}
		cfg.Gateway.DebugMode = b
	}

	return nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Scheduler.MetricsListenAddr) == "" {
		c.Scheduler.MetricsListenAddr = ":9101"
	}
	if c.Scheduler.ReportTTL <= 0 {
		c.Scheduler.ReportTTL = 30 * time.Second
	}
	if c.Scheduler.BindingTTL <= 0 {
		c.Scheduler.BindingTTL = 30 * time.Second
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Mode) == "" {
		c.Scheduler.Discovery.Mode = "static"
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Kubernetes.Scheme) == "" {
		c.Scheduler.Discovery.Kubernetes.Scheme = "http"
	}
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
	}
}

func (c Config) Validate() error {
	return c.validate(false)
}

func (c Config) validate(schedulerQueryOnly bool) error {
	if c.Service == "" {
		return errors.New("service is required")
	}
	if c.LogLevel == "" {
		return errors.New("log_level is required")
	}
	if c.LogFormat == "" {
		return errors.New("log_format is required")
	}
	switch strings.ToLower(c.LogFormat) {
	case "auto", "console", "json":
	default:
		return errors.New("log_format must be one of auto, console, json")
	}
	if c.Service == "scheduler" {
		if c.Scheduler.GRPCListenAddr == "" {
			return errors.New("scheduler.grpc_listen_addr is required")
		}
		if c.Scheduler.MetricsListenAddr == "" {
			return errors.New("scheduler.metrics_listen_addr is required")
		}
		if c.Scheduler.ReportTTL <= 0 {
			return errors.New("scheduler.report_ttl must be greater than zero")
		}
		if c.Scheduler.BindingTTL <= 0 {
			return errors.New("scheduler.binding_ttl must be greater than zero")
		}
		if schedulerQueryOnly {
			if strings.TrimSpace(c.Scheduler.RedisAddr) == "" {
				return errors.New("scheduler --query-only requires scheduler.redis_addr")
			}
			return nil
		}
		if c.Scheduler.ArtifactStoreCapacity <= 0 {
			return errors.New("scheduler.artifact_store_capacity must be greater than zero")
		}
		if err := c.Scheduler.NodeResourceLimit.Validate(); err != nil {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(c.Scheduler.Discovery.Mode)) {
		case "static":
			if len(c.Scheduler.Nodes) == 0 {
				return errors.New("scheduler.nodes must not be empty")
			}
			for _, n := range c.Scheduler.Nodes {
				if n.ID == "" || n.Endpoint == "" {
					return errors.New("scheduler.nodes require id and endpoint")
				}
			}
		case "kubernetes":
			kube := c.Scheduler.Discovery.Kubernetes
			if strings.TrimSpace(kube.Namespace) == "" {
				return errors.New("scheduler.discovery.kubernetes.namespace is required")
			}
			if strings.TrimSpace(kube.ServiceName) == "" {
				return errors.New("scheduler.discovery.kubernetes.service_name is required")
			}
			if kube.Port <= 0 {
				return errors.New("scheduler.discovery.kubernetes.port must be greater than zero")
			}
			if strings.TrimSpace(kube.Scheme) == "" {
				return errors.New("scheduler.discovery.kubernetes.scheme is required")
			}
		default:
			return errors.New("scheduler.discovery.mode must be one of static, kubernetes")
		}
	}
	if c.Service == "gateway" {
		if c.Gateway.HTTPListenAddr == "" {
			return errors.New("gateway.http_listen_addr is required")
		}
		if c.Gateway.MetricsListenAddr == "" {
			return errors.New("gateway.metrics_listen_addr is required")
		}
		if c.Gateway.SchedulerAddr == "" {
			return errors.New("gateway.scheduler_addr is required")
		}
	}
	return nil
}
