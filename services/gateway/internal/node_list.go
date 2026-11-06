package gateway

import (
	"context"
	"net/http"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func nodeStatusToString(status schedulerv1.NodeStatus) string {
	switch status {
	case schedulerv1.NodeStatus_NODE_STATUS_READY:
		return "ready"
	case schedulerv1.NodeStatus_NODE_STATUS_CONNECTING:
		return "connecting"
	case schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:
		return "unhealthy"
	default:
		return "unspecified"
	}
}

type nodeListMachineInfo struct {
	CPUFamily       string `json:"cpuFamily"`
	CPUModel        string `json:"cpuModel"`
	CPUModelName    string `json:"cpuModelName"`
	CPUArchitecture string `json:"cpuArchitecture"`
}

type nodeListDiskMetrics struct {
	MountPoint     string `json:"mountPoint"`
	Device         string `json:"device"`
	FilesystemType string `json:"filesystemType"`
	UsedBytes      uint64 `json:"usedBytes"`
	TotalBytes     uint64 `json:"totalBytes"`
}

type nodeListMetrics struct {
	AllocatedCPU               uint32                `json:"allocatedCPU"`
	AllocatedMemoryBytes       uint64                `json:"allocatedMemoryBytes"`
	CPUPercent                 uint32                `json:"cpuPercent"`
	CPUCount                   uint32                `json:"cpuCount"`
	MemoryUsedBytes            uint64                `json:"memoryUsedBytes"`
	MemoryTotalBytes           uint64                `json:"memoryTotalBytes"`
	Disks                      []nodeListDiskMetrics `json:"disks"`
	PausedAllocatedCPU         uint32                `json:"pausedAllocatedCPU"`
	PausedAllocatedMemoryBytes uint64                `json:"pausedAllocatedMemoryBytes"`
}

type nodeListAdmissionLayer struct {
	MemoryGB uint64 `json:"memoryGB"`
	Admitted bool   `json:"admitted"`
	// Reason is omitted when the layer is admitted.
	Reason string `json:"reason,omitempty"`
}

type nodeListAdmission struct {
	AllocatableMemoryBytes uint64                   `json:"allocatableMemoryBytes"`
	RuntimeOverheadBytes   uint64                   `json:"runtimeOverheadBytes"`
	CapacityMemoryBytes    uint64                   `json:"capacityMemoryBytes"`
	CapacitySource         string                   `json:"capacitySource"`
	ReservedMemoryBytes    uint64                   `json:"reservedMemoryBytes"`
	ReservedCPU            uint32                   `json:"reservedCPU"`
	Layers                 []nodeListAdmissionLayer `json:"layers"`
}

type nodeListItem struct {
	Version              string              `json:"version"`
	Commit               string              `json:"commit"`
	ID                   string              `json:"id"`
	ServiceInstanceID    string              `json:"serviceInstanceID"`
	ClusterID            string              `json:"clusterID"`
	MachineInfo          nodeListMachineInfo `json:"machineInfo"`
	Status               string              `json:"status"`
	SandboxCount         uint32              `json:"sandboxCount"`
	Metrics              nodeListMetrics     `json:"metrics"`
	Admission            *nodeListAdmission  `json:"admission,omitempty"`
	CreateSuccesses      uint64              `json:"createSuccesses"`
	CreateFails          uint64              `json:"createFails"`
	SandboxStartingCount uint32              `json:"sandboxStartingCount"`
	SandboxPausedCount   uint32              `json:"sandboxPausedCount"`
}

// mapNodeAdmission converts the scheduler verdict into the gateway's JSON
// shape, preserving the closed reason set and omitting the reason when the
// layer is admitted.
func mapNodeAdmission(admission *schedulerv1.NodeAdmission) *nodeListAdmission {
	if admission == nil {
		return nil
	}
	layers := make([]nodeListAdmissionLayer, 0, len(admission.GetLayers()))
	for _, layer := range admission.GetLayers() {
		layers = append(layers, nodeListAdmissionLayer{
			MemoryGB: layer.GetMemoryGb(),
			Admitted: layer.GetAdmitted(),
			Reason:   layer.GetReason(),
		})
	}
	return &nodeListAdmission{
		AllocatableMemoryBytes: admission.GetAllocatableMemoryBytes(),
		RuntimeOverheadBytes:   admission.GetRuntimeOverheadBytes(),
		CapacityMemoryBytes:    admission.GetCapacityMemoryBytes(),
		CapacitySource:         admission.GetCapacitySource(),
		ReservedMemoryBytes:    admission.GetReservedMemoryBytes(),
		ReservedCPU:            admission.GetReservedCpu(),
		Layers:                 layers,
	}
}

func isNodeListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	trimmed := strings.TrimRight(strings.TrimSpace(r.URL.Path), "/")
	return trimmed == "/nodes"
}

func nodeIDFromPath(path string) (string, bool) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", false
	}
	trimmed = strings.Trim(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] != "nodes" {
		return "", false
	}
	if strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return parts[1], true
}

func isNodeDetailRequest(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet {
		return "", false
	}
	return nodeIDFromPath(r.URL.Path)
}

func (s *Server) handleNodeList(w http.ResponseWriter, r *http.Request, routingCtx context.Context) {
	rpcStart := time.Now()
	resp, err := s.scheduler.ListObservedNodes(routingCtx, &schedulerv1.ListObservedNodesRequest{
		ClusterId: strings.TrimSpace(r.URL.Query().Get("clusterID")),
	})
	recordGatewaySchedulerRPC("ListObservedNodes", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	out := make([]nodeListItem, 0, len(resp.GetNodes()))
	for _, observed := range resp.GetNodes() {
		snapshot := observed.GetSnapshot()
		disks := make([]nodeListDiskMetrics, 0, len(snapshot.GetDisks()))
		for _, disk := range snapshot.GetDisks() {
			disks = append(disks, nodeListDiskMetrics{
				MountPoint:     disk.GetMountPoint(),
				Device:         disk.GetDevice(),
				FilesystemType: disk.GetFilesystemType(),
				UsedBytes:      disk.GetUsedBytes(),
				TotalBytes:     disk.GetTotalBytes(),
			})
		}

		out = append(out, nodeListItem{
			Version:           observed.GetVersion(),
			Commit:            observed.GetCommit(),
			ID:                observed.GetNodeId(),
			ServiceInstanceID: observed.GetServiceInstanceId(),
			ClusterID:         observed.GetClusterId(),
			MachineInfo: nodeListMachineInfo{
				CPUFamily:       observed.GetMachineInfo().GetCpuFamily(),
				CPUModel:        observed.GetMachineInfo().GetCpuModel(),
				CPUModelName:    observed.GetMachineInfo().GetCpuModelName(),
				CPUArchitecture: observed.GetMachineInfo().GetCpuArchitecture(),
			},
			Status:       nodeStatusToString(snapshot.GetStatus()),
			SandboxCount: snapshot.GetSandboxCount(),
			Admission:    mapNodeAdmission(observed.GetAdmission()),
			Metrics: nodeListMetrics{
				AllocatedCPU:               snapshot.GetAllocatedCpu(),
				AllocatedMemoryBytes:       snapshot.GetAllocatedMemoryBytes(),
				CPUPercent:                 snapshot.GetCpuPercent(),
				CPUCount:                   snapshot.GetCpuCount(),
				MemoryUsedBytes:            snapshot.GetMemoryUsedBytes(),
				MemoryTotalBytes:           snapshot.GetMemoryTotalBytes(),
				Disks:                      disks,
				PausedAllocatedCPU:         snapshot.GetPausedAllocatedCpu(),
				PausedAllocatedMemoryBytes: snapshot.GetPausedAllocatedMemoryBytes(),
			},
			CreateSuccesses:      snapshot.GetCreateSuccesses(),
			CreateFails:          snapshot.GetCreateFails(),
			SandboxStartingCount: snapshot.GetSandboxStartingCount(),
			SandboxPausedCount:   snapshot.GetPausedSandboxCount(),
		})
	}

	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNodeDetail(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	nodeID string,
	longLived bool,
) {
	rpcStart := time.Now()
	resolveResp, err := s.scheduler.GetNode(routingCtx, &schedulerv1.GetNodeRequest{
		NodeId:    nodeID,
		ClusterId: strings.TrimSpace(r.URL.Query().Get("clusterID")),
	})
	recordGatewaySchedulerRPC("GetNode", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	resolved := resolveResp.GetNode()
	if strings.TrimSpace(resolved.GetEndpoint()) == "" {
		http.Error(w, "resolved node endpoint is empty", http.StatusBadGateway)
		return
	}

	upstreamURL, err := joinUpstream(
		resolved.GetEndpoint(),
		r.URL.Path,
		requestEscapedPath(r),
		r.URL.RawQuery,
	)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		&schedulerv1.Node{NodeId: resolved.GetNodeId(), Endpoint: resolved.GetEndpoint()},
		proxyRequestOptions{flushImmediately: longLived},
	)
}
