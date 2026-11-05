package scheduler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func pointer[T any](v T) *T { return &v }

// A fake clientset feeds the real informers: the pod name from the
// EndpointSlice maps to the Kubernetes node, whose status.allocatable.memory
// becomes the capacity.
func TestKubernetesDiscoveryCapacityFromFakeClientset(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "k8s-node-1"},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("119336Mi"),
			},
		},
	}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "agentenv-nodes-1", Namespace: "agentenv-system"},
		Endpoints: []discoveryv1.Endpoint{{
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "pod-a"},
			NodeName:  pointer("k8s-node-1"),
		}},
	}
	clientset := fake.NewSimpleClientset(node, slice)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	nodeInformer := factory.Core().V1().Nodes().Informer()
	sliceInformer := factory.Discovery().V1().EndpointSlices().Informer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced, sliceInformer.HasSynced) {
		t.Fatal("informer cache did not sync")
	}

	discovery := &KubernetesDiscovery{
		endpointSliceInformer: sliceInformer,
		nodeInformer:          nodeInformer,
		capacity:              newNodeCapacityIndex(),
	}
	discovery.syncCapacity(sliceInformer.GetStore().List())

	capacity, ok := discovery.Capacity("pod-a")
	if !ok {
		t.Fatal("expected capacity for pod-a")
	}
	if capacity.AllocatableMemoryBytes != 119336<<20 {
		t.Fatalf("allocatable = %d, want 119336 MiB", capacity.AllocatableMemoryBytes)
	}
	if capacity.Source != "kubernetes" {
		t.Fatalf("source = %q", capacity.Source)
	}
	if _, ok := discovery.Capacity("unknown-pod"); ok {
		t.Fatal("an unmapped pod must not report a capacity")
	}
}

func TestPodNodeNamesAndAllocatable(t *testing.T) {
	slice := &discoveryv1.EndpointSlice{Endpoints: []discoveryv1.Endpoint{
		{TargetRef: &corev1.ObjectReference{Name: "pod-a"}, NodeName: pointer("node-1")},
		{TargetRef: &corev1.ObjectReference{Name: "pod-b"}}, // no nodeName
		{NodeName: pointer("node-2")},                       // no pod
	}}
	names := podNodeNames([]*discoveryv1.EndpointSlice{slice, nil})
	if names["pod-a"] != "node-1" || len(names) != 1 {
		t.Fatalf("unexpected pod-to-node map: %v", names)
	}

	nodes := allocatableMemoryFromNodes([]*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("64Gi"),
			}},
		},
		nil,
		{ObjectMeta: metav1.ObjectMeta{Name: "empty"}},
	})
	if nodes["node-1"] != 64*gib {
		t.Fatalf("node-1 allocatable = %d, want 64 GiB", nodes["node-1"])
	}
	if _, ok := nodes["empty"]; ok {
		t.Fatal("a node without allocatable memory must be omitted")
	}
}

func TestNodeCapacitySourceIsLateBound(t *testing.T) {
	source := NewNodeCapacitySource()
	if _, ok := source.Capacity("pod-a"); ok {
		t.Fatal("an unset source must report no capacity")
	}
	source.Set(fixedCapacityProvider{
		"pod-a": {AllocatableMemoryBytes: 32 * gib, Source: "kubernetes"},
	})
	capacity, ok := source.Capacity("pod-a")
	if !ok || capacity.AllocatableMemoryBytes != 32*gib {
		t.Fatalf("late-bound capacity = %+v ok=%v", capacity, ok)
	}
}

func TestNodeCapacityIndexIgnoresExpiredMappingAfterReplace(t *testing.T) {
	index := newNodeCapacityIndex()
	index.replace(map[string]string{"pod-a": "node-1"}, map[string]uint64{"node-1": 64 * gib})
	if _, ok := index.Capacity("pod-a"); !ok {
		t.Fatal("expected the initial mapping")
	}
	index.replace(map[string]string{}, map[string]uint64{"node-1": 64 * gib})
	if _, ok := index.Capacity("pod-a"); ok {
		t.Fatal("a replaced index must drop the stale pod mapping")
	}
}
