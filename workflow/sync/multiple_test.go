package sync

import (
	"fmt"
	"testing"
	"time"

	"github.com/argoproj/argo-workflows/v4/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

var wfTmpl = `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: %s
  namespace: default
spec:
  entrypoint: whalesay
  synchronization:
%s
  templates:
  - name: whalesay
    container:
      image: docker/whalesay:latest
      command: [cowsay]
      args: ["hello world"]
`

func templatedWorkflow(name string, syncBlock string) *wfv1.Workflow {
	return wfv1.MustUnmarshalWorkflow(fmt.Sprintf(wfTmpl, name, syncBlock))
}

func TestMultipleMutexLock(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	kube := fake.NewClientset()
	syncLimitFunc := GetSyncLimitFunc(kube)
	t.Run("MultipleMutex", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wfall := templatedWorkflow("all",
			`    mutexes:
      - name: one
      - name: two
      - name: three
`)
		wf1 := templatedWorkflow("one",
			`    mutexes:
      - name: one
`)
		wf2 := templatedWorkflow("two",
			`    mutexes:
      - name: two
`)
		wf3 := templatedWorkflow("three",
			`    mutexes:
      - name: three
`)
		// Acquire 1
		status, wfUpdate, msg, failedLockName, err := syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Acquire 2
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf2, "", wf2.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Acquire 3
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf3, "", wf3.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Fail to acquire because one locked
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/one", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		syncManager.ReleaseAll(ctx, wf1)
		// Fail to acquire because two locked
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		syncManager.ReleaseAll(ctx, wf2)
		// Fail to acquire because three locked
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/three", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		syncManager.ReleaseAll(ctx, wf3)
		// Now lock
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)
	})
	t.Run("MultipleMutexOrdering", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wfall := templatedWorkflow("all",
			`    mutexes:
      - name: one
      - name: two
      - name: three
`)
		wf1 := templatedWorkflow("one",
			`    mutexes:
      - name: one
`)
		wf2 := templatedWorkflow("two",
			`    mutexes:
      - name: two
`)
		// Acquire 1
		status, wfUpdate, msg, failedLockName, err := syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Fail to acquire because one locked
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/one", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Attempt 2, but blocked by all
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf2, "", wf2.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		// Fail to acquire because one locked
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/one", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		syncManager.ReleaseAll(ctx, wf1)
		syncManager.ReleaseAll(ctx, wf2)

		// Now lock
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfall, "", wfall.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)
	})
}

const multipleConfigMap = `
apiVersion: v1
kind: ConfigMap
metadata:
 name: my-config
data:
 double: "2"
`

func TestMutexAndSemaphore(t *testing.T) {
	kube := fake.NewClientset()
	var cm v1.ConfigMap
	wfv1.MustUnmarshal([]byte(multipleConfigMap), &cm)

	ctx := logging.TestContext(t.Context())
	_, err := kube.CoreV1().ConfigMaps("default").Create(ctx, &cm, metav1.CreateOptions{})
	require.NoError(t, err)

	syncLimitFunc := GetSyncLimitFunc(kube)
	t.Run("MutexSemaphore", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wfmands1 := templatedWorkflow("mands1",
			`    mutexes:
       - name: one
    semaphores:
       - configMapKeyRef:
           key: double
           name: my-config
`)
		wfmands1copy := wfmands1.DeepCopy()
		wfmands2 := templatedWorkflow("mands2",
			`    mutexes:
       - name: two
    semaphores:
       - configMapKeyRef:
           key: double
           name: my-config
`)
		wf1 := templatedWorkflow("one",
			`    mutexes:
       - name: one
`)
		wf2 := templatedWorkflow("two",
			`    mutexes:
       - name: two
`)
		wfsem := templatedWorkflow("three",
			`    semaphores:
       - configMapKeyRef:
           key: double
           name: my-config
`)
		// Acquire sem + 1
		status, wfUpdate, msg, failedLockName, err := syncManager.TryAcquire(ctx, wfmands1, "", wfmands1.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Acquire sem + 2
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfmands2, "", wfmands2.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Fail 1
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/one", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Fail 2
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf2, "", wf2.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Fail sem
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfsem, "", wfsem.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/ConfigMap/my-config/double", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Release 1 and sem
		syncManager.ReleaseAll(ctx, wfmands1)

		// Succeed 1
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Fail 2
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf2, "", wf2.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		// Succeed sem
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfsem, "", wfsem.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		syncManager.ReleaseAll(ctx, wf1)
		syncManager.ReleaseAll(ctx, wfsem)

		// And reacquire in a sem+mutex wf
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfmands1copy, "", wfmands1copy.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)
	})
}
func TestPriority(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	kube := fake.NewClientset()
	syncLimitFunc := GetSyncLimitFunc(kube)
	t.Run("Priority", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wflow := templatedWorkflow("prioritylow",
			`    mutexes:
       - name: one
       - name: two
`)
		wfhigh := wflow.DeepCopy()
		wfhigh.Name = "priorityhigh"
		wfhigh.Spec.Priority = new(int32(5))
		wf1 := templatedWorkflow("one",
			`    mutexes:
       - name: two
`)
		// wf2 takes mutex two
		wf2 := templatedWorkflow("two",
			`    mutexes:
       - name: two
`)
		// Acquire 1 + 2 as low
		status, wfUpdate, msg, failedLockName, err := syncManager.TryAcquire(ctx, wflow, "", wflow.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)

		// Attempt to acquire 2, fail
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Attempt get 1 + 2 as high but fail
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfhigh, "", wfhigh.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/one", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Attempt to acquire 2 again as two, fail
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf2, "", wf2.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.True(t, wfUpdate)

		// Release locks
		syncManager.ReleaseAll(ctx, wflow)

		// Attempt to acquire 2 again, but priority blocks
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wf1, "", wf1.Spec.Synchronization)
		require.NoError(t, err)
		assert.NotEmpty(t, msg)
		assert.Equal(t, "default/Mutex/two", failedLockName)
		assert.False(t, status)
		assert.False(t, wfUpdate)

		// Attempt get 1 + 2 as high and priority succeeds
		status, wfUpdate, msg, failedLockName, err = syncManager.TryAcquire(ctx, wfhigh, "", wfhigh.Spec.Synchronization)
		require.NoError(t, err)
		assert.Empty(t, msg)
		assert.Empty(t, failedLockName)
		assert.True(t, status)
		assert.True(t, wfUpdate)
	})
}

func TestDuplicates(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	kube := fake.NewClientset()
	syncLimitFunc := GetSyncLimitFunc(kube)
	t.Run("Mutex", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wfdupmutex := templatedWorkflow("mutex",
			`    mutexes:
       - name: one
       - name: one
`)
		_, _, _, _, err = syncManager.TryAcquire(ctx, wfdupmutex, "", wfdupmutex.Spec.Synchronization)
		assert.Error(t, err)
	})
	t.Run("Semaphore", func(t *testing.T) {
		syncManager, err := NewLockManager(ctx, kube, "", nil, syncLimitFunc, func(key string) {},
			WorkflowExistenceFunc, false)
		require.NoError(t, err)

		wfdupsemaphore := templatedWorkflow("semaphore",
			`    semaphores:
       - configMapKeyRef:
           key: double
           name: my-config
       - configMapKeyRef:
           key: double
           name: my-config
`)
		_, _, _, _, err = syncManager.TryAcquire(ctx, wfdupsemaphore, "", wfdupsemaphore.Spec.Synchronization)
		assert.Error(t, err)
	})
}

// TestReleaseAllRemovesWorkflowFromEveryQueue covers a workflow that requests several
// locks, is queued on all of them while one is busy, and finishes without ever acquiring
// them (for example a node that errors on its second reconcile). ReleaseAll must drop it
// from every queue, not only from the lock recorded in the status, and wake up the next
// waiter behind it: an entry left at the front of a queue blocks that lock until the
// controller restarts.
func TestReleaseAllRemovesWorkflowFromEveryQueue(t *testing.T) {
	semaphore := func() *wfv1.SemaphoreRef {
		return &wfv1.SemaphoreRef{ConfigMapKeyRef: &v1.ConfigMapKeySelector{
			LocalObjectReference: v1.LocalObjectReference{Name: "my-config"},
			Key:                  "template",
		}}
	}
	mutex := func(name string) *wfv1.Mutex { return &wfv1.Mutex{Name: name} }
	bareWorkflow := func(name string, created time.Time) *wfv1.Workflow {
		return &wfv1.Workflow{ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(created),
		}}
	}

	tests := []struct {
		name       string
		nodeName   string
		holderSync *wfv1.Synchronization
		leakerSync *wfv1.Synchronization
		victimSync *wfv1.Synchronization
		busyLock   string
		freeLock   string
		dehydrate  bool
	}{
		{
			name:       "TemplateSemaphoreAndMutex",
			nodeName:   "node-1",
			holderSync: &wfv1.Synchronization{Semaphores: []*wfv1.SemaphoreRef{semaphore()}, Mutexes: []*wfv1.Mutex{mutex("a")}},
			leakerSync: &wfv1.Synchronization{Semaphores: []*wfv1.SemaphoreRef{semaphore()}, Mutexes: []*wfv1.Mutex{mutex("b")}},
			victimSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("b")}},
			busyLock:   "default/ConfigMap/my-config/template",
			freeLock:   "default/Mutex/b",
		},
		{
			name:       "TemplateMutexAndMutex",
			nodeName:   "node-1",
			holderSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("x"), mutex("a")}},
			leakerSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("x"), mutex("b")}},
			victimSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("b")}},
			busyLock:   "default/Mutex/x",
			freeLock:   "default/Mutex/b",
		},
		{
			// A mutex without a holder is never recorded in Status.Synchronization.Mutex.Waiting.
			name:       "WorkflowLevelMutexAndMutex",
			holderSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("x"), mutex("a")}},
			leakerSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("x"), mutex("b")}},
			victimSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("b")}},
			busyLock:   "default/Mutex/x",
			freeLock:   "default/Mutex/b",
		},
		{
			// Node statuses are offloaded before ReleaseAll runs when offloading is enabled.
			name:       "TemplateDehydratedNodes",
			nodeName:   "node-1",
			holderSync: &wfv1.Synchronization{Semaphores: []*wfv1.SemaphoreRef{semaphore()}, Mutexes: []*wfv1.Mutex{mutex("a")}},
			leakerSync: &wfv1.Synchronization{Semaphores: []*wfv1.SemaphoreRef{semaphore()}, Mutexes: []*wfv1.Mutex{mutex("b")}},
			victimSync: &wfv1.Synchronization{Mutexes: []*wfv1.Mutex{mutex("b")}},
			busyLock:   "default/ConfigMap/my-config/template",
			freeLock:   "default/Mutex/b",
			dehydrate:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := logging.TestContext(t.Context())
			kube := fake.NewClientset()
			var cm v1.ConfigMap
			wfv1.MustUnmarshal(configMap, &cm)
			_, err := kube.CoreV1().ConfigMaps("default").Create(ctx, &cm, metav1.CreateOptions{})
			require.NoError(t, err)
			notified := map[string]bool{}
			syncManager, err := NewLockManager(ctx, kube, "", nil, GetSyncLimitFunc(kube), func(key string) {
				notified[key] = true
			}, WorkflowExistenceFunc, false)
			require.NoError(t, err)

			now := time.Now()
			holder := bareWorkflow("holder", now)
			leaker := bareWorkflow("leaker", now.Add(time.Second))
			victim := bareWorkflow("victim", now.Add(2*time.Second))

			status, _, _, _, err := syncManager.TryAcquire(ctx, holder, tt.nodeName, tt.holderSync)
			require.NoError(t, err)
			require.True(t, status, "holder should acquire its locks")

			// The leaker is queued on both locks but only records the busy one.
			status, _, msg, failedLockName, err := syncManager.TryAcquire(ctx, leaker, tt.nodeName, tt.leakerSync)
			require.NoError(t, err)
			require.False(t, status, "leaker should wait for the busy lock")
			require.Equal(t, tt.busyLock, failedLockName)
			assert.Contains(t, msg, "Waiting for")
			leakerKey := getHolderKey(leaker, tt.nodeName)
			if tt.nodeName != "" {
				// This is what markNodeWaitingForLock records in the controller.
				leaker.Status.Nodes = wfv1.Nodes{tt.nodeName: wfv1.NodeStatus{
					ID:                    tt.nodeName,
					Name:                  tt.nodeName,
					Phase:                 wfv1.NodeError,
					SynchronizationStatus: &wfv1.NodeSynchronizationStatus{Waiting: failedLockName},
				}}
			}
			for _, lockName := range []string{tt.busyLock, tt.freeLock} {
				var pending []string
				pending, err = syncManager.syncLockMap[lockName].getCurrentPending(ctx)
				require.NoError(t, err)
				assert.Contains(t, pending, leakerKey, "leaker should be queued on %s", lockName)
			}

			// The victim only needs the free lock but is stuck behind the leaker's entry.
			status, _, msg, _, err = syncManager.TryAcquire(ctx, victim, tt.nodeName, tt.victimSync)
			require.NoError(t, err)
			require.False(t, status, "victim should be queued behind the leaker")
			assert.Contains(t, msg, "Waiting for")

			if tt.dehydrate {
				leaker.Status.Nodes = nil
			}
			clear(notified)
			syncManager.ReleaseAll(ctx, leaker)

			for _, lockName := range []string{tt.busyLock, tt.freeLock} {
				var pending []string
				pending, err = syncManager.syncLockMap[lockName].getCurrentPending(ctx)
				require.NoError(t, err)
				assert.NotContains(t, pending, leakerKey, "leaker should be removed from %s", lockName)
			}
			holders, err := syncManager.syncLockMap[tt.busyLock].getCurrentHolders(ctx)
			require.NoError(t, err)
			assert.Equal(t, []string{getHolderKey(holder, tt.nodeName)}, holders, "holder should keep the busy lock")
			assert.True(t, notified["default/victim"], "victim should be woken up, notified: %v", notified)

			status, _, _, _, err = syncManager.TryAcquire(ctx, victim, tt.nodeName, tt.victimSync)
			require.NoError(t, err)
			assert.True(t, status, "victim should acquire the lock once the leaker is gone")
		})
	}
}
