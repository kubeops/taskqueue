/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"sync"

	"k8s.io/client-go/util/workqueue"
)

type SharedQueuePool struct {
	MapOfQueues map[string]*workqueue.Typed[string]
	locks       sync.Map
	globalMu    sync.RWMutex
}

func NewSharedQueuePool() *SharedQueuePool {
	return &SharedQueuePool{
		MapOfQueues: make(map[string]*workqueue.Typed[string]),
	}
}

func (pool *SharedQueuePool) WithMutexLock(fn func()) {
	pool.globalMu.Lock()
	defer pool.globalMu.Unlock()
	fn()
}

func (pool *SharedQueuePool) getLock(queueName string) *sync.RWMutex {
	lockIface, _ := pool.locks.LoadOrStore(queueName, &sync.RWMutex{})
	return lockIface.(*sync.RWMutex)
}

func (pool *SharedQueuePool) Add(queueName string) {
	pool.globalMu.Lock()
	defer pool.globalMu.Unlock()

	if _, exists := pool.MapOfQueues[queueName]; !exists {
		pool.MapOfQueues[queueName] = workqueue.NewTyped[string]()
	}
}

func (pool *SharedQueuePool) Remove(queueName string) {
	pool.globalMu.Lock()
	defer pool.globalMu.Unlock()

	if q, exists := pool.MapOfQueues[queueName]; exists {
		q.ShutDown()
		delete(pool.MapOfQueues, queueName)
		pool.locks.Delete(queueName)
	}
}

func (pool *SharedQueuePool) Enqueue(queueName string, data string) {
	lock := pool.getLock(queueName)
	lock.RLock()
	defer lock.RUnlock()

	pool.globalMu.RLock()
	q, exists := pool.MapOfQueues[queueName]
	pool.globalMu.RUnlock()

	if exists {
		q.Add(data)
	}
}

func (pool *SharedQueuePool) Dequeue(queueName string) (string, bool) {
	lock := pool.getLock(queueName)
	lock.RLock()
	defer lock.RUnlock()

	pool.globalMu.RLock()
	q, exists := pool.MapOfQueues[queueName]
	pool.globalMu.RUnlock()

	if exists {
		item, shutdown := q.Get()
		q.Done(item)
		return item, shutdown
	}
	return "", false
}

func (pool *SharedQueuePool) Exists(queueName string) bool {
	pool.globalMu.RLock()
	defer pool.globalMu.RUnlock()
	_, exists := pool.MapOfQueues[queueName]
	return exists
}

func (pool *SharedQueuePool) Len() int {
	pool.globalMu.RLock()
	defer pool.globalMu.RUnlock()
	return len(pool.MapOfQueues)
}

func (pool *SharedQueuePool) QueueLength(queueName string) int {
	lock := pool.getLock(queueName)
	lock.RLock()
	defer lock.RUnlock()

	pool.globalMu.RLock()
	defer pool.globalMu.RUnlock()

	if q, exists := pool.MapOfQueues[queueName]; exists {
		return q.Len()
	}
	return 0
}

func (pool *SharedQueuePool) ListQueues() []string {
	pool.globalMu.RLock()
	defer pool.globalMu.RUnlock()

	keys := make([]string, 0, len(pool.MapOfQueues))
	for name := range pool.MapOfQueues {
		keys = append(keys, name)
	}
	return keys
}
