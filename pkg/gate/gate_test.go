package gate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestGate_ShouldGaranteeFairScheduling(t *testing.T) {
	const (
		maxConcurrency   = 1000
		newWorkersPerSec = 1000
		totalWorkers     = 10000
	)

	g := NewGate(maxConcurrency, nil)
	ctx := context.Background()
	wg := sync.WaitGroup{}
	wg.Add(totalWorkers)

	maxWaitTimeMx := sync.Mutex{}
	maxWaitTime := time.Duration(0)

	for i := 1; i <= totalWorkers; i++ {
		// Start the initial set workers immediately, then start following workers over the time.
		if i > newWorkersPerSec*2 {
			time.Sleep(time.Second / newWorkersPerSec)
		}

		go func(workerID int) {
			defer g.Done()
			defer wg.Done()

			startTime := time.Now()
			testutil.Ok(t, g.IsMyTurn(ctx))
			waitTime := time.Since(startTime)

			maxWaitTimeMx.Lock()
			if waitTime > maxWaitTime {
				maxWaitTime = waitTime
			}
			maxWaitTimeMx.Unlock()

			time.Sleep(time.Second)
		}(i)
	}

	wg.Wait()

	fmt.Println(maxWaitTime)
}
