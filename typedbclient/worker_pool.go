package typedbclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Task represents a query execution task
type Task struct {
	Database string // Database name
	TxType   TransactionType
	Query    string
	ResultCh chan TaskResult // Unified result channel for thread-safe communication
	Ctx      context.Context
}

// TaskResult contains the result of a task execution
type TaskResult struct {
	Result *QueryResult
	Error  error
}

// WorkerPool manages a pool of workers with limited concurrency
type WorkerPool struct {
	client       *Client
	taskQueue    chan *Task
	workerCount  int
	shutdownOnce sync.Once
	shutdown     chan struct{}
	wg           sync.WaitGroup
	activeCount  atomic.Int32 // Track active workers for monitoring
}

// NewWorkerPool creates a worker pool with specified concurrency
// queueSize: buffer size for pending tasks (recommended: 100000+)
// workerCount: max concurrent workers (recommended: 10-50, must be < gRPC stream limit)
func NewWorkerPool(client *Client, queueSize int, workerCount int) *WorkerPool {
	pool := &WorkerPool{
		client:      client,
		taskQueue:   make(chan *Task, queueSize),
		workerCount: workerCount,
		shutdown:    make(chan struct{}),
	}

	// Start workers
	for i := 0; i < workerCount; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool
}

// Submit submits a task to the pool (thread-safe, blocks if queue is full)
func (p *WorkerPool) Submit(ctx context.Context, database string, txType TransactionType, query string) (*QueryResult, error) {
	// Create task with buffered result channel (size=1 to prevent goroutine leak)
	task := &Task{
		Database: database,
		TxType:   txType,
		Query:    query,
		ResultCh: make(chan TaskResult, 1), // Buffer size 1 ensures worker never blocks
		Ctx:      ctx,
	}

	// Submit task to queue (thread-safe via channel)
	select {
	case p.taskQueue <- task:
		// Successfully queued
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.shutdown:
		return nil, fmt.Errorf("worker pool is shutdown")
	}

	// Wait for result (thread-safe via channel receive)
	select {
	case result := <-task.ResultCh:
		return result.Result, result.Error
	case <-ctx.Done():
		// Context cancelled, but task may still be running in worker
		// The buffered channel ensures worker can still send result without blocking
		return nil, ctx.Err()
	case <-p.shutdown:
		return nil, fmt.Errorf("worker pool is shutdown")
	}
}

// worker executes tasks from the queue
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case task := <-p.taskQueue:
			if task == nil {
				return // Channel closed
			}

			p.activeCount.Add(1)
			p.executeTask(id, task)
			p.activeCount.Add(-1)

		case <-p.shutdown:
			return
		}
	}
}

// executeTask executes a single task (thread-safe)
func (p *WorkerPool) executeTask(workerID int, task *Task) {
	// Get database handle
	db := p.client.GetDatabase(task.Database)

	var result *QueryResult
	var err error

	// Execute query based on transaction type
	switch task.TxType {
	case Read:
		result, err = db.ExecuteRead(task.Ctx, task.Query)
	case Write:
		result, err = db.ExecuteWrite(task.Ctx, task.Query)
	case Schema:
		result, err = db.ExecuteSchema(task.Ctx, task.Query)
	default:
		err = fmt.Errorf("unknown transaction type: %v", task.TxType)
	}

	// Send result back (thread-safe via buffered channel)
	// The channel has buffer size 1, so this send will never block
	// Even if the receiver has already cancelled (via context), the goroutine won't leak
	task.ResultCh <- TaskResult{
		Result: result,
		Error:  err,
	}
}

// Shutdown gracefully shuts down the worker pool
func (p *WorkerPool) Shutdown() {
	p.shutdownOnce.Do(func() {
		close(p.shutdown)
		close(p.taskQueue)
		p.wg.Wait()
	})
}

// GetStats returns pool statistics
func (p *WorkerPool) GetStats() (queueLen int, activeWorkers int32) {
	return len(p.taskQueue), p.activeCount.Load()
}
