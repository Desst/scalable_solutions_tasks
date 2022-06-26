package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type Job interface {
	ID() string
	Do() error
}

type jobImpl struct {
	id string
	f  func() error
}

func NewJob(id string, jobFunc func() error) jobImpl {
	return jobImpl{id: id, f: jobFunc}
}

func (j jobImpl) ID() string {
	return j.id
}

func (j jobImpl) Do() error {
	return j.f()
}

type JobResult struct {
	JobID string
	Err   error
}

type WorkerPool interface {
	Start(ctx context.Context)
	AddWorkers(count int)
	RemoveWorkers(count int)

	AddJob(job Job)
	Subscribe() <-chan JobResult
	Stop(ctx context.Context) error
}

type WorkerPoolImpl struct {
	ctx context.Context

	mainWg       *sync.WaitGroup
	shutDownChan chan struct{}
	done         chan struct{}

	wmu     *sync.Mutex
	workers []worker
	wWg     *sync.WaitGroup

	addWorkersEvChan chan int
	rmWorkersEvChan  chan int
	shutdownEvChan   chan context.Context

	jmu     *sync.Mutex
	jobs    []Job
	jobChan chan Job

	jobResChan  chan JobResult
	subscribers []chan<- JobResult
	smu         *sync.Mutex

	stopResultChan chan error
}

type worker interface {
	ID() int
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type workerImpl struct {
	runCtx    context.Context
	runCancel context.CancelFunc

	id         int
	stopChan   chan struct{}
	done       chan struct{}
	jobChan    <-chan Job
	jobResChan chan<- JobResult
}

func newWorker(id int, jobChan <-chan Job, jobResChan chan<- JobResult) *workerImpl {
	return &workerImpl{
		runCtx: context.Background(),

		id:         id,
		jobChan:    jobChan,
		jobResChan: jobResChan,
		stopChan:   make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (w *workerImpl) ID() int {
	return w.id
}

func (w *workerImpl) Stop(ctx context.Context) error {
	defer log.Printf("Worker(%d) STOPPED", w.id)
	close(w.stopChan)
	// either wait for graceful shutdown or for context to timeout
	select {
	case <-ctx.Done():
		log.Printf("FORCING Worker(%d) to STOP", w.id)
		w.runCancel()
		<-w.done
		return fmt.Errorf("worker(%d): %v", w.id, w.runCtx.Err())
	case <-w.done:
		return nil
	}
}

func (w *workerImpl) Run(ctx context.Context) error {
	defer close(w.done)

	w.runCtx, w.runCancel = context.WithCancel(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.stopChan:
			return nil
		case job := <-w.jobChan:
			log.Printf("Worker(%d) received job(%s)", w.id, job.ID())
			err := job.Do()
			log.Printf("Worker(%d) has DONE job(%s)", w.id, job.ID())

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-w.stopChan:
				return nil
			case w.jobResChan <- JobResult{JobID: job.ID(), Err: err}:
			}
		}
	}
}

func NewWorkerPool() *WorkerPoolImpl {
	wp := &WorkerPoolImpl{
		mainWg:       &sync.WaitGroup{},
		shutDownChan: make(chan struct{}),
		done:         make(chan struct{}),

		wWg:     &sync.WaitGroup{},
		wmu:     &sync.Mutex{},
		workers: make([]worker, 0, 5),

		jmu:     &sync.Mutex{},
		jobs:    make([]Job, 0, 10),
		jobChan: make(chan Job),

		addWorkersEvChan: make(chan int),
		rmWorkersEvChan:  make(chan int),
		shutdownEvChan:   make(chan context.Context),

		jobResChan: make(chan JobResult),
		smu:        &sync.Mutex{},

		stopResultChan: make(chan error),
	}

	return wp
}

func (wp *WorkerPoolImpl) Start(ctx context.Context) {
	wp.ctx = ctx
	stopCtx := ctx

	wp.mainWg.Add(1)
	go func() { //event handler go routine
		defer wp.mainWg.Done()
		defer close(wp.shutDownChan)
		for {
			select {
			case <-ctx.Done():
				return
			case stopCtx = <-wp.shutdownEvChan:
				log.Println("BEGIN SHUTDOWN")
				return
			case n := <-wp.addWorkersEvChan:
				wp.addWorkers(n)
			case n := <-wp.rmWorkersEvChan:
				wp.removeWorkers(n)
			case res := <-wp.jobResChan:
				wp.broadcastResult(res)
			}
		}
	}()

	wp.mainWg.Add(1)
	go wp.runSendJobs(ctx)

	go func() {
		wp.mainWg.Wait()
		wp.stopResultChan <- wp.stop(stopCtx)
	}()
}

func (wp *WorkerPoolImpl) AddJob(job Job) {
	wp.jmu.Lock()
	defer wp.jmu.Unlock()
	wp.jobs = append(wp.jobs, job)
	log.Printf("Added job(%s). Pending jobs: %d", job.ID(), len(wp.jobs))
}

func (wp *WorkerPoolImpl) Subscribe() <-chan JobResult {
	wp.smu.Lock()
	defer wp.smu.Unlock()

	jrc := make(chan JobResult)
	wp.subscribers = append(wp.subscribers, jrc)
	return jrc
}

func (wp *WorkerPoolImpl) AddWorkers(count int) {
	select {
	case wp.addWorkersEvChan <- count:
	case <-wp.shutDownChan:
	case <-wp.ctx.Done():
	}

}

func (wp *WorkerPoolImpl) RemoveWorkers(count int) {
	select {
	case wp.rmWorkersEvChan <- count:
	case <-wp.shutDownChan:
	case <-wp.ctx.Done():
	}
}

func (wp *WorkerPoolImpl) Stop(ctx context.Context) error {
	select {
	case wp.shutdownEvChan <- ctx: // init shutdown
		log.Println("SHUTDOWN INITIATED")
	case <-wp.shutDownChan:
		return errors.New("shutdown in progress")
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	}

	return <-wp.stopResultChan
}

func (wp *WorkerPoolImpl) stop(ctx context.Context) error {
	errsChan := make(chan error, len(wp.workers))
	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w worker) {
			defer wg.Done()
			errsChan <- w.Stop(ctx) // will either stop gracefully or after ctx timeout/cancel
		}(w)
	}

	wg.Wait()
	close(errsChan)

	var combinedErr error
	for e := range errsChan {
		if e != nil {
			if combinedErr != nil {
				combinedErr = fmt.Errorf("%w; %v", combinedErr, e)
			} else {
				combinedErr = e
			}
		}
	}

	return combinedErr
}

func (wp *WorkerPoolImpl) addWorkers(count int) {
	if count < 1 {
		return
	}

	defer log.Printf("added %d workers", count)

	wp.wmu.Lock()
	defer wp.wmu.Unlock()

	activeWorkersCount := len(wp.workers)

	workersToAdd := make([]worker, count)
	for i := 0; i < count; i++ {
		workersToAdd[i] = newWorker(activeWorkersCount+i, wp.jobChan, wp.jobResChan)
		wp.wWg.Add(1)
		go workersToAdd[i].Run(wp.ctx)
	}

	wp.workers = append(wp.workers, workersToAdd...)
}

func (wp *WorkerPoolImpl) removeWorkers(count int) {
	if count < 1 || len(wp.workers) == 0 {
		return
	}

	numWorkersToRemove := count
	if len(wp.workers) < count {
		numWorkersToRemove = len(wp.workers)
	}

	defer log.Printf("removed %d workers", numWorkersToRemove)

	workersToRemove := wp.workers[len(wp.workers)-numWorkersToRemove:]
	wp.workers = wp.workers[:len(wp.workers)-numWorkersToRemove]

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(wp.ctx, 3*time.Second) //graceful stop timeout
	defer cancel()

	for _, w := range workersToRemove {
		wg.Add(1)
		go func(w worker) {
			defer wg.Done()
			w.Stop(ctx) // will either stop gracefully or after ctx timeout
		}(w)
	}
	wg.Wait()
}

func (wp *WorkerPoolImpl) runSendJobs(ctx context.Context) {
	defer wp.mainWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.shutDownChan:
			return
		case <-time.After(3 * time.Second):
			func() {
				wp.jmu.Lock()
				defer wp.jmu.Unlock()
				defer func() {
					log.Printf("Pending jobs: %d", len(wp.jobs))
				}()
				log.Printf("Distributing %d jobs", len(wp.jobs))
				for i := 0; i < len(wp.jobs); i++ {
					select {
					case wp.jobChan <- wp.jobs[i]:
						continue
					default: //could not start i`th job. reslice
						wp.jobs = wp.jobs[i:]
						return
					}
				}
				//all jobs are sent for processing. reset slice
				wp.jobs = make([]Job, 0, 10)
			}()
		}
	}
}

func (wp *WorkerPoolImpl) broadcastResult(jobResult JobResult) {
	log.Printf("Broadcasting result for job(%s)", jobResult.JobID)
	wp.smu.Lock()
	defer wp.smu.Unlock()

	for _, sub := range wp.subscribers {
		sub <- jobResult
	}
}
