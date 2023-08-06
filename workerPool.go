package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type JobRequest[T any] struct {
	Ctx     context.Context
	Request T
}

type InOrderJobRequest[T any] struct {
	JobRequest JobRequest[T]
	Index      int32
}

func NewJobRequest[T any](request T) *JobRequest[T] {
	return &JobRequest[T]{Request: request}
}

func NewJobRequestWithContext[T any](ctx context.Context, request T) *JobRequest[T] {
	return &JobRequest[T]{Ctx: ctx, Request: request}
}

type JobResponse[R any] struct {
	Response R
	Error    error
}

type JobFunc[JobRequest, JobResponse any] func(JobRequest) JobResponse

type WorkerPool[JobRequest, JobResponse any] interface {
	NumberOfWorkers() int32
	AddWorkers(int32)
	RemoveWorkers(int32)
	SetNumberOfWorkers(int32)
	WorkerScheduler(context.Context)
	SetJobFunc(JobFunc[JobRequest, JobResponse]) WorkerPool[JobRequest, JobResponse]
	AddJobs(jobRequests ...JobRequest) WorkerPool[JobRequest, JobResponse]
	RunInBackground()
}

type workerPool[JobRequest, JobResponse any] struct {
	wg                 *sync.WaitGroup
	numberOfWorkers    int32
	jobRequests        []JobRequest
	jobFunc            JobFunc[JobRequest, JobResponse]
	jobQueue           chan JobRequest
	backgroundJobQueue chan JobRequest
	inOrderJobQueue    chan InOrderJobRequest[JobRequest]
	jobResponseChan    chan JobResponse
	addWorkerChan      chan struct{}
	removeWorkerChan   chan struct{}
	cond               *sync.Cond
	counter            int
}

func New[JobRequest, JobResponse any](workerCtx context.Context, numberOfWorkers int32) WorkerPool[JobRequest, JobResponse] {
	if numberOfWorkers <= 0 {
		numberOfWorkers = 1
	}
	wp := &workerPool[JobRequest, JobResponse]{
		numberOfWorkers:    0,
		jobQueue:           make(chan JobRequest, 3*numberOfWorkers),
		backgroundJobQueue: make(chan JobRequest, 3*numberOfWorkers),
		jobResponseChan:    make(chan JobResponse, numberOfWorkers),
		addWorkerChan:      make(chan struct{}),
		removeWorkerChan:   make(chan struct{}),
		wg:                 new(sync.WaitGroup),
		cond:               sync.NewCond(new(sync.Mutex)),
		counter:            0,
	}
	// launch workers
	go wp.WorkerScheduler(workerCtx)
	// add workers
	wp.AddWorkers(numberOfWorkers)

	return wp
}

func (wp *workerPool[JobRequest, JobResponse]) NumberOfWorkers() int32 {
	return wp.numberOfWorkers
}

func (wp *workerPool[JobRequest, JobResponse]) WorkerScheduler(workerCtx context.Context) {
	for {
		select {
		case <-wp.addWorkerChan:
			go wp.worker(workerCtx)
		}
	}
}

func (wp *workerPool[JobRequest, JobResponse]) SetNumberOfWorkers(count int32) {
	if wp.numberOfWorkers == count {
		return
	}
	if count < wp.numberOfWorkers {
		wp.RemoveWorkers(wp.numberOfWorkers - count)

		return
	}
	wp.AddWorkers(count - wp.numberOfWorkers)
}

func (wp *workerPool[JobRequest, JobResponse]) AddWorkers(count int32) {
	if count <= 0 {
		return
	}
	for i := int32(0); i < count; i++ {
		wp.addWorkerChan <- struct{}{}
	}
	wp.numberOfWorkers += count
}

func (wp *workerPool[JobRequest, JobResponse]) RemoveWorkers(count int32) {
	if wp.jobFunc == nil {
		panic(errors.New("JobFunc is nil"))
	}
	if count <= 0 {
		return
	}
	if count >= wp.numberOfWorkers {
		count = wp.numberOfWorkers - 1
	}
	for i := int32(0); i < count; i++ {
		wp.removeWorkerChan <- struct{}{}
	}
	wp.numberOfWorkers -= count
}

func (wp *workerPool[JobRequest, JobResponse]) SetJobFunc(jobFunc JobFunc[JobRequest, JobResponse]) WorkerPool[JobRequest, JobResponse] {
	wp.jobFunc = jobFunc

	return wp
}

func (wp *workerPool[JobRequest, JobResponse]) AddJobs(jobRequests ...JobRequest) WorkerPool[JobRequest, JobResponse] {
	wp.jobRequests = append(wp.jobRequests, jobRequests...)

	return wp
}

func (wp *workerPool[JobRequest, JobResponse]) RunInBackground() {
	go func() {
		for _, jobRequest := range wp.jobRequests {
			fmt.Println("scheduling job:", jobRequest)
			wp.wg.Add(1)
			wp.backgroundJobQueue <- jobRequest
		}
		wp.wg.Wait()
		fmt.Println("Background Jobs Done!!!")
	}()
}

func (wp *workerPool[JobRequest, JobResponse]) RunInOrder() WorkerPool[JobRequest, JobResponse] {
	wp.jobResponseChan = make(chan JobResponse, len(wp.jobQueue))
	go func() {
		for _, jobRequest := range wp.jobRequests {
			fmt.Println("scheduling job:", jobRequest)
			wp.wg.Add(1)
			wp.jobQueue <- jobRequest
		}
		wp.wg.Wait()
		fmt.Println("Jobs Done!!!")
		close(wp.jobResponseChan)
	}()

	return wp
}

func (wp *workerPool[JobRequest, JobResponse]) ForEach(f func(JobResponse)) {
	for v := range wp.jobResponseChan {
		f(v)
	}
}

func (wp *workerPool[T, R]) worker(workerCtx context.Context) {
	defer fmt.Println("exiting worker")
	fmt.Println("launched worker")
	for {
		select {
		case bgJobRequest := <-wp.backgroundJobQueue:
			func() {
				defer wp.wg.Done()
				defer func() {
					if err := recover(); err != nil {
						log.Println(err)
					}
				}()
				wp.jobFunc(bgJobRequest)
			}()
		case jobRequest := <-wp.jobQueue:
			func() {
				defer wp.wg.Done()
				defer func() {
					if err := recover(); err != nil {
						log.Println(err)
					}
				}()
				fmt.Println("Doing Job::", jobRequest)
				jobResponse := wp.jobFunc(jobRequest)
				wp.jobResponseChan <- jobResponse
				fmt.Println("req:", jobRequest, " res:", jobResponse)
			}()
		case <-workerCtx.Done():
			fmt.Println("Worker Context cancelled")
			return
		case <-wp.removeWorkerChan:
			return
		}
	}
}
