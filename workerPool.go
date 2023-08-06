package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type Data interface {
	any
}

type JobRequest[T Data] struct {
	Ctx     context.Context
	Request T
}

type InOrderJobRequest[T Data] struct {
	JobRequest JobRequest[T]
	Index      int32
}

func NewJobRequest[T Data](request T) *JobRequest[T] {
	return &JobRequest[T]{Request: request}
}

func NewJobRequestWithContext[T Data](ctx context.Context, request T) *JobRequest[T] {
	return &JobRequest[T]{Ctx: ctx, Request: request}
}

type JobResponse[R Data] struct {
	Response R
	Error    error
}

type JobFunc[T Data, R Data] func(JobRequest[T]) JobResponse[R]

type WorkerPool[T Data, R Data] interface {
	NumberOfWorkers() int32
	AddWorkers(int32)
	RemoveWorkers(int32)
	SetNumberOfWorkers(int32)
	WorkerScheduler(context.Context)
	SetJobFunc(JobFunc[T, R]) WorkerPool[T, R]
	AddJobs(...JobRequest[T]) WorkerPool[T, R]
	RunInBackground()
	RunInOrder() WorkerPool[T, R]
	ForEach(f func(JobResponse[R]))
}

type workerPool[T Data, R Data] struct {
	wg                 *sync.WaitGroup
	numberOfWorkers    int32
	jobRequests        []JobRequest[T]
	jobFunc            JobFunc[T, R]
	jobQueue           chan JobRequest[T]
	backgroundJobQueue chan JobRequest[T]
	inOrderJobQueue    chan InOrderJobRequest[T]
	jobResponseChan    chan JobResponse[R]
	addWorkerChan      chan struct{}
	removeWorkerChan   chan struct{}
	cond               *sync.Cond
	counter            int32
}

func New[T Data, R Data](workerCtx context.Context,
	numberOfWorkers int32) WorkerPool[T, R] {
	if numberOfWorkers <= 0 {
		numberOfWorkers = 1
	}
	wp := &workerPool[T, R]{
		numberOfWorkers:    0,
		jobQueue:           make(chan JobRequest[T], 3*numberOfWorkers),
		backgroundJobQueue: make(chan JobRequest[T], 3*numberOfWorkers),
		inOrderJobQueue:    make(chan InOrderJobRequest[T], 3*numberOfWorkers),
		// jobResponseChan:    make(chan JobResponse[R], numberOfWorkers),
		addWorkerChan:    make(chan struct{}),
		removeWorkerChan: make(chan struct{}),
		wg:               new(sync.WaitGroup),
		cond:             sync.NewCond(new(sync.Mutex)),
		counter:          0,
	}
	// launch workers
	go wp.WorkerScheduler(workerCtx)
	// add workers
	wp.AddWorkers(numberOfWorkers)

	return wp
}

func (wp *workerPool[T, R]) NumberOfWorkers() int32 {
	return wp.numberOfWorkers
}

func (wp *workerPool[T, R]) WorkerScheduler(workerCtx context.Context) {
	for {
		select {
		case <-wp.addWorkerChan:
			go wp.worker(workerCtx)
		}
	}
}

func (wp *workerPool[T, R]) SetNumberOfWorkers(count int32) {
	if wp.numberOfWorkers == count {
		return
	}
	if count < wp.numberOfWorkers {
		wp.RemoveWorkers(wp.numberOfWorkers - count)

		return
	}
	wp.AddWorkers(count - wp.numberOfWorkers)
}

func (wp *workerPool[T, R]) AddWorkers(count int32) {
	if count <= 0 {
		return
	}
	for i := int32(0); i < count; i++ {
		wp.addWorkerChan <- struct{}{}
	}

	wp.numberOfWorkers += count
}

func (wp *workerPool[T, R]) RemoveWorkers(count int32) {
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

func (wp *workerPool[T, R]) SetJobFunc(
	jobFunc JobFunc[T, R]) WorkerPool[T, R] {
	wp.jobFunc = jobFunc

	return wp
}

func (wp *workerPool[T, R]) AddJobs(
	jobRequests ...JobRequest[T]) WorkerPool[T, R] {
	wp.jobRequests = append(wp.jobRequests, jobRequests...)

	return wp
}

func (wp *workerPool[T, R]) RunInBackground() {
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

func (wp *workerPool[T, R]) RunInOrder() WorkerPool[T, R] {
	wp.jobResponseChan = make(chan JobResponse[R], len(wp.jobQueue))
	go func() {
		for index, jobRequest := range wp.jobRequests {
			fmt.Println("scheduling job:", jobRequest)
			wp.wg.Add(1)
			wp.inOrderJobQueue <- InOrderJobRequest[T]{JobRequest: jobRequest, Index: int32(index)}
		}
		wp.wg.Wait()
		fmt.Println("InOrder Jobs Done!!!")
		close(wp.jobResponseChan)
	}()

	return wp
}

func (wp *workerPool[T, R]) ForEach(f func(JobResponse[R])) {
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
		case inOrderJobRequest := <-wp.inOrderJobQueue:
			func() {
				defer wp.wg.Done()
				defer func() {
					if err := recover(); err != nil {
						log.Println(err)
					}
				}()
				jobResponse := wp.jobFunc(inOrderJobRequest.JobRequest)
				wp.cond.L.Lock()
				for wp.counter != inOrderJobRequest.Index {
					wp.cond.Wait()
				}
				wp.cond.L.Unlock()
				wp.jobResponseChan <- jobResponse
				wp.cond.L.Lock()
				wp.counter++
				wp.cond.Broadcast()
				wp.cond.L.Unlock()
			}()
		case <-workerCtx.Done():
			fmt.Println("Worker Context cancelled")
			return
		case <-wp.removeWorkerChan:
			return
		}
	}
}
