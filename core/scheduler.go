package core

import (
	"fmt"
	"time"
)

type Lock interface {
	Lock() (bool, error)
	Unlock()
}

type Locker interface {
	NewLock(*Job) Lock
}

type jobScheduler struct {
	initialized bool
	locker      Locker
}

var scheduler = &jobScheduler{
	initialized: false,
	locker:      nil,
}

func RegisterJobLocker(locker Locker) {
	scheduler.locker = locker
	scheduler.initialized = true
}

func (s *jobScheduler) registerJob(job *Job) {
	if !s.initialized {
		panic("Job scheduler is not initialized! Please, use RegisterJobLocker() to initialize it.")
	}

	switch job.TaskType {
	case ONCE:
		s.scheduleOnceJob(job)
	case FIXED_RATE:
		s.scheduleFixedRateJob(job)
	}
}

func (s *jobScheduler) scheduleOnceJob(job *Job) {
	go func() {
		time.Sleep(job.Duration)

		s.runLocked(job)
	}()
}

func (s *jobScheduler) scheduleFixedRateJob(job *Job) {
	go func() {
		for {
			s.runLocked(job)

			time.Sleep(job.Duration)
		}
	}()
}

func (s *jobScheduler) runLocked(job *Job) {
	lock := s.locker.NewLock(job)

	// if we can't lock the job, we just skip it - it runs anything else
	if ok, err := lock.Lock(); !ok {
		if err != nil {
			fmt.Println(err)
		}

		return
	}

	defer lock.Unlock()

	job.Task()
}
