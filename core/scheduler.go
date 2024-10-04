package core

import "time"

type Locker interface {
	Lock()
	Unlock()
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
	case PERIODIC:
		s.schedulePeriodicJob(job)
	}
}

func (s *jobScheduler) scheduleOnceJob(job *Job) {
	go func() {
		time.Sleep(job.duration)

		s.runLocked(job)
	}()
}

func (s *jobScheduler) schedulePeriodicJob(job *Job) {
	go func() {
		for {
			s.runLocked(job)

			time.Sleep(job.duration)
		}
	}()
}

func (s *jobScheduler) runLocked(job *Job) {
	s.locker.Lock()
	defer s.locker.Unlock()

	job.task()
}
