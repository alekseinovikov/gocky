package core

import (
	"fmt"
	"time"
)

type TaskType int

const (
	_ TaskType = iota
	ONCE
	FIXED_RATE
)

type Task func()

type Job struct {
	Name     string
	TaskType TaskType
	Task     Task
	Duration time.Duration
}

func NewJob(options ...func(job *Job)) (*Job, error) {
	job := &Job{}

	for _, option := range options {
		option(job)
	}

	err := validateState(job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func validateState(job *Job) error {
	if job.Name == "" {
		return fmt.Errorf("job name is required")
	}

	if job.Task == nil {
		return fmt.Errorf("job task is required")
	}

	if job.Duration == 0 {
		return fmt.Errorf("job duration is required")
	}

	if job.TaskType == 0 {
		return fmt.Errorf("job task type is required")
	}

	return nil
}

func WithTask(task Task) func(job *Job) {
	return func(job *Job) {
		job.Task = task
	}
}

func WithDuration(duration time.Duration) func(job *Job) {
	return func(job *Job) {
		job.Duration = duration
	}
}

func Once() func(job *Job) {
	return func(job *Job) {
		job.TaskType = ONCE
	}
}

func FixedRate() func(job *Job) {
	return func(job *Job) {
		job.TaskType = FIXED_RATE
	}
}

func (j *Job) Schedule() {
	scheduler.registerJob(j)
}
