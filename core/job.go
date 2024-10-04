package core

import (
	"github.com/google/uuid"
	"time"
)

type TaskType int

const (
	_ TaskType = iota
	PERIODIC
	ONCE
)

type Task func()

type Job struct {
	Uuid     string
	TaskType TaskType
	task     Task
	duration time.Duration
}

func NewJob(options ...func(job *Job)) *Job {
	job := &Job{
		Uuid: uuid.NewString(),
	}

	for _, option := range options {
		option(job)
	}

	return job
}

func WithTask(task Task) func(job *Job) {
	return func(job *Job) {
		job.task = task
	}
}

func WithDuration(duration time.Duration) func(job *Job) {
	return func(job *Job) {
		job.duration = duration
	}
}

func Once() func(job *Job) {
	return func(job *Job) {
		job.TaskType = ONCE
	}
}

func Periodic() func(job *Job) {
	return func(job *Job) {
		job.TaskType = PERIODIC
	}
}

func (j *Job) Schedule() {
	scheduler.registerJob(j)
}
