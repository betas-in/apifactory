package apifactory

import (
	"context"
	"fmt"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/pool"
	"github.com/betas-in/sidejob"
)

type failedQueue struct {
	log       *logger.Logger
	heartbeat time.Duration
	poll      time.Duration
	queue     *sidejob.Queue
	af        *apiFactory
}

// NewFailedQueue returns a apifactory failed queue
func NewFailedQueue(log *logger.Logger, af *apiFactory, heartbeat, poll time.Duration) sidejob.WorkerGroup {
	return &failedQueue{
		log:       log,
		heartbeat: heartbeat,
		poll:      poll * 2,
		af:        af,
	}
}

func (w *failedQueue) SetQueue(q *sidejob.Queue) {
	w.queue = q
}

func (w *failedQueue) Process(ctx context.Context, workerCtx *pool.WorkerContext, id string) {
	workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Ping: true}

	heartbeatTicker := time.NewTicker(w.heartbeat)
	pollTicker := time.NewTicker(w.poll)
	defer heartbeatTicker.Stop()
	defer pollTicker.Stop()

	worker, err := w.queue.GetWorker(id)
	if err != nil {
		w.logErr(id, err)
		workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
		return
	}

	for {
		select {
		case <-pollTicker.C:
			originalJob, err := worker.Consume()
			if err != nil {
				w.logErr(id, err)
				continue
			}
			if originalJob == "" {
				continue
			}
			// Get Context from JSON data
			job, err := NewContext(originalJob)
			if err != nil {
				w.rejectAndContinue(worker, id, originalJob, err)
				continue
			}

			// Check if time is more than Next
			// If yes, move it back to process queue
			// If no, move it to bottom of the queue

			switch {
			case !job.IsPastWaitingTime():
				_, err = worker.Move(worker.WorkerName(), queueAFFailed)
				if err != nil {
					w.rejectAndContinue(worker, id, job.String(w.log), err)
					continue
				}
			case job.IsPastWaitingTime():
				_, err = worker.Move(worker.WorkerName(), queueAFProcess)
				if err != nil {
					w.rejectAndContinue(worker, id, job.String(w.log), err)
					continue
				}
			}
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
		case j := <-workerCtx.Jobs:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 1}
			workerCtx.Processed <- j
		case <-workerCtx.Close:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
			return
		case <-ctx.Done():
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Closed: true}
			return
		case <-heartbeatTicker.C:
			workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Ping: true}
		}
	}
}

func (w *failedQueue) FullName(id string) string {
	return fmt.Sprintf("%s%s%s", w.queue.QName, separator, id)
}

func (w *failedQueue) logErr(id string, err error) {
	w.log.Error("af.failed.logErr").Str("worker", w.FullName(id)).Msgf("%+v", err)
}

func (w *failedQueue) rejectAndContinue(worker *sidejob.Worker, id, job string, err error) {
	if err != nil {
		w.logErr(id, err)
	}

	err = worker.Reject(job)
	if err != nil {
		w.logErr(id, err)
	}
}
