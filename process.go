package apifactory

import (
	"context"
	"fmt"
	"time"

	"github.com/betas-in/getter"
	"github.com/betas-in/logger"
	"github.com/betas-in/pool"
	"github.com/betas-in/sidejob"
)

type processQueue struct {
	log             *logger.Logger
	heartbeat       time.Duration
	poll            time.Duration
	queue           *sidejob.Queue
	getter          getter.Getter
	af              *apiFactory
	maxErrorCount   int
	exponentialBase time.Duration
}

// NewProcessQueue returns a new process queue
func NewProcessQueue(log *logger.Logger, af *apiFactory, heartbeat, poll, exponentialBase time.Duration, maxErrorCount int) sidejob.WorkerGroup {
	return &processQueue{
		log:             log,
		heartbeat:       heartbeat,
		poll:            poll,
		getter:          af.getter,
		af:              af,
		maxErrorCount:   maxErrorCount,
		exponentialBase: exponentialBase,
	}
}

func (w *processQueue) SetQueue(q *sidejob.Queue) {
	w.queue = q
}

func (w *processQueue) Process(ctx context.Context, workerCtx *pool.WorkerContext, id string) {
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
			// get data from queue
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

			// check if data is already persisted
			exists, err := job.CheckPersistedData(w.af.storage)
			if err != nil {
				w.logErr(id, err)
			}
			if exists {
				// file already exists in storage, don't need to query again
				w.logDebug(id, "persistData.exists")
				job.Response.Code = 200
				job.QueueResponseError()
				job.LogRequestSuccess(w.log, w.FullName(id), true)
				job.MarkAsProcessed()
				_, err = worker.UpdateTop(worker.WorkerName(), job.String(w.log))
				if err != nil {
					w.logErr(id, err)
					workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
					continue
				}

				_, err = worker.Move(worker.WorkerName(), job.ParsedQueue)
				if err != nil {
					w.logErr(id, err)
					continue
				}
				workerCtx.Processed <- job.String(w.log)
				workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 1}
				continue
			}

			// Get response from getter
			job.Prepare()
			job.Response = w.getter.FetchResponse(*job.Request)

			switch {
			case job.IsRateLimited():
				// move to bottom of queue
				_, err = worker.Move(worker.WorkerName(), queueAFProcess)
				if err != nil {
					w.logErr(id, err)
				}
				continue

			case !job.IsSuccess():
				job.QueueResponseError()
				job.UpdateNextProcessingTime(w.exponentialBase)
				job.LogRequestFailure(w.log, w.FullName(id))
				_, err := worker.UpdateTop(worker.WorkerName(), job.String(w.log))
				if err != nil {
					w.logErr(id, err)
					workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
					continue
				}

				if job.IsAtMaxErrorCount(w.maxErrorCount) {
					w.rejectAndContinue(worker, id, job.String(w.log), nil)
					// Marking as processed when pushing to DLQ
					workerCtx.Processed <- job.String(w.log)
					continue
				}

				_, err = worker.Move(worker.WorkerName(), queueAFFailed)
				if err != nil {
					w.rejectAndContinue(worker, id, job.String(w.log), err)
					continue
				}

				workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
				continue

			case job.IsSuccess():
				job.QueueResponseError()
				err := job.PersistData(w.af.storage)
				if err != nil {
					w.logErr(id, err)
					workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
					continue
				}
				job.LogRequestSuccess(w.log, w.FullName(id), false)
				job.MarkAsProcessed()
				_, err = worker.UpdateTop(worker.WorkerName(), job.String(w.log))
				if err != nil {
					w.logErr(id, err)
					workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 0}
					continue
				}

				_, err = worker.Move(worker.WorkerName(), job.ParsedQueue)
				if err != nil {
					w.logErr(id, err)
					continue
				}
				workerCtx.Processed <- job.String(w.log)
				workerCtx.Heartbeat <- pool.Heartbeat{ID: id, Processed: 1}
				continue
			}
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

func (w *processQueue) FullName(id string) string {
	return fmt.Sprintf("%s%s%s", w.queue.QName, separator, id)
}

func (w *processQueue) logErr(id string, err error) {
	w.log.Error("w.process").Str("worker", w.FullName(id)).Msgf("%+v", err)
}

func (w *processQueue) logDebug(id string, message string) {
	w.log.Debug("w.process").Str("worker", w.FullName(id)).Msgf("%s", message)
}

func (w *processQueue) rejectAndContinue(worker *sidejob.Worker, id, job string, err error) {
	if err != nil {
		w.logErr(id, err)
	}

	// #TODO process apifactory DLQ
	err = worker.Reject(job)
	if err != nil {
		w.logErr(id, err)
	}
}
