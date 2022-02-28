package apifactory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/betas-in/getter"
	"github.com/betas-in/googlestorage"
	"github.com/betas-in/logger"
	"github.com/betas-in/sidejob"
)

// APIFactory ...
type APIFactory interface {
	Start(count int64) (chan interface{}, chan interface{})
	Stop(ctx context.Context)
	UpdateProcess(count int64)
	UpdateFailed(count int64)
	AddRateLimit(host, bucket string, count int64) error
	Publish(cx *Context) error
}

type apiFactory struct {
	conf             *Config
	log              *logger.Logger
	sidejob          sidejob.Sidejob
	processPublisher *sidejob.Queue
	processConsumer  *sidejob.Queue
	failedPublisher  *sidejob.Queue
	failedConsumer   *sidejob.Queue
	parsedPublishers map[string]*sidejob.Queue
	getter           getter.Getter
	storage          googlestorage.GCStorage
	lock             *sync.RWMutex
}

type Config struct {
	Heartbeat       time.Duration
	Poll            time.Duration
	ExponentialBase time.Duration
	MaxErrorCount   int
}

var (
	queueAFProcess = "af.process"
	queueAFFailed  = "af.failed"
	separator      = "."
)

// NewAPIFactory returns a new APIfactory object
func NewAPIFactory(conf *Config, log *logger.Logger, sj sidejob.Sidejob, gcstore googlestorage.GCStorage) (APIFactory, error) {
	a := apiFactory{
		conf:             conf,
		log:              log,
		getter:           getter.NewGetter(log),
		sidejob:          sj,
		parsedPublishers: map[string]*sidejob.Queue{},
		storage:          gcstore,
		lock:             &sync.RWMutex{},
	}

	// a.getter.SetDefaultTimeout()
	// a.getter.SetUserAgent()
	a.getter.SetCache(sj.GetCache())

	var err error
	a.processPublisher, err = sj.GetPublisher(queueAFProcess)
	if err != nil {
		log.Fatal("af.new").Msgf("%+v", err)
		return nil, err
	}

	a.failedPublisher, err = sj.GetPublisher(queueAFFailed)
	if err != nil {
		log.Fatal("af.new").Msgf("%+v", err)
		return nil, err
	}

	a.processConsumer, err = sj.GetConsumerWithWorker(
		queueAFProcess,
		NewProcessQueue(log, &a, conf.Heartbeat, conf.Poll, conf.ExponentialBase, conf.MaxErrorCount),
	)
	if err != nil {
		log.Fatal("af.new").Msgf("%+v", err)
		return nil, err
	}

	a.failedConsumer, err = sj.GetConsumerWithWorker(
		queueAFFailed,
		NewFailedQueue(log, &a, conf.Heartbeat, conf.Poll),
	)
	if err != nil {
		log.Fatal("af.new").Msgf("%+v", err)
		return nil, err
	}

	return &a, nil
}

func (a *apiFactory) Start(count int64) (chan interface{}, chan interface{}) {
	process := a.processConsumer.Start(count)
	failed := a.failedConsumer.Start(1)
	return process, failed
}

func (a *apiFactory) UpdateProcess(size int64) {
	a.processConsumer.Update(size)
}

func (a *apiFactory) UpdateFailed(size int64) {
	a.failedConsumer.Update(size)
}

func (a *apiFactory) Stop(ctx context.Context) {
	a.processConsumer.Stop(ctx)
	a.failedConsumer.Stop(ctx)
}

func (a *apiFactory) AddRateLimit(host, bucket string, count int64) error {
	// #TODO move rate limit to config
	return a.getter.AddRateLimit(host, bucket, count)
}

func (a *apiFactory) Publish(cx *Context) error {
	if cx.Request.Path == "" {
		return fmt.Errorf("path is missing in request")
	}
	if cx.ParsedQueue == "" {
		return fmt.Errorf("parsedQueue is missing in request")
	}
	cx.Published = time.Now()

	_, err := a.processPublisher.Publish(cx.String(a.log))
	a.log.Info("af.publish").Msgf("toProcess: %s", cx.Request.Path)
	return err
}

//
// Internal functions
//
