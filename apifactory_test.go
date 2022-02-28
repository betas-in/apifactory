package apifactory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/betas-in/getter"
	"github.com/betas-in/googlestorage"
	"github.com/betas-in/logger"
	"github.com/betas-in/rediscache"
	"github.com/betas-in/sidejob"
	"github.com/betas-in/utils"
)

func TestAPIFactory(t *testing.T) {
	log := logger.NewLogger(1, true)
	redisConf := rediscache.Config{
		Host:     "127.0.0.1",
		Port:     9876,
		Password: "596a96cc7bf9108cd896f33c44aedc8a",
	}

	heartbeat, _ := time.ParseDuration("15s")
	poll, _ := time.ParseDuration("100ms")
	base, _ := time.ParseDuration("1ms")
	timeout, _ := time.ParseDuration("10m")
	bucket := "networth.leftshift.io"

	apifConfig := Config{
		Heartbeat:       heartbeat,
		Poll:            poll,
		ExponentialBase: base,
		MaxErrorCount:   5,
	}

	cache, err := rediscache.NewCache(&redisConf, log)
	utils.Test().Nil(t, err)
	sj, err := sidejob.NewSidejob(log, cache, heartbeat)
	utils.Test().Nil(t, err)
	gcstore, err := googlestorage.NewGCStorage(bucket, timeout, log)
	utils.Test().Nil(t, err)
	af, err := NewAPIFactory(&apifConfig, log, sj, gcstore)
	utils.Test().Nil(t, err)

	queueParsed := "test.parsed"
	publishCount := 20
	workerCount := 2

	err = af.AddRateLimit("httpbin.org", "1s", 5)
	utils.Test().Nil(t, err)

	// Start API factory
	pp, _ := af.Start(int64(workerCount))

	// Publish requests
	for i := 0; i < publishCount-3; i++ {
		cx := Context{
			Request:     &getter.Request{Path: "https://httpbin.org/status/200%2C201%2C202%2C404%2C500"},
			ParsedQueue: queueParsed,
		}
		err = af.Publish(&cx)
		utils.Test().Nil(t, err)
	}

	err = af.Publish(&Context{
		Request:     &getter.Request{Path: "https://httpbin.org/redirect-to?url=https%3A%2F%2Fhttpbin.org%2Fstatus%2F211&status_code=211"},
		ParsedQueue: queueParsed,
	})
	utils.Test().Nil(t, err)

	err = af.Publish(&Context{
		Request:     &getter.Request{Path: "https://github.com/gojekfarm/async-worker/archive/refs/heads/master.zip"},
		ParsedQueue: queueParsed,
		// PersistHash: "gojekfarm/async-worker/archive/refs/heads/master.zip",
	})
	utils.Test().Nil(t, err)

	err = af.Publish(&Context{
		Request:     &getter.Request{Path: "https://adslfkjasldfjasjfa.com"},
		ParsedQueue: queueParsed,
	})
	utils.Test().Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := 0
	for cxParsed := range pp {
		count++
		cx, err := NewContext(cxParsed.(string))
		if err != nil {
			fmt.Println(err)
		}
		if cx.Response.DataPath != "" {
			err = os.Remove(cx.Response.DataPath)
			if err != nil {
				fmt.Println(err)
			}
		}

		if count == publishCount {
			af.Stop(ctx)
			break
		}
	}

}
