package apifactory

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/betas-in/getter"
	"github.com/betas-in/googlestorage"
	"github.com/betas-in/logger"
	"github.com/betas-in/utils"
)

// Context contains details about a request in apifactory
type Context struct {
	Request        *getter.Request
	Response       *getter.Response
	Next           time.Time
	Published      time.Time
	Processed      time.Time
	ProcessingTime time.Duration
	ParsedQueue    string
	QueueErrors    []string
	PersistHash    string
}

// NewContext creates a context from a string
func NewContext(payload string) (*Context, error) {
	c := Context{}
	err := json.Unmarshal([]byte(payload), &c)
	if c.Response == nil {
		c.Response = &getter.Response{}
	}
	return &c, err
}

func (c *Context) String(log *logger.Logger) string {
	data, err := json.Marshal(c)
	if err != nil {
		log.Error("af.context.string").Msgf("%+v", err)
	}
	return string(data)
}

func (c *Context) Prepare() {
	if c.PersistHash != "" {
		c.Request.SaveToDisk = true
	}
}

// IsRateLimited checks if request was rate limited
func (c *Context) IsRateLimited() bool {
	return errors.Is(c.Response.Error, getter.ErrRateLimited)
}

// IsSuccess checks if the response was successful or not
func (c *Context) IsSuccess() bool {
	return (c.Response.Code >= 200 && c.Response.Code < 500)
}

// IsAtMaxErrorCount checks if the number of failures is equal to max
func (c *Context) IsAtMaxErrorCount(max int) bool {
	return len(c.QueueErrors) >= max
}

// MarkAsProcessed update processed timestamp and processing time
func (c *Context) MarkAsProcessed() {
	c.Processed = time.Now()
	c.ProcessingTime = c.Processed.Sub(c.Published)
}

// QueueResponseError adds the response error to the queue of error
func (c *Context) QueueResponseError() {
	if c.Response != nil && c.Response.Error != nil {
		c.QueueErrors = append(c.QueueErrors, c.Response.Error.Error())
		c.Response.Error = nil
	}
}

// UpdateNextProcessingTime updates the processing time to the next value
func (c *Context) UpdateNextProcessingTime(delta time.Duration) {
	if delta == 0 {
		delta = time.Millisecond
	}
	errorLength := len(c.QueueErrors)
	extention := math.Pow(10, float64(errorLength))
	// Add jitter for +- 10%, float64 (0-1), /5 (0-0.2), +0.9 (0.9-1.1)
	multiplier := (rand.Float64() / 5) + 0.9
	extention = extention * multiplier
	c.Next = time.Now().Add(delta * time.Duration(extention))
}

// IsPastWaitingTime checks if the job is beyond the waiting time
func (c *Context) IsPastWaitingTime() bool {
	return time.Now().Unix() >= c.Next.Unix()
}

// LogRequestSuccess logs a successful request
func (c *Context) LogRequestSuccess(log *logger.Logger, name string, fromStorage bool) {
	storageText := ""
	if fromStorage {
		storageText = " (from storage)"
	}
	log.Info("af.context.logRequestSuccess").Str("worker", name).Msgf("successfully fetched: %s%s", c.Request.Path, storageText)
}

// LogRequestFailure logs a request failure
func (c *Context) LogRequestFailure(log *logger.Logger, name string) {
	delta := time.Until(c.Next)
	if c.Response.Error == nil {
		log.Error("af.context.logRequestFailure").Str("worker", name).Msgf("request failed %d, retrying in %s: %s", c.Response.Code, delta, c.Request.Path)
	} else {
		log.Error("af.context.logRequestFailure").Str("worker", name).Msgf("request failed %d %+v, retrying in %s: %s", c.Response.Code, c.Response.Error, delta, c.Request.Path)
	}
}

// CheckPersistedData ...
func (c *Context) CheckPersistedData(gcs googlestorage.GCStorage) (bool, error) {
	if !c.shouldPersist() {
		return false, nil
	}

	exists, err := gcs.Exists(c.PersistHash)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// PersistData ...
func (c *Context) PersistData(gcs googlestorage.GCStorage) error {
	if !c.shouldPersist() {
		return nil
	}

	err := gcs.Upload(c.Response.DataPath, c.PersistHash)
	if err != nil {
		return err
	}
	os.Remove(c.Response.DataPath)
	c.Response.DataPath = fmt.Sprintf("gs://%s", c.PersistHash)
	return nil
}

func (c *Context) shouldPersist() bool {
	if c.PersistHash != "" {
		return true
	}
	if utils.Array().Contains([]string{"zip", "gz", "pdf"}, c.Response.ContentType, false) >= 0 {
		c.PersistHash = c.Request.Path
		c.PersistHash = strings.Replace(c.PersistHash, "http://", "", -1)
		c.PersistHash = strings.Replace(c.PersistHash, "https://", "", -1)
		c.PersistHash = strings.Replace(c.PersistHash, "/", "_", -1)
		return true
	}
	return false
}
