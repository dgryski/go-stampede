// Package stampede provides optimal cache stampede prevention
/*
   http://www.vldb.org/pvldb/vol8/p886-vattani.pdf
*/
package stampede

import (
	"math"
	"math/rand"
	"time"
)

// Item is a cache item
type Item struct {
	Value  interface{}
	Expiry time.Time
	Delta  time.Duration
}

// Cache is the interface to the backing cache
type Cache interface {
	// Cache Read
	Get(key string) (Item, error)

	// Cache Write
	Set(key string, item Item) error
}

// XFetcher provides stampede protection for items in a cache
type XFetcher struct {
	cache Cache
	r     *rand.Rand
}

const Beta = 1

// New returns a new XFetcher protecting the cache.
func New(cache Cache) *XFetcher {
	return &XFetcher{
		cache: cache,
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Fetch retrieves `key`, recomputing it if needed.  The `recompute` function
// should compute the value for key, returning also the desired time-to-live and any
// error.
func (xf *XFetcher) Fetch(key string, recompute func() (value interface{}, ttl time.Duration, err error)) (interface{}, error) {

	item, err := xf.cache.Get(key)

	if err != nil || time.Now().Add(-time.Duration(float64(item.Delta*Beta)*math.Log(xf.r.Float64()))).After(item.Expiry) {
		start := time.Now()
		value, ttl, err := recompute()
		if err != nil {
			return nil, err
		}
		item = Item{
			Value:  value,
			Expiry: time.Now().Add(ttl),
			Delta:  time.Since(start),
		}
		// TODO(dgryski): Determine behaviour on cache write failure
		_ /* err */ = xf.cache.Set(key, item)
	}

	return item.Value, nil
}
