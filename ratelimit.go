/*
A generic rate limiter.

Limits the rate at which events can complete.
*/
package ratelimit

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

// Set up a logger that can be turned on for debugging.
var DebugLog = log.New(ioutil.Discard, "", 0)

type RateLimit struct {
	maxEvents int
	period    time.Duration

	expireEvents <-chan time.Time
	events       map[time.Time]struct{}

	start  chan struct{}
	finish chan bool
	close  chan chan error
}

/* countEvents should only ever called by run, dangerous if used elsewhere. */
func (e *RateLimit) countEvents() (eventCount int) {
	var nextExpire time.Time
	now := time.Now()

	for t := range e.events {
		if t.Before(now) {
			delete(e.events, t)
		} else {
			eventCount++
		}
		if nextExpire.IsZero() || t.Before(nextExpire) {
			nextExpire = t
		}
	}

	if nextExpire.IsZero() {
		e.expireEvents = nil
	} else {
		e.expireEvents = time.After(nextExpire.Sub(now))
	}
	return
}

/* addEvent should only ever called by run, dangerous if used elsewhere. */
func (e *RateLimit) addEvent() {
	e.events[time.Now().Add(e.period)] = struct{}{}
}

func (e *RateLimit) run() {
	var count, outstanding int
	var startChan = e.start

	for {
		select {
		case skip := <-e.finish:
			if skip {
				DebugLog.Printf("Event finished, but going uncounted.")
			} else {
				e.addEvent()
				count = e.countEvents()

				DebugLog.Printf("Event finished, current count is %d.", count)
				if count >= e.maxEvents {
					// Stop listening for new start requests.
					startChan = nil

					DebugLog.Printf("Event limit reached, blocking start requests.")
				}
			}

			outstanding--
			if outstanding+count < e.maxEvents {
				DebugLog.Printf("Event limit clear, accepting new start requests.")
				startChan = e.start
			}

		case <-startChan:
			count = e.countEvents()

			outstanding++
			DebugLog.Printf("New Item Starting, %d outstanding.", outstanding)
			if outstanding+count == e.maxEvents {
				DebugLog.Printf("New requests could break error limit, slowing down.")
				// Stop listening for start requests causing new ones to block until
				// some existing tasks finish.
				startChan = nil
			} else if outstanding+count > e.maxEvents {
				log.Printf("New requests have broken error limit, this shouldn't happen. %d+%d (%d) > %d", outstanding, count, outstanding+count, e.maxEvents)
			}

		case respChan := <-e.close:
			DebugLog.Printf("Beginning worker cleanup.")

			close(e.close)
			close(e.start)
			close(e.finish)

			var err error
			if outstanding > 0 {
				err = fmt.Errorf("error closing, %d events still outstanding", outstanding)
			}

			respChan <- err

			DebugLog.Printf("Worker cleanup complete, shutting down.")
			return

		case <-e.expireEvents:
			count = e.countEvents()
			DebugLog.Printf("Expired events, have %d events remaining.", count)

			if outstanding+count < e.maxEvents {
				DebugLog.Printf("Event limit clear, continuing")
				startChan = e.start
			}

		}
	}
}

func NewRateLimit(maxEvents int, period time.Duration) *RateLimit {
	var e RateLimit
	e.start = make(chan struct{})
	e.finish = make(chan bool, maxEvents*2)
	e.close = make(chan chan error)

	e.events = make(map[time.Time]struct{}, maxEvents)

	e.maxEvents = maxEvents
	e.period = period

	go e.run()

	return &e
}

var ErrTimeout = errors.New("timeout waiting for clearance to continue")
var ErrAlreadyClosed = errors.New("already closed")

func (e *RateLimit) Start(timeout time.Duration) (retErr error) {
	// Use recover to avoid panicing the entire program should start be called
	// on a closed RateLimit.
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(error)
			if !ok || e == nil {
				panic(r)
			}

			if e.Error() == "runtime error: send on closed channel" {
				retErr = ErrAlreadyClosed
			} else {
				retErr = e
			}
		}
	}()

	var timeoutChan <-chan time.Time
	if timeout != 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case <-timeoutChan:
		return ErrTimeout

	case e.start <- struct{}{}:
		return nil
	}
}

func (e *RateLimit) Finish(skip bool) (retErr error) {
	// Use recover to avoid panicing the entire program should start be called
	// on a closed RateLimit.
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(error)
			if !ok || e == nil {
				panic(r)
			}

			if e.Error() == "runtime error: send on closed channel" {
				DebugLog.Printf("Already closed: %s", e)
				retErr = ErrAlreadyClosed
			} else {
				DebugLog.Printf("Other Error: %s", e)
				retErr = e
			}
		}
	}()

	e.finish <- skip

	return nil
}

func (e *RateLimit) Close() (retErr error) {
	// Use recover to avoid panicing the entire program should start be called
	// on a closed RateLimit.
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(error)
			if !ok || e == nil {
				panic(r)
			}

			if e.Error() == "runtime error: send on closed channel" {
				DebugLog.Printf("Already closed: %s", e)
				retErr = ErrAlreadyClosed
			} else {
				DebugLog.Printf("Other Error: %s", e)
				retErr = e
			}
		}
	}()

	respChan := make(chan error)
	e.close <- respChan
	err := <-respChan

	return err
}
