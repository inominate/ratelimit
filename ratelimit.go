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

	outstanding  int
	expireEvents <-chan time.Time
	events       map[time.Time]struct{}
	nextExpire   time.Time

	// activeStart is what we nil out when we need to block new requests
	activeStart chan struct{}

	// start keeps our real start channel.
	start  chan struct{}
	finish chan bool
	close  chan chan error
}

/* countEvents should only ever called by run, dangerous if used elsewhere. */
func (rl *RateLimit) countEvents() (eventCount int) {
	var nextExpire time.Time
	now := time.Now()

	for t := range rl.events {
		if t.Before(now) {
			delete(rl.events, t)
		} else {
			eventCount++
		}
		if nextExpire.IsZero() || t.Before(nextExpire) {
			nextExpire = t
		}
	}

	if nextExpire.IsZero() {
		rl.expireEvents = nil
	} else if nextExpire != rl.nextExpire {
		// Don't create new timers unless we actually need to.
		rl.nextExpire = nextExpire
		rl.expireEvents = time.After(nextExpire.Sub(now))
	}
	return
}

/* addEvent should only ever called by run, dangerous if used elsewhere. */
func (rl *RateLimit) addEvent() {
	rl.events[time.Now().Add(rl.period)] = struct{}{}
}

func (rl *RateLimit) run() {
runLoop:
	for {
		select {
		case <-rl.expireEvents:
			rl.runExpire()

		case skip := <-rl.finish:
			rl.runFinish(skip)

		case <-rl.activeStart:
			rl.runStart()

		case respChan := <-rl.close:
			rl.runClose(respChan)
			break runLoop

		}
	}

	DebugLog.Printf("Worker cleanup complete, shutting down.")
}

func (rl *RateLimit) runExpire() {
	count := rl.countEvents()
	DebugLog.Printf("Expired events, have %d events remaining.", count)

	if rl.outstanding+count < rl.maxEvents {
		DebugLog.Printf("Event limit clear, continuing")
		rl.activeStart = rl.start
	}
}

func (rl *RateLimit) runFinish(skip bool) {
	count := rl.countEvents()

	if skip {
		DebugLog.Printf("Event finished, but going uncounted.")
	} else {
		rl.addEvent()
		count++

		DebugLog.Printf("Event finished, current count is %d.", count)
		if count >= rl.maxEvents {
			// Stop listening for new start requests.
			rl.activeStart = nil

			DebugLog.Printf("Event limit reached, blocking start requests.")
		}
	}

	rl.outstanding--
	if rl.outstanding+count < rl.maxEvents {
		DebugLog.Printf("Event limit clear, accepting new start requests.")
		rl.activeStart = rl.start
	}
}

func (rl *RateLimit) runStart() {
	count := len(rl.events)

	rl.outstanding++
	if rl.outstanding+count == rl.maxEvents {
		// Stop listening for start requests causing new ones to block until
		// some existing tasks finish.
		rl.activeStart = nil

		DebugLog.Printf("New requests could break error limit, slowing down.")
	} else if rl.outstanding+count > rl.maxEvents {
		log.Printf("New requests have broken error limit, this shouldn't happen. %d+%d (%d) > %d", rl.outstanding, count, rl.outstanding+count, rl.maxEvents)
	}
}

func (rl *RateLimit) runClose(respChan chan error) {
	close(rl.close)
	close(rl.start)
	close(rl.finish)

	var err error
	if rl.outstanding > 0 {
		err = fmt.Errorf("error closing, %d events still rl.outstanding", rl.outstanding)
	}

	respChan <- err
}

func NewRateLimit(maxEvents int, period time.Duration) *RateLimit {
	var rl RateLimit

	rl.start = make(chan struct{})
	rl.finish = make(chan bool, maxEvents*2)
	rl.close = make(chan chan error)

	rl.events = make(map[time.Time]struct{}, maxEvents)

	rl.maxEvents = maxEvents
	rl.period = period

	rl.activeStart = rl.start

	go rl.run()

	return &rl
}

var ErrTimeout = errors.New("timeout waiting for clearance to continue")
var ErrAlreadyClosed = errors.New("already closed")

func (rl *RateLimit) Start(timeout time.Duration) (retErr error) {
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

	case rl.start <- struct{}{}:
		return nil
	}
}

func (rl *RateLimit) Finish(skip bool) (retErr error) {
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

	rl.finish <- skip

	return nil
}

func (rl *RateLimit) Close() (retErr error) {
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
	rl.close <- respChan
	err := <-respChan

	return err
}
