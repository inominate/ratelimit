package ratelimit

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

/* DebugLog be set up with a logger for debugging purposes. */
var DebugLog = log.New(ioutil.Discard, "", 0)

//var DebugLog = log.New(os.Stdout, "DEBUG", log.Lshortfile|log.Ltime)

/*
RateLimit should only be created using NewRateLimit()
*/
type RateLimit struct {
	maxEvents int
	period    time.Duration

	outstanding  int
	expireEvents <-chan time.Time
	nextExpire   time.Time

	expires []time.Time
	idx     int
	count   int

	// activeStart is what we nil out when we need to block new requests
	activeStart chan struct{}

	// start keeps our real start channel.
	start  chan struct{}
	finish chan bool
	close  chan chan error

	countReq       chan chan int
	outstandingReq chan chan int
}

/*
run is the main handler, started by NewRateLimit and should never be used
elsewhere
*/
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

		case respChan := <-rl.countReq:
			respChan <- rl.count

		case respChan := <-rl.outstandingReq:
			respChan <- rl.outstanding

		case respChan := <-rl.close:
			rl.runClose(respChan)
			break runLoop
		}
	}

	DebugLog.Printf("Worker cleanup complete, shutting down.")
}

/* addEvent should only ever called by run, dangerous if used elsewhere. */
func (rl *RateLimit) addEvent() {
	now := time.Now()
	rl.expires[rl.idx] = now.Add(rl.period)

	if rl.nextExpire.IsZero() {
		rl.nextExpire = rl.expires[rl.idx]
		rl.expireEvents = time.After(rl.period)
		DebugLog.Printf("No expire exists, adding for %.2f seconds.", rl.period.Seconds())
	}

	rl.idx++
	rl.count++
	if rl.idx >= rl.maxEvents {
		rl.idx = 0
	}

	if rl.count+rl.outstanding >= rl.maxEvents {
		// Stop listening for new start requests.
		rl.activeStart = nil

		DebugLog.Printf("Event limit reached, blocking start requests.")
	}
}

/*  runExpire is used by run to expire events on a timer. */
func (rl *RateLimit) runExpire() {
	now := time.Now()

	oldestEvent := rl.idx - rl.count
	if oldestEvent < 0 {
		oldestEvent = rl.maxEvents + oldestEvent
	}

	DebugLog.Printf("Expiring events, have %d events.", rl.count)

	var next int
	for rl.count > 0 {
		next = oldestEvent + 1
		if next >= rl.maxEvents {
			next = 0
		}

		rl.count--
		oldestEvent = next

		if rl.expires[next].After(now) {
			break
		}

	}
	/*
		DebugLog.Printf("Expired events, have %d events remaining as of %s", rl.count, now.Format(time.StampMilli))

		for i := 0; i < rl.maxEvents; i++ {
			str := ""
			if i == oldestEvent {
				str = "Oldest "
			}

			DebugLog.Printf("%sEvent %d: %s", str, i, rl.expires[i].Format(time.StampMilli))
		}
		time.Sleep(100 * time.Millisecond)
	*/
	if rl.count == 0 {
		DebugLog.Printf("No events left. Going to sleep.")
		rl.nextExpire = time.Time{}
	} else {
		rl.nextExpire = rl.expires[oldestEvent]
		rl.expireEvents = time.After(rl.nextExpire.Sub(now))
		DebugLog.Printf("Waiting until %s", rl.nextExpire.Format(time.StampMilli))
	}

	if rl.activeStart == nil && rl.outstanding+rl.count < rl.maxEvents {
		DebugLog.Printf("Event limit clear, continuing")
		rl.activeStart = rl.start
	}
}

/* runFinish is used by run to handle the completion of a task, marking an event */
func (rl *RateLimit) runFinish(skip bool) {
	if skip {
		DebugLog.Printf("Event finished, but going uncounted.")
	} else {
		rl.addEvent()
		DebugLog.Printf("Event finished, current count is %d.", rl.count)
	}

	rl.outstanding--
	if rl.activeStart == nil && rl.outstanding+rl.count < rl.maxEvents {
		DebugLog.Printf("Event limit clear, accepting new start requests.")
		rl.activeStart = rl.start
	}
}

/* runStart is used by run to handle the beginning of an event. */
func (rl *RateLimit) runStart() {
	rl.outstanding++
	if rl.outstanding+rl.count == rl.maxEvents {
		// Stop listening for start requests causing new ones to block until
		// some existing events finish.
		rl.activeStart = nil

		DebugLog.Printf("New requests could break error limit, slowing down.")
	} else if rl.outstanding+rl.count > rl.maxEvents {
		log.Printf("New requests have broken error limit, this shouldn't happen. %d+%d (%d) > %d", rl.outstanding, rl.count, rl.outstanding+rl.count, rl.maxEvents)
	}
}

/* runClose is used by run to handle the dirty work of shutting down */
func (rl *RateLimit) runClose(respChan chan error) {
	close(rl.close)
	close(rl.start)
	close(rl.finish)
	close(rl.countReq)
	close(rl.outstandingReq)

	var err error
	if rl.outstanding > 0 {
		err = fmt.Errorf("error closing, %d events still rl.outstanding", rl.outstanding)
	}

	respChan <- err
}
