package common

import (
	"log"
	"time"
)

type Ticker struct {
	stoppedChannel chan struct{}
	stopChannel    chan struct{}
	refresh        time.Duration
}

func NewTicker(refresh time.Duration) *Ticker {
	return &Ticker{
		refresh: refresh,
	}
}

func (t *Ticker) Start(action func() error) {
	t.stopChannel = make(chan struct{})
	t.stoppedChannel = make(chan struct{})
	ticker := time.NewTicker(t.refresh)

	go func() {
		for {
			select {
			case <-t.stopChannel:
				ticker.Stop()
				t.stopChannel = nil
				t.stoppedChannel <- struct{}{}
				return
			case <-ticker.C:
				err := action()
				if err != nil {
					ticker.Stop()
					t.stopChannel = nil
					t.stoppedChannel = nil
					log.Default().Println("Error in ticker action: ", err)
					return
				}
			}
		}
	}()
}

func (t *Ticker) Stop() {
	if t.stopChannel == nil {
		return
	}

	t.stopChannel <- struct{}{}
	<-t.stoppedChannel
}
