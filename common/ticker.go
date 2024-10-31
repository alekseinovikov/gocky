package common

import "time"

type Ticker struct {
	stopChannel chan struct{}
	refresh     time.Duration
}

func NewTicker(refresh time.Duration) *Ticker {
	return &Ticker{
		refresh: refresh,
	}
}

func (t *Ticker) Start(action func() error) {
	t.stopChannel = make(chan struct{})
	ticker := time.NewTicker(t.refresh)

	go func() {
		for {
			select {
			case <-t.stopChannel:
				ticker.Stop()
				return
			case <-ticker.C:
				err := action()
				if err != nil {
					ticker.Stop()
					close(t.stopChannel)
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
	close(t.stopChannel)
}
