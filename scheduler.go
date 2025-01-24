package main

import (
	"sync"
	"time"

	"github.com/nubmq/set"
)

/*
set.Size(entry)
set.Insert(entry)
set.Erase(entry)
set.Contains(entry)
set.Clear()
set.Begin(), to get the actual entry of out it: entry := (set.Begin()).Value().(Entry)
set.RBegin()
*/

type Entry struct {
	key       string
	value     string
	canExpire bool
	TTL       int64
}

type SetStorage struct {
	queue                    chan Entry
	set                      set.Set
	mutex                    sync.Mutex
	KeyEntryKeeper           map[string]Entry
	EarliestExpiringKeyEntry Entry
}

func getSet() *set.Set {
	return set.NewSet(func(a, b interface{}) int {
		entryA := a.(Entry)
		entryB := b.(Entry)
		if entryA.TTL < entryB.TTL {
			return -1
		} else if entryA.TTL > entryB.TTL {
			return 1
		}
		return 0
	})
}

// inserts stuff in setStorage
func HandleKeyTTLInsertion(setStorage SetStorage) {
	for {
		entry := <-setStorage.queue

		setStorage.mutex.Lock()
		// check if already exists in setStorage

		val, exists := setStorage.KeyEntryKeeper[entry.key]
		if exists {
			if setStorage.EarliestExpiringKeyEntry == val {
				if setStorage.set.Size() > 0 {
					setStorage.EarliestExpiringKeyEntry = (setStorage.set.Begin()).Value().(Entry)

				} else {
					// make it null I guess
				}
			}
			setStorage.set.Remove(val)
		}
		if setStorage.set.Size() == 0 || setStorage.EarliestExpiringKeyEntry.TTL > entry.TTL {
			setStorage.EarliestExpiringKeyEntry = entry
		}
		setStorage.KeyEntryKeeper[entry.key] = entry
		setStorage.set.Insert(entry)

		setStorage.mutex.Unlock()
	}
}

// sleeps until it gets called
func HandleKeyTTLEviction(setStorage SetStorage, updateChan chan time.Duration) {
	timer := time.NewTimer(time.Duration(1 * time.Hour))

	for {
		select {
		case <-timer.C:
			// start the cleanup
			setStorage.mutex.Lock()
			if setStorage.set.Size() > 0 {
				for it := setStorage.set.Begin(); setStorage.set.Size() > 0; {
					entry := it.Value().(Entry)
					curTTL := entry.TTL
					now := time.Now().Unix()
					if curTTL <= now {
						setStorage.set.Remove(entry)
						// TODO: send an expiry event out for this key
					} else {
						duration := time.Duration(curTTL-now) * time.Second
						timer.Reset(duration)
						break
					}
				}
			}
			setStorage.mutex.Unlock()
		case newDuration := <-updateChan:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(newDuration)
		}
	}
}

func startStuff() {
	setContainer := SetStorage{
		queue:          make(chan Entry),
		set:            *getSet(),
		KeyEntryKeeper: make(map[string]Entry),
	}
	updateChan := make(chan time.Duration)
	go HandleKeyTTLInsertion(setContainer)
	go HandleKeyTTLEviction(setContainer, updateChan)
}

func main() {
	startStuff()
}
