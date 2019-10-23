package betterwatcher

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/bsm/redislock"
)

const (
	LOCK_TTL_DEFAULT       = 1 * time.Minute
	LOCK_OBTAIN_RETRY_TIME = 10 * time.Second
	LOCK_REFRESH_TIME      = 10 * time.Second
	LOCK_PREFIX            = "betterwatcher::lock::"
)

func New(redisConfig RedisConfig, opts ...Options) *WatcherFactory {
	factory := WatcherFactory{}
	factory.redisClient = redisConfig.Client
	if redisConfig.Client == nil {
		factory.redisClient = GetRedisInstance(redisConfig.Url)
	}
	if len(opts) > 0 {
		factory.options = opts[0]
	} else {
		factory.options = Options{}
	}

	if factory.options.Context == nil {
		factory.options.Context = context.Background()
	}

	return &factory
}

// lock refresh loop until received error (sent to errorChan) or got signal to stop (through endSignalChan)
func startRefreshing(lock *redislock.Lock) (chan bool, chan error) {

	endSignalChan := make(chan bool, 1)
	errorChan := make(chan error, 1)

	go func() {
	start:
		err := lock.Refresh(LOCK_TTL_DEFAULT, nil)
		if err != nil {
			errorChan <- err
			return
		}
		select {
		case <-endSignalChan:
		case <-time.After(LOCK_REFRESH_TIME):
			goto start
		}
	}()

	return endSignalChan, errorChan
}

func stopRefreshing(endSignalChan chan bool) {
	select {
	case endSignalChan <- true:
	default:
	}
}

func (factory WatcherFactory) CreateWatcher(collection *mongo.Collection, pipeline []bson.M, watcherId string, handler func(*ChangeDoc) error) error {
	ctx := factory.options.Context

	var locker *redislock.Client
	var lock *redislock.Lock
	var err error
	var timeAtWatchStart int64

	locker = redislock.New(factory.redisClient)

start:

	lock, err = locker.Obtain(LOCK_PREFIX+watcherId, LOCK_TTL_DEFAULT, nil)
	defer lock.Release()
	if err == redislock.ErrNotObtained {
		time.Sleep(LOCK_OBTAIN_RETRY_TIME)
		goto start
	} else if err != nil {
		return err
	}

	endSignalChan, errorChan := startRefreshing(lock)
	defer stopRefreshing(endSignalChan)

	opts := options.ChangeStream()
	if factory.options.AttachDocumentOnUpdate {
		opts = opts.SetFullDocument(options.UpdateLookup)
	}
	resumeToken, err := factory.getToken(watcherId)
	if err != nil {
		return err
	}
	if resumeToken != nil {
		opts = opts.SetResumeAfter(resumeToken)
	}
	stream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return err
	}
	defer stream.Close(ctx)
	fmt.Println("Watcher " + watcherId + ": Watcher created")

	timeAtWatchStart = time.Now().UTC().UnixNano()

next:

	err = lock.Refresh(LOCK_TTL_DEFAULT, nil)
	if err == redislock.ErrNotObtained {
		goto restart
	} else if err != nil {
		return err
	}

	// if WatcherSwitchAfter time passed, we need to hang over the watch to other instance (whoever obtain the lock next)
	if timeAtWatchStart > 0 && factory.options.WatcherSwitchAfter > 0 && time.Now().UTC().UnixNano()-timeAtWatchStart > int64(factory.options.WatcherSwitchAfter) {

		stopRefreshing(endSignalChan)
		err := lock.Release()

		// to prevent obtaining lock immediately while other instances may be waiting
		time.Sleep(LOCK_OBTAIN_RETRY_TIME)

		if err != nil {
			return err
		}
		goto restart
	}

	// if we received error from startRefreshing loop
	select {
	case err = <-errorChan:
		if err == redislock.ErrNotObtained {
			goto restart
		}
		return err
	default:
	}

	// if some error ocurred with the stream
	if stream.Err() != nil {
		fmt.Println("Watcher " + watcherId + ": Stream Error occured: " + stream.Err().Error())
		goto restart
	}

	// NOTE: Next() is blocking forever, passing maxAwaitTimeMS to Watch() does not seem to affect it for some reason
	if stream.Next(ctx) {
		var changeDoc ChangeDoc
		next := stream.Current
		err := bson.Unmarshal(next, &changeDoc)
		if err != nil {
			return err
		}

	retry:
		err = handler(&changeDoc)
		if err != nil {
			if factory.options.MaxRetries == 0 {
				goto retry
			}
			currentRetries, err := factory.increaseRetries(changeDoc.Id)
			if err != nil {
				return err
			}
			if currentRetries < factory.options.MaxRetries {
				goto retry
			}
		}

		// finally store resume token
		err = factory.storeToken(changeDoc.Id, watcherId)

		if err != nil {
			return err
		}
	}

	goto next

restart:
	fmt.Println("Watcher " + watcherId + ": Closing and restarting")
	stream.Close(ctx)
	goto start
}
