While working with mongo watchers, I had to do a lot of work scaling it out and managing resume tokens.
<br>I had to make sure that each message is processed only once, since mongo watcher updates are broadcasted in fanout mode which results in messages being processed multiple times if you scale the watchers on mutliple servers, and I didn't want to manage additional single server dedicated only to updates.

I decided to write a library to run multiple watcher instances and still handle each message once, and manage resume token internally, so that it proceeds to read after last successfuly handled message.

Only one instance at a time will run watcher on collection, the other instances will serve as failover instances, the moment the current watcher instance closes stream due to panic or internal error, any other instance will automatically take over.

### Import

`go get -u github.com/markshapiro/go-mongo-better-watcher`

```go
import (
	betterwatcher "github.com/markshapiro/go-mongo-better-watcher"
)
```

### Usage

```go
myCollection := mognoConn.Database("my_database").Collection("MyCollection")

config := betterwatcher.RedisConfig{ Url: "127.0.0.1:6379" }

opts := betterwatcher.Options{
    AttachDocumentOnUpdate: true,
    WatcherSwitchAfter:     1 * time.Minute,
    MaxRetries:             3,
}

factory := betterwatcher.New(config, opts)

pipeline := []bson.M{
    bson.M{"$match": bson.M{
        "$or": []bson.M{
            bson.M{"operationType": "insert"}, // listen to inserts
            //bson.M{"operationType": "update"}, // listen to updates
        },
    }},
}

watchId := "watch_id"

go func() {
    err = factory.CreateWatcher(myCollection, pipeline, watchId, func(changeDoc *betterwatcher.ChangeDoc) error {
        // handler
        
        return nil
    })
}()
```

### Config

`New()` takes redis connection config as first parameter, with either database endpoint passed in `Url` property, or a client of `github.com/go-redis/redis` library in `Client` property.

### Options

##### WatcherSwitchAfter
no switching by default

If you want your watcher instances to switch once in a while to balance out the load, use this parameter to pass a time duration after which the instances should switch.

##### AttachDocumentOnUpdate
default: false

Will pass full document to handler when document was modified (by default it happens only for inserted documents)

##### MaxRetries
default: unlimited

How many times should the library retry to process the message before discarding it, when handler returns error.

### NOTE
1) When update handler returns error, the message will be reprocessed unlimited amount of times and hold the processing of next messages, unless `MaxRetries` is specified.
2) If your message handler runs too long, it will delay the handling of next message, in this case it would be better to forward the messages into some queue.

### TODO
There is an idea for proper solution for horizontal scaling described here: https://stackoverflow.com/questions/54295043/what-is-a-good-horizontal-scaling-strategy-for-a-mongodb-change-stream-reader
<br>but it requires mongo version 4 in order to convert `_id` to string before applying regex.
