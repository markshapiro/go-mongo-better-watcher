package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/markshapiro/go-mongo-better-watcher/betterwatcher"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	betterwatcher "github.com/markshapiro/go-mongo-better-watcher"
)

func main() {
	const URI = "<insert mongo URI>"

	connStr := strings.Replace(URI, "+", "%2B", -1)

	mognoConn, err := mongo.NewClient(options.Client().ApplyURI(connStr))

	if err != nil {
		panic("mongo.NewClient(): " + err.Error())
	}

	ctx := context.Background()

	err = mognoConn.Connect(ctx)

	if err != nil {
		panic("Connect(): " + err.Error())
	}
	myCollection := mognoConn.Database("my_database").Collection("MyCollection")

	config := betterwatcher.RedisConfig{
		Url: "127.0.0.1:6379",
	}

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
			fmt.Println("first instance handling: ", changeDoc.DocKey.Id)
			time.Sleep(1 * time.Second)
			return nil
		})
		fmt.Println("error: ", err)
	}()

	// second instance, to be run on another server
	go func() {
		err = factory.CreateWatcher(myCollection, pipeline, watchId, func(changeDoc *betterwatcher.ChangeDoc) error {
			fmt.Println("second instance handling: ", changeDoc.DocKey.Id)
			time.Sleep(1 * time.Second)
			return nil
		})
		fmt.Println("error: ", err)
	}()

	select {}
}
