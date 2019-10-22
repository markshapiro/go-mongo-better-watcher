package betterwatcher

import (
	"context"
	"time"

	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WatcherFactory struct {
	redisClient *redis.Client
	options     Options
}

type Options struct {
	Context                context.Context
	AttachDocumentOnUpdate bool
	WatcherSwitchAfter     time.Duration
	MaxRetries             int
}

type RedisConfig struct {
	Url    string
	Client *redis.Client
}

type ChangeDoc struct {
	Id     *bson.Raw `bson:"_id"`
	DocKey struct {
		Id primitive.ObjectID `bson:"_id"`
	} `bson:"documentKey"`
	Operation         string              `bson:"operationType"`
	FullDoc           *bson.Raw           `bson:"fullDocument"`
	Timestamp         primitive.Timestamp `bson:"clusterTime"`
	UpdateDescription struct {
		UpdatedFields *bson.Raw `bson:"updatedFields"`
		RemovedFields []string  `bson:"removedFields"`
	} `bson:"updateDescription"`
}
