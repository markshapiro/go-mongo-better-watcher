package betterwatcher

import (
	"encoding/json"

	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
)

func (factory WatcherFactory) storeToken(token *bson.Raw, redisKey string) error {
	tokenJSON, err := json.Marshal(*token)
	if err != nil {
		return err
	}
	setResult := factory.redisClient.Set(redisKey, tokenJSON, 0)
	return checkError(setResult.Err())
}

func (factory WatcherFactory) increaseRetries(token *bson.Raw) (int, error) {
	tokenJSON, err := json.Marshal(*token)
	if err != nil {
		return 0, err
	}
	incrResult := factory.redisClient.Incr(string(tokenJSON) + "::retries")

	if incrResult.Err() != nil {
		return 0, incrResult.Err()
	}
	currentRetries, err := incrResult.Result()
	if err != nil {
		return 0, err
	}
	return int(currentRetries), nil
}

func (factory WatcherFactory) getToken(redisKey string) (bson.Raw, error) {
	var resumeAfter bson.Raw
	val, err := factory.redisClient.Get(redisKey).Result()
	if err == nil {
		err = json.Unmarshal([]byte(val), &resumeAfter)
		if err != nil {
			return nil, err
		}
	}
	return resumeAfter, checkError(err)
}

func checkError(err error) error {
	if err != nil && err != nil && err != redis.Nil {
		return err
	}
	return nil
}
