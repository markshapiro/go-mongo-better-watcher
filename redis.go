package betterwatcher

import (
	"sync"

	"github.com/go-redis/redis"
)

var (
	once     sync.Once
	instance *redis.Client
)

func GetRedisInstance(url string) *redis.Client {
	once.Do(func() {
		if instance == nil {
			instance = redis.NewClient(&redis.Options{
				Addr:     url,
				Password: "",
				DB:       0,
			})
		}
	})
	return instance
}
