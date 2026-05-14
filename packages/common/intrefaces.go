package common

import "github.com/redis/go-redis/v9"

type Redis struct {
	*redis.Client
}

