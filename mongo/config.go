package mongo

import "go.mongodb.org/mongo-driver/mongo/options"

type Config struct {
	URI                      string
	MinPoolSize, MaxPoolSize uint64
	Credential               options.Credential
}
