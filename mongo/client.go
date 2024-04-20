package mongo

import (
	"context"

	alphaBroker "github.com/AlphaMinZ/alpha_broker"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client struct {
	*alphaBroker.BaseComponent
	RealCli *mongo.Client
}

func NewClient(ctx context.Context, config *Config) *mongo.Client {
	opt := options.Client().ApplyURI(config.URI).SetAuth(config.Credential)
	opt.SetMinPoolSize(config.MinPoolSize).SetMaxPoolSize(config.MaxPoolSize)
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		panic(err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}
	return client
}
