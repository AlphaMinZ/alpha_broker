package mongo

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeStream define what to watch? client, database or collection
type ChangeStream struct {
	collection string
	database   string
	pipeline   []bson.D
}

type callback func(bson.M)

// SetCollection se collection
func (cs *ChangeStream) SetCollection(collection string) {
	cs.collection = collection
}

// SetDatabase set database
func (cs *ChangeStream) SetDatabase(database string) {
	cs.database = database
}

// SetPipeline set pipeline
func (cs *ChangeStream) SetPipeline(pipeline []bson.D) {
	cs.pipeline = pipeline
}

// NewChangeStream get a new ChangeStream
func NewChangeStream() *ChangeStream {
	return &ChangeStream{}
}

// Watch print oplog in JSON format
func (cs *ChangeStream) Watch(client *mongo.Client, cb callback) {
	var err error
	var ctx = context.Background()
	var cur *mongo.ChangeStream
	fmt.Println("pipeline", cs.pipeline)
	opts := options.ChangeStream()
	opts.SetFullDocument("updateLookup")
	if cs.collection != "" && cs.database != "" {
		fmt.Println("Watching", cs.database+"."+cs.collection)
		var coll = client.Database(cs.database).Collection(cs.collection)
		if cur, err = coll.Watch(ctx, cs.pipeline, opts); err != nil {
			panic(err)
		}
	} else if cs.database != "" {
		fmt.Println("Watching", cs.database)
		var db = client.Database(cs.database)
		if cur, err = db.Watch(ctx, cs.pipeline, opts); err != nil {
			panic(err)
		}
	} else {
		fmt.Println("Watching all")
		if cur, err = client.Watch(ctx, cs.pipeline, opts); err != nil {
			panic(err)
		}
	}

	defer cur.Close(ctx)
	var doc bson.M
	for cur.Next(ctx) {
		if err = cur.Decode(&doc); err != nil {
			log.Fatal(err)
		}
		cb(doc)
	}
	if err = cur.Err(); err != nil {
		log.Fatal(err)
	}
}
