package mongo

import (
	"bufio"
	"bytes"
	"context"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
*	这个方法的作用是执行一个MongoDB的聚合操作，根据指定的管道对集合中的文档进行聚合处理，
*   并返回聚合结果的游标对象。调用者可以通过游标对象迭代获取聚合结果集合中的文档。
 */
func (c *Client) Aggregate(ctx context.Context, dbName, collName string, pipeline mongo.Pipeline) (*mongo.Cursor, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	cursor, err := collection.Aggregate(ctx, pipeline)
	return cursor, err
}

func (c *Client) InsertOne(ctx context.Context, dbName, collName string, data interface{}) (*mongo.InsertOneResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	res, err := collection.InsertOne(ctx, data)
	return res, err
}

func (c *Client) InsertMany(ctx context.Context, dbName, collName string, data []interface{}) (*mongo.InsertManyResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	res, err := collection.InsertMany(ctx, data)
	return res, err
}

func (c *Client) FindOne(ctx context.Context, dbName, collName string, filter interface{}) *mongo.SingleResult {
	collection := c.RealCli.Database(dbName).Collection(collName)

	return collection.FindOne(ctx, filter)
}

func (c *Client) Find(ctx context.Context, dbName, collName string, filter interface{}) (*mongo.Cursor, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	return collection.Find(ctx, filter)
}

/*
*	这个方法的作用是执行带有选项的查询操作，并返回查询结果的游标对象。
*	通过查询选项，可以对查询的行为进行定制，例如设置排序、限制返回字段等。
 */
func (c *Client) FindWithOption(ctx context.Context, dbName, collName string, filter interface{},
	findOptions *options.FindOptions) (*mongo.Cursor, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	return collection.Find(ctx, filter, findOptions)
}

func (c *Client) Distinct(ctx context.Context, dbName, collName string, fieldName string, filter interface{}) ([]interface{}, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	return collection.Distinct(ctx, fieldName, filter)
}

// UpdateOne 该方法用于更新集合中符合筛选条件的第一个文档。
func (c *Client) UpdateOne(ctx context.Context, dbName, collName string, filter interface{}, data interface{}) (*mongo.UpdateResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	return collection.UpdateOne(ctx, filter, data)
}

// UpdateMany 该方法用于更新集合中符合筛选条件的所有文档。
func (c *Client) UpdateMany(ctx context.Context, dbName, collName string, filter interface{}, data interface{}) (*mongo.UpdateResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	return collection.UpdateMany(ctx, filter, data)
}

// UpdateByID 该方法用于根据文档的 _id 字段更新集合中的特定文档。
func (c *Client) UpdateByID(ctx context.Context, dbName, collName string, id interface{}, data interface{}) (*mongo.UpdateResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	return collection.UpdateByID(ctx, id, data)
}

// UpdateOneWithSession 该方法用于在一个会话中执行更新集合中符合筛选条件的第一个文档，具有强一致性。
func (c *Client) UpdateOneWithSession(ctx context.Context, dbName, collName string, filter interface{}, data interface{}) error {
	collection := c.RealCli.Database(dbName).Collection(collName)

	var (
		session mongo.Session
		err     error
	)
	session, err = c.RealCli.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}
	f := func(sessionContext mongo.SessionContext) error {
		_, err = collection.UpdateOne(sessionContext, filter, data)
		if err != nil {
			return err
		}
		err = session.CommitTransaction(sessionContext)
		if err != nil {
			return err
		}
		return nil
	}
	err = mongo.WithSession(ctx, session, f)
	if err != nil {
		return err
	}
	session.EndSession(ctx)
	return nil
}

func (c *Client) UpdateManyWithSession(ctx context.Context, dbName, collName string, filter interface{}, data interface{}) error {
	collection := c.RealCli.Database(dbName).Collection(collName)

	var (
		session mongo.Session
		err     error
	)
	session, err = c.RealCli.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}
	f := func(sessionContext mongo.SessionContext) error {
		_, err = collection.UpdateMany(sessionContext, filter, data)
		if err != nil {
			return err
		}
		err = session.CommitTransaction(sessionContext)
		if err != nil {
			return err
		}
		return nil
	}
	err = mongo.WithSession(ctx, session, f)
	if err != nil {
		return err
	}
	session.EndSession(ctx)
	return nil
}

func (c *Client) UpdateByIDWithSession(ctx context.Context, dbName, collName string, id interface{}, data interface{}) error {
	collection := c.RealCli.Database(dbName).Collection(collName)

	var (
		session mongo.Session
		err     error
	)
	session, err = c.RealCli.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}
	f := func(sessionContext mongo.SessionContext) error {
		_, err = collection.UpdateByID(sessionContext, id, data)
		if err != nil {
			return err
		}
		err = session.CommitTransaction(sessionContext)
		if err != nil {
			return err
		}
		return nil
	}
	err = mongo.WithSession(ctx, session, f)
	if err != nil {
		return err
	}
	session.EndSession(ctx)
	return nil
}

// ReplaceOne 该方法用于在集合中替换（Replace）符合筛选条件的第一个文档。
func (c *Client) ReplaceOne(ctx context.Context, dbName, collName string, filter interface{}, replacement interface{}) (*mongo.UpdateResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	result, err := collection.ReplaceOne(ctx, filter, replacement)

	return result, err
}

func (c *Client) DeleteOne(ctx context.Context, dbName, collName string, filter interface{}) (*mongo.DeleteResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	return collection.DeleteOne(ctx, filter)
}

func (c *Client) DeleteMany(ctx context.Context, dbName, collName string, filter interface{}) (*mongo.DeleteResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	return collection.DeleteMany(ctx, filter)
}

func (c *Client) Count(ctx context.Context, dbName, collName string, filter interface{}) (int64, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	return collection.CountDocuments(ctx, filter)
}

func (c *Client) ChangeStreamClient(client *mongo.Client, coll *mongo.Collection) {
	//
}

func (c *Client) ChangeStreamCollection(client *mongo.Client, coll *mongo.Collection) {
	//
}

func (c *Client) ChangeStreamDB(client *mongo.Client, coll *mongo.Collection) {
	//
}

// UploadGridFS 将数据上传到 MongoDB 的 GridFS 中。
// bucketOptions：GridFS 存储桶的选项，用于配置存储桶的行为，例如指定文件块大小、元数据等。
func (c *Client) UploadGridFS(ctx context.Context, filename string, data interface{}, db *mongo.Database, bucketOptions *options.BucketOptions) error {
	bucket, err := gridfs.NewBucket(db, bucketOptions)
	if err != nil {
		return err
	}
	opts := options.GridFSUpload()
	//opts.SetMetadata(bsonx.Doc{{Key: "content-type", Value: bsonx.String("application/json")}})
	var upLoadStream *gridfs.UploadStream
	if upLoadStream, err = bucket.OpenUploadStream(filename, opts); err != nil {
		return err
	}
	str, err := jsoniter.MarshalToString(data)
	if err != nil {
		return err
	}
	if _, err = upLoadStream.Write([]byte(str)); err != nil {
		return err
	}
	upLoadStream.Close()
	return nil
}

// DownLoadGridFS 从 GridFS 中下载文件。
// 返回的是一个字符串类型
func (c *Client) DownLoadGridFS(ctx context.Context, fileID interface{}, db *mongo.Database, bucketOptions *options.BucketOptions) (string, error) {
	bucket, err := gridfs.NewBucket(db, bucketOptions)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	if _, err = bucket.DownloadToStream(fileID, w); err != nil {
		return "", err
	}
	return b.String(), err
}

// EstimatedDocumentCount You can get an approximation on the number of documents in a collection
func (c *Client) EstimatedDocumentCount(ctx context.Context, dbName, collName string) (int64, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	estCount, estCountErr := collection.EstimatedDocumentCount(ctx)
	return estCount, estCountErr
}

// CountDocuments You can get an exact number of documents in a collection
func (c *Client) CountDocuments(ctx context.Context, dbName, collName string, filter interface{}) (int64, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	estCount, estCountErr := collection.CountDocuments(ctx, filter)
	return estCount, estCountErr
}

// RunCommand 在指定的数据库上运行一个命令，并返回结果。
func (c *Client) RunCommand(ctx context.Context, dbName string, command interface{}) (bson.M, error) {
	db := c.RealCli.Database(dbName)
	var result bson.M
	err := db.RunCommand(ctx, command).Decode(&result)
	return result, err
}

// BulkWrite 执行批量写入操作。
func (c *Client) BulkWrite(ctx context.Context, dbName, collName string, models []mongo.WriteModel, opts *options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)
	results, err := collection.BulkWrite(ctx, models, opts)
	return results, err
}

func (c *Client) CreateIndex(ctx context.Context, dbName, collName string, indexModel mongo.IndexModel) (string, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	name, err := collection.Indexes().CreateOne(ctx, indexModel)
	return name, err
}

func (c *Client) DropIndex(ctx context.Context, dbName, collName string, indexName string) (bson.Raw, error) {
	collection := c.RealCli.Database(dbName).Collection(collName)

	res, err := collection.Indexes().DropOne(ctx, indexName)
	return res, err
}
