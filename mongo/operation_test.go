package mongo

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	alphaBroker "github.com/AlphaMinZ/alpha_broker"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestInsertOne(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := tc.InsertOne(ctx, "alpha_app", "text", bson.D{{"name", "pi"}, {"value", 3.1415926}})
		if err != nil {
			fmt.Println(err)
		}
		id := res.InsertedID
		fmt.Println(id)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestInsertMany(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs := []interface{}{
			bson.D{{"name", "doc4"}, {"value", 1}},
			bson.D{{"name", "doc4"}, {"value", 2}},
			bson.D{{"name", "doc4"}, {"value", 3}},
		}
		res, err := tc.InsertMany(ctx, "alpha_app", "text", docs)
		if err != nil {
			t.Fatalf("InsertMany failed: %v", err)
		}

		// 验证插入操作是否成功
		if len(res.InsertedIDs) != len(docs) {
			t.Fatalf("Number of inserted documents doesn't match")
		}

		// 打印插入的文档 ID
		for i, id := range res.InsertedIDs {
			t.Logf("Inserted document %d ID: %v", i, id)
		}
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestFindOne(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 调用 FindOne 方法查找文档
		filter := bson.D{{"name", "doc4"}} // 设置查询条件
		result := tc.FindOne(ctx, "alpha_app", "text", filter)

		// 验证返回的结果是否符合预期
		if err := result.Err(); err != nil {
			t.Fatalf("FindOne failed: %v", err)
		}

		var doc bson.M
		if err := result.Decode(&doc); err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		delete(doc, "_id")

		expected := bson.M{"name": "doc2", "value": 2} // 预期的文档数据

		if !reflect.DeepEqual(doc, expected) {
			fmt.Printf("Result mismatch: expected %v, got %v", expected, doc)
		}

		t.Logf("Found document: %v", doc)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestFind(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 创建一个过滤条件
		filter := bson.D{{"name", "doc4"}}

		cursor, err := tc.Find(ctx, "alpha_app", "text", filter)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}
		defer cursor.Close(ctx)

		// 遍历游标，处理查询结果
		for cursor.Next(ctx) {
			var result bson.M
			if err := cursor.Decode(&result); err != nil {
				t.Fatalf("Error decoding document: %v", err)
			}
			fmt.Printf("Found document: %v\n", result)
		}

		// 检查游标遍历过程中是否发生错误
		if err := cursor.Err(); err != nil {
			t.Fatalf("Error iterating cursor: %v", err)
		}
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestFindWithOptio(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 创建一个过滤条件
		filter := bson.D{{"name", "doc4"}}

		// 定义查询选项
		findOptions := options.Find()

		// 设置查询选项，例如设置要返回的字段
		// findOptions.SetProjection(bson.M{"name": 1, "value": 1})
		// findOptions.SetLimit(2)
		// findOptions.SetSkip(2)
		// findOptions.SetSort(bson.D{{"value", -1}})	// 按照 value 字段的值排列输出。1升序，-1降序
		// findOptions.SetCollation(&options.Collation{Locale: "en", Strength: 1})
		findOptions.SetHint(bson.D{{"value", 1}}) // 提示使用 value 字段的索引

		cursor, err := tc.FindWithOption(ctx, "alpha_app", "text", filter, findOptions)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}
		defer cursor.Close(ctx)

		// 遍历游标，处理查询结果
		for cursor.Next(ctx) {
			var result bson.M
			if err := cursor.Decode(&result); err != nil {
				t.Fatalf("Error decoding document: %v", err)
			}
			fmt.Printf("Found document: %v\n", result)
		}

		// 检查游标遍历过程中是否发生错误
		if err := cursor.Err(); err != nil {
			t.Fatalf("Error iterating cursor: %v", err)
		}
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestDistinct(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 创建一个过滤条件
		filter := bson.D{{"value", bson.D{{"$gt", 1}}}}

		result, err := tc.Distinct(ctx, "alpha_app", "text", "name", filter)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}

		fmt.Println("Distinct values for field", "name", ":", result)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestUpdateOne(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 创建一个过滤条件
		filter := bson.D{{"name", "doc4"}}

		// 假设要更新的数据
		updateData := bson.D{{"$set", bson.D{{"value", 100}}}}

		result, err := tc.UpdateOne(ctx, "alpha_app", "text", filter, updateData)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}

		// 输出更新操作的结果
		fmt.Println("Matched Count:", result.MatchedCount)
		fmt.Println("Modified Count:", result.ModifiedCount)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestUpdateMany(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 创建一个过滤条件
		filter := bson.D{{"name", "doc4"}}

		// 假设要更新的数据
		updateData := bson.D{{"$set", bson.D{{"value", 100}}}}

		result, err := tc.UpdateMany(ctx, "alpha_app", "text", filter, updateData)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}

		// 输出更新操作的结果
		fmt.Println("Matched Count:", result.MatchedCount)
		fmt.Println("Modified Count:", result.ModifiedCount)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

func TestUpdateByID(t *testing.T) {
	ctx := context.Background()
	to := &testOwner{}
	tc := &Client{
		BaseComponent: alphaBroker.NewBaseComponent(),
		RealCli: NewClient(ctx, &Config{
			URI:         "mongodb://localhost:27017",
			MinPoolSize: 3,
			MaxPoolSize: 3000,
			Credential: options.Credential{
				Username: "alpha",
				Password: "883721",
			},
		}),
	}
	defer tc.RealCli.Disconnect(ctx)

	to.c = tc
	to.Launch()
	fn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		id, _ := primitive.ObjectIDFromHex("662136e93685460d9bcc76a4")

		// 假设要更新的数据
		// updateData := bson.D{{"$set", bson.D{{"name", "doc4"}, {"value", 1000}}}}
		updateData := bson.D{{"$set", bson.D{{"value", 1010}}}}

		result, err := tc.UpdateByID(ctx, "alpha_app", "text", id, updateData)

		if err != nil {
			t.Fatalf("Error executing find operation: %v", err)
		}

		// 输出更新操作的结果
		fmt.Println("Matched Count:", result.MatchedCount)
		fmt.Println("Modified Count:", result.ModifiedCount)
	}
	op := alphaBroker.Operation{
		Cb:  fn,
		Ret: make(chan interface{}),
	}
	to.c.Resolve(op)
	<-op.Ret // 接受结果信号
	fmt.Println("op success")
	time.Sleep(time.Second * 5)
	to.Stop()
}

// 目前还有点问题
// func TestUpdateOneWithSession(t *testing.T) {
// 	ctx := context.Background()
// 	to := &testOwner{}
// 	tc := &Client{
// 		BaseComponent: alphaBroker.NewBaseComponent(),
// 		RealCli: NewClient(ctx, &Config{
// 			URI:         "mongodb://localhost:27017",
// 			MinPoolSize: 3,
// 			MaxPoolSize: 3000,
// 			Credential: options.Credential{
// 				Username: "alpha",
// 				Password: "883721",
// 			},
// 		}),
// 	}
// 	defer tc.RealCli.Disconnect(ctx)

// 	to.c = tc
// 	to.Launch()
// 	fn := func() {
// 		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 		defer cancel()

// 		// 创建一个过滤条件
// 		filter := bson.D{{"name", "doc4"}}

// 		// 假设要更新的数据
// 		updateData := bson.D{{"$set", bson.D{{"value", 1000}}}}

// 		err := tc.UpdateOneWithSession(ctx, "alpha_app", "text", filter, updateData)

// 		if err != nil {
// 			t.Fatalf("Error executing find operation: %v", err)
// 		}
// 	}
// 	op := alphaBroker.Operation{
// 		Cb:  fn,
// 		Ret: make(chan interface{}),
// 	}
// 	to.c.Resolve(op)
// 	<-op.Ret // 接受结果信号
// 	fmt.Println("op success")
// 	time.Sleep(time.Second * 5)
// 	to.Stop()
// }
