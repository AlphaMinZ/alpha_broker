package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

	alphaBroker "github.com/AlphaMinZ/alpha_broker"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type testOwner struct {
	c *Client
}

func (t testOwner) Launch() {
	t.c.Launch()
}

func (t testOwner) Stop() {
	t.c.Stop()
}

func TestClient(t *testing.T) {
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
