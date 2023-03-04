package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Reader struct {
	redis *redis.Client
	ctx   context.Context
	RawType
}
type RawType struct {
	Item string    `json:"item"`
	Qty  int       `json:"qty"`
	Tags [2]string `json:"tags"`
	Uom  struct {
		H   int    `json:"h"`
		W   int    `json:"w"`
		Uom string `json:"uom"`
	}
}

func (raw RawType) MarshalBinary(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func ChannelName() string {
	return "data_aggr"
}

func init() {
	err := godotenv.Load("app.env")
	if err != nil {
		log.Fatalf("couldn't load env file %v", err)
	}
}

func main() {
	ctx := context.Background()
	red := &Reader{
		ctx: ctx,
	}
	url := os.Getenv("MONGO_URL")

	opt, err := redis.ParseURL("redis://default:redispw@localhost:55000")
	if err != nil {
		log.Fatalf("couldn't connect to redis: %v", err)
	}
	rdb := redis.NewClient(opt)
	red.redis = rdb

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		fmt.Errorf("couldn't connect to mongodb remote")
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	collection := client.Database("test_logs").Collection("exports")

	mu := sync.RWMutex{}
	opts := options.Find().SetLimit(76000).SetBatchSize(50)
	mu.RLock()
	defer mu.RUnlock()
	cur, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)
	go red.readFromRedis()
	for cur.Next(ctx) {
		var result bson.D
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		go red.sendToRedis(result.Map())
		time.Sleep(time.Second * 2)
	}
}

func (r *Reader) sendToRedis(data primitive.M) {
	d, err := r.MarshalBinary(data)
	if err != nil {
		log.Fatalf("couldn't publish data: %v", err)
	}
	fmt.Println("publishing to redis")
	err = r.redis.Publish(r.ctx, ChannelName(), d).Err()
	if err != nil {
		log.Fatalf("couldn't publish data: %v\n", err)
	}
}

func (r *Reader) readFromRedis() {
	fmt.Println("reading from redis")
	subs := r.redis.Subscribe(r.ctx, ChannelName())
	defer subs.Close()
	ch := subs.Channel()
	data := RawType{}
	for msg := range ch {
		if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
			log.Fatalf("couldn't unmarshal the data: %v", err)
		}

	}
}
