package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	UPPER_LIMIT = 1000000
	CHUNK_SIZE  = 1000
)

type Inserter struct {
	counter int
	client  *mongo.Collection
	redis   *redis.Client
	mu      sync.RWMutex
	ctx     context.Context
}

func New() *Inserter {
	return &Inserter{
		counter: 0,
	}
}

func init() {
	err := godotenv.Load("app.env")
	if err != nil {
		log.Fatalf("couldn't load env file %v", err)
	}
}

func (in *Inserter) looper(ctx context.Context, col *mongo.Collection) {
	for {

		i := in.randomInt(100000000, in.randomInt(100000, 1))
		bson := bson.D{
			{"item", "journal"},
			{"qty", int32(i)},
			{"tags", bson.A{"blank", "red"}},
			{"size", bson.D{
				{"h", i},
				{"w", i},
				{"uom", "cm"},
			}},
		}

		in.mu.Lock()
		in.counter++
		res, err := col.InsertOne(ctx, bson, nil)
		if err != nil {
			fmt.Printf("couldn't write data to store %v\n", err)
			continue
		}
		in.mu.Unlock()
		fmt.Printf("written data is -> %v\n", res.InsertedID)
		fmt.Printf("<<<--- %d -->>>>\n", in.counter)
	}
}

func client() (*mongo.Client, context.Context, error) {
	url := os.Getenv("MONGO_URL")

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't connect to mongodb remote")
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	return client, ctx, nil
}

func (in *Inserter) randomInt(max, min int) int {
	return rand.Intn(max-min) + max
}

func main() {
	in := New()
	url := os.Getenv("MONGO_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	// ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		fmt.Errorf("couldn't connect to mongodb remote")
	}
	opt, err := redis.ParseURL("redis://default:redispw@localhost:55000")
	if err != nil {
		log.Fatalf("couldn't connect to redis: %v", err)
	}
	rdb := redis.NewClient(opt)
	in.redis = rdb
	in.ctx = ctx

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	client.Database("test_logs").CreateCollection(ctx, "exports", nil)
	collection := client.Database("test_logs").Collection("exports")
	in.client = collection
	for i := 0; i < 10; i++ {
		go in.looper(ctx, collection)
	}
	in.looper(ctx, collection)
}
