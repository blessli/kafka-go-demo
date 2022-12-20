package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	topic       = kingpin.Flag("topic", "Topic name").Default("localtest").String()
	brokers     = kingpin.Flag("brokers", "REQUIRED: A comma separated list of broker addresses.").Default("localhost:9092").String()
	messageLoad = kingpin.Flag("message-load", "REQUIRED: The number of messages to produce to -topic.").Default("50000").Int()
	messageSize = kingpin.Flag("message-size", "REQUIRED: The approximate size (in bytes) of each message to produce to -topic.").Default("100").Int()
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Client      sarama.ConsumerGroup
	ctx         context.Context
	cancel      context.CancelFunc
	counter     int
	numMessages int
	Ready       chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Watch() {
	topics := strings.Split(*topic, ",")
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims.
		if err := consumer.Client.Consume(consumer.ctx, topics, consumer); err != nil {
			log.WithError(err).Panic("Error from consumer")
		}
		// Check if context was cancelled, signaling that the consumer should stop
		if consumer.ctx.Err() != nil {
			log.Info("Reached the number of consumed messages... exiting...")
			return
		}
		// consumer.Ready = make(chan bool)
	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// if consumer.counter >= consumer.numMessages {
		// 	log.Infof("Consumed %d messages successfully...", consumer.counter)
		// 	// consumer.cancel()
		// 	return nil
		// }
		consumer.counter++
		session.MarkMessage(message, "")
	}
	return nil
}

// NewConsumer returns a new sarama consumer.
func NewConsumer(brokers string, numMessages int) *Consumer {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(context.Background())
	group := fmt.Sprintf("group-%d", rand.Intn(10000))
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.WithError(err).Panic("Error creating consumer group client")
	}

	consumer := &Consumer{
		Client:      client,
		ctx:         ctx,
		cancel:      cancel,
		numMessages: numMessages,
		Ready:       make(chan bool),
	}
	return consumer
}

// PrepareConsume returns a function that can be used during the benchmark as it only
// performs the consuming of messages.
func PrepareConsume(consumer *Consumer) {
	log.Infof("Preparing to receive %d messages", consumer.numMessages)
	for i := 0; i < 3; i++ {
		consumer := NewConsumer(*brokers, *messageLoad)
		consumer.Watch()
	}
	// return func() {
	// 	wg := &sync.WaitGroup{}
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		for {
	// 			// `Consume` should be called inside an infinite loop, when a
	// 			// server-side rebalance happens, the consumer session will need to be
	// 			// recreated to get the new claims.
	// 			if err := consumer.Client.Consume(consumer.ctx, topics, consumer); err != nil {
	// 				log.WithError(err).Panic("Error from consumer")
	// 			}
	// 			// Check if context was cancelled, signaling that the consumer should stop
	// 			if consumer.ctx.Err() != nil {
	// 				log.Info("Reached the number of consumed messages... exiting...")
	// 				return
	// 			}
	// 			// consumer.Ready = make(chan bool)
	// 		}
	// 	}()

	// 	// <-consumer.Ready
	// 	wg.Wait()
	// }
}

func main() {
	kingpin.Parse()
	consumer := NewConsumer(*brokers, *messageLoad)
	PrepareConsume(consumer)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-signals:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	log.Println("done")
}
