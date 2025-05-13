package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/GiorgosMarga/swarmq/broker"
	"github.com/GiorgosMarga/swarmq/consumer"
	"github.com/GiorgosMarga/swarmq/producer"
)

func main() {
	var (
		m              string
		brokerAddr     string
		groupId        string
		topicId        string
		partitions     int
		producerValues int
	)
	flag.StringVar(&m, "mode", "broker", "broker | consumer | producer")
	flag.StringVar(&brokerAddr, "brokerAddr", ":4000", "Broker's Address")
	flag.StringVar(&groupId, "gId", "foo", "Group id for consumers")
	flag.StringVar(&topicId, "tId", "foo_group_id", "Topic id for both consumers and producers")
	flag.IntVar(&partitions, "partitions", 12, "Partitions of the given topic")
	flag.IntVar(&producerValues, "producerValues", 10, "Number of values that the producer will send")

	flag.Parse()
	fmt.Println("Swarmq")

	fmt.Println(m)
	switch m {
	case "broker":
		broker := broker.NewBroker(brokerAddr)
		broker.Start()
	case "producer":
		prd, err := producer.NewProducer(brokerAddr)
		if err != nil {
			log.Fatal(err)
		}

		if err := prd.CreateNewTopic(topicId, partitions); err != nil {
			fmt.Println(err)
		}

		time.Sleep(500 * time.Millisecond)
		for i := range producerValues {
			key := fmt.Sprintf("key_%d", i)
			val := fmt.Sprintf("val_%d", i)
			if err := prd.Pub(topicId, key, []byte(val)); err != nil {
				fmt.Println(err)
			}
		}
	case "consumer":
		c, err := consumer.NewConsumer(brokerAddr)
		if err != nil {
			log.Fatal(err)
		}
		if err := c.Join(groupId, []string{topicId}); err != nil {
			log.Fatal(err)
		}

		time.Sleep(100 * time.Millisecond)
		if err := c.Read(-1, -1, 1); err != nil {
			log.Fatal(err)
		}
	}

}
