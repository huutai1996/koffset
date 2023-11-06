package main

import (
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

func connectKafka(brokersurl []string, username string, password string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	if username != "" && password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
	}
	conn, err := sarama.NewConsumer(brokersurl, config)
	return conn, err
}
func main() {
	// specific topic and partiton
	brokers := flag.String("b", "", "bootstrap-server to connect to kafka")
	topic := flag.String("t", "", "topic name")
	offset := flag.Int64("offset", sarama.OffsetOldest, "Kafka offset (e.g., OffsetOldest or OffsetNewest)")
	p := flag.String("p", "all", "partition consume")
	username := flag.String("username", "", "username for authentication kafka")
	password := flag.String("password", "", "password for authentication kafka")
	flag.Parse()
	fmt.Println(os.Args)
	if len(os.Args) < 5 {
		fmt.Println("Please providing bootstrap server with -b  and topic with -t")
		return
	}
	if *topic == "" || *brokers == "" {
		fmt.Println("Please providing topic and broker ")
		return
	}
	brokerlist := strings.Split(*brokers, ",")
	// Create new kafka consumer
	consumer, err := connectKafka(brokerlist, *username, *password)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	// Create a signal to gracefull shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	fmt.Println("Starting consume message from kafka: ")
	// List Partition of topic
	var listpartitions []int32
	if *p == "all" {
		listpartitions, err = consumer.Partitions(*topic)
		if err != nil {
			panic(err)
		}
	} else {
		listpartitiontemp, err := strconv.Atoi(*p)
		if err != nil {
			panic(err)
		}
		listpartitions = append(listpartitions, int32(listpartitiontemp))
	}
	// consume message from all partition
	for _, partition := range listpartitions {
		partitionConsumer, err := consumer.ConsumePartition(*topic, partition, *offset)
		if err != nil {
			panic(err)
		}
		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case msg := <-pc.Messages():
					fmt.Printf("Partiton: %d, Offset: %d, Message: %s\n", msg.Partition, msg.Offset, string(msg.Value))
				case <-signals:
					return
				}

			}

		}(partitionConsumer)
	}
	select {
	case <-signals:
		return
	}
}
