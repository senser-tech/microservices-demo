// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/frontend/genproto"
	"github.com/segmentio/kafka-go"
)

const (
	ENV_CLUSTERID    = "CLUSTERID"
	KAFKA_BROKER1    = "KAFKA_BROKER1"
	KAFKA_BROKER2    = "KAFKA_BROKER2"
	MY_POD_NAMESPACE = "MY_POD_NAMESPACE"

	broker1AddressDefault = "broker1.senser.tech:30718"
	broker2AddressDefault = "broker2.senser.tech:30719"
)

type SenserConsumer struct {
	broker1Address string
	broker2Address string
	clusterID      string

	ctx context.Context

	topic1 string
	topic2 string
	topic3 string

	reader1 *kafka.Reader

	msgCh chan string

	feSvc *frontendServer
}

var globalSenserKafka *SenserConsumer
var StopFrontEnd bool

func InitSenserKafkaConsumer(feSvc *frontendServer) {
	globalSenserKafka = &SenserConsumer{
		ctx:   context.Background(),
		feSvc: feSvc,
	}
	globalSenserKafka.setConnParams()
	globalSenserKafka.initReaders()

	globalSenserKafka.msgCh = make(chan string)
	c := globalSenserKafka
	go func() {
		r := c.reader1
		for {
			ret, err := r.ReadMessage(c.ctx)
			fmt.Printf("Got Kafka Message, %v. err: %v\n", ret, err)
			if err == nil {
				c.msgCh <- string(ret.Value)
			} else {
				c.msgCh <- ""
			}
		}
	}()

	go globalSenserKafka.readMessages()
}

func (c *SenserConsumer) setConnParams() {
	c.broker1Address = os.Getenv(KAFKA_BROKER1)
	if c.broker1Address == "" {
		c.broker1Address = broker1AddressDefault
	}

	c.broker2Address = os.Getenv(KAFKA_BROKER2)
	if c.broker2Address == "" {
		c.broker2Address = broker2AddressDefault
	}

	c.clusterID = os.Getenv(ENV_CLUSTERID)
	if c.clusterID == "" {
		c.clusterID = "karol_mac_cluster"
	}

	c.topic1 = c.clusterID + "-topic1"

	fmt.Printf("Senser Init: broker1Address=%s\n", c.broker1Address)
	fmt.Printf("Senser Init: broker2Address=%s\n", c.broker2Address)
	fmt.Printf("Senser Init: clusterID=%s\n", c.clusterID)

	fmt.Printf("Senser Init: topic1=%s\n", c.topic1)

	fmt.Printf("Senser Init: ConsumerGroupId=%s\n", os.Getenv(MY_POD_NAMESPACE))
}

func (c *SenserConsumer) initReaders() {
	namespace := os.Getenv(MY_POD_NAMESPACE)

	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	c.reader1 = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{c.broker1Address, c.broker2Address},
		Topic:       c.topic1,
		StartOffset: kafka.LastOffset,
		GroupID:     namespace,
		// MaxWait:     1 * time.Second,
	})
}

func (c *SenserConsumer) readMessages() (string, error) {
	// Read will read a batch of messages, so to ensure "fetch" commands we always grab the latest messages
	// the `readMessage` method blocks until we receive the next event
	locked := false

	var result string
	for {
		select {
		case result = <-c.msgCh:

		case <-time.After(1 * time.Second):
			result = ""
		}

		if result == "" {
			if !locked {
				fmt.Println("HTTP control: locked")
				StopFrontEnd = true
				locked = true
			}
		} else {
			if locked {
				fmt.Println("HTTP control: unlocked")
				StopFrontEnd = false
				locked = false

			}
			// Call Api for senser tests of Api chain calls to be related to the Kafka messages
			_, err := c.feSvc.getShippingQuote(c.ctx, []*pb.CartItem{}, defaultCurrency)
			if err != nil {
				fmt.Printf("failed to get shipping quote from kafka handler. Err: %v", err)
			}
		}
	}

}
