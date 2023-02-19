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
	reader2 *kafka.Reader
	reader3 *kafka.Reader
}

var globalSenserKafka *SenserConsumer

func InitSenserKafkaConsumer() {
	globalSenserKafka = &SenserConsumer{
		ctx: context.Background(),
	}
	globalSenserKafka.setConnParams()
	globalSenserKafka.initReaders()
}

func GetCurrencyDataFromKafka() (string, error) {
	response := ""
	c := globalSenserKafka

	ret, err := c.readMessage(c.reader1)
	if err != nil {
		return "", err
	}
	response += ret

	ret, err = c.readMessage(c.reader2)
	if err != nil {
		return "", err
	}
	response += "|" + ret

	ret, err = c.readMessage(c.reader3)
	if err != nil {
		return "", err
	}
	response += "|" + ret

	return response, nil
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
	c.topic2 = c.clusterID + "-topic2"
	c.topic3 = c.clusterID + "-topic3"

	fmt.Printf("Senser Init: broker1Address=%s\n", c.broker1Address)
	fmt.Printf("Senser Init: broker2Address=%s\n", c.broker2Address)
	fmt.Printf("Senser Init: clusterID=%s\n", c.clusterID)

	fmt.Printf("Senser Init: topic1=%s\n", c.topic1)
	fmt.Printf("Senser Init: topic2=%s\n", c.topic2)
	fmt.Printf("Senser Init: topic3=%s\n", c.topic3)

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
	})

	c.reader2 = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{c.broker1Address, c.broker2Address},
		Topic:       c.topic2,
		StartOffset: kafka.LastOffset,
		GroupID:     namespace,
	})

	c.reader3 = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{c.broker1Address, c.broker2Address},
		Topic:       c.topic3,
		StartOffset: kafka.LastOffset,
		GroupID:     namespace,
	})
}

func (c *SenserConsumer) readMessage(r *kafka.Reader) (string, error) {
	// Read will read a batch of messages, so to ensure "fetch" commands we always grab the latest messages
	r.SetOffsetAt(c.ctx, time.Now().Add(-10*time.Second))

	// the `readMessage` method blocks until we receive the next event
	msg, err := r.ReadMessage(c.ctx)
	if err != nil {
		fmt.Printf("Error: could not read message " + err.Error())
		time.Sleep(time.Second * 5)
		return "", err
	}
	return string(msg.Value), nil
}
