package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	producer sarama.SyncProducer
)

func sub(client mqtt.Client, mqttTopic string) {
	defer client.Disconnect(250)
	topic := mqttTopic
	token := client.Subscribe(topic, 1, nil)
	fmt.Printf("Subscribed to topic: %s", topic)
	for {
		token.Wait()
	}
}

func initKafkaProducer(brokers []string) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	var err error
	fmt.Printf("kafka brokers %v \n", brokers)
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Printf("init producer failed -> %v \n", err)
		panic(err)
	}
	fmt.Println("producer init success")
}

func dumpString(v interface{}) (str string) {

	bs, err := json.Marshal(v)
	b := bytes.Buffer{}
	if err != nil {
		b.WriteString("{err:\"json format error.")
		b.WriteString(err.Error())
		b.WriteString("\"}")
	} else {
		b.Write(bs)
	}
	str = b.String()
	return str
}

func produceMsg(topic string, msg []byte) {
	msgX := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	fmt.Printf("SendMsg -> %v\n", dumpString(msgX))

	partition, offset, err := producer.SendMessage(msgX)
	if err != nil {
		fmt.Printf("send msg error:%s \n", err)
		return
	}
	fmt.Printf("msg send success, message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}

func main() {
	broker := flag.String("broker", "localhost", "mqtt broker server list, seperated by ',' ")
	port := flag.Int("port", 1883, "mqtt broker port")
	username := flag.String("user", "", "user name")
	pwd := flag.String("pwd", "", "password")
	clientId := flag.String("clientid", "go-mqtt-kafka-connector", "client id")
	kafka := flag.String("kafka", "localhost:9092", "kafka server list, seperated by ',' ")
	topic := flag.String("topic", "test", "kafka topic to connect")
	mqttTopic := flag.String("mqtt_topic", "test/#", "mqtt topic to connect")
	flag.Parse()

	brokers := strings.Split(*broker, ",")

	opts := mqtt.NewClientOptions()
	for _, brk := range brokers {
		opts.AddBroker(fmt.Sprintf("tcp://%s:%d", brk, *port))
	}
	opts.SetClientID(*clientId)
	if *username != "" {
		opts.SetUsername(*username)
		opts.SetPassword(*pwd)
	}

	messagePubHandler := func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
		produceMsg(*topic, msg.Payload())
	}

	connectHandler := func(client mqtt.Client) {
		fmt.Println("Connected")
	}

	var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Connect lost: %v", err)
	}
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	initKafkaProducer(strings.Split(*kafka, ","))
	sub(client, *mqttTopic)
}
