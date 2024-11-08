package com.nats.natsdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NatsSubscriber {
    public static void main(String[] args) {
        try {

            Options options = new Options.Builder()
                    .server("nats://localhost:4222")
                    .connectionTimeout(Duration.ofSeconds(5))  // Adjust the timeout as needed
                    .build();
            Connection natsConnection = Nats.connect(options);
            // Create a JetStream context
            JetStream js = natsConnection.jetStream();
            // Create a JetStreamManagement context to manage streams
            JetStreamManagement jsm = natsConnection.jetStreamManagement();
            // Stream and subject configuration
            String streamName = "mystream1";
            String[] subjects = {"subject1", "subject2", "subject3"};
            // Create stream configuration for persistent storage of messages
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)  // Unique stream name
                    .subjects(subjects)  // Subject (topic) the stream listens to
                    .maxMessages(3)
                    .storageType(StorageType.File)  // File-based persistent storage
                    .build();

            // Create the stream (if it doesn't exist already)
            StreamInfo streamInfo;
            if(jsm.getStreamInfo(streamName) != null){
                streamInfo=  jsm.updateStream(streamConfig);
            }
            else {
                 streamInfo = jsm.addStream(streamConfig);
            }
            System.out.println("Stream created or already exists: " + streamInfo.getConfiguration().getName());

            new Thread(() -> {
                // Create a ConsumerConfiguration for the durable subscriber
                ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                        .durable("my_durable")  // Durable name ensures missed messages are saved for the subscriber
                        .ackWait(Duration.ofSeconds(30))  // Set acknowledgment wait time for consumers
                        .build();
                ConsumerConfiguration consumerConfig1 = ConsumerConfiguration.builder()
                        .durable("my_durable1")  // Durable name ensures missed messages are saved for the subscriber
                        .ackWait(Duration.ofSeconds(30))  // Set acknowledgment wait time for consumers
                        .build();
                ConsumerConfiguration consumerConfig2 = ConsumerConfiguration.builder()
                        .durable("my_durable2")  // Durable name ensures missed messages are saved for the subscriber
                        .ackWait(Duration.ofSeconds(30))  // Set acknowledgment wait time for consumers
                        .build();


                // Create PushSubscribeOptions with the durable consumer configuration
                PushSubscribeOptions pushSubscribeOptions = PushSubscribeOptions.builder()
                        .configuration(consumerConfig)
                        .build();
                PushSubscribeOptions pushSubscribeOptions1 = PushSubscribeOptions.builder()
                        .configuration(consumerConfig1)
                        .build();
                PushSubscribeOptions pushSubscribeOptions2 = PushSubscribeOptions.builder()
                        .configuration(consumerConfig2)
                        .build();

                // Now subscribe to the JetStream stream with durable options
                JetStreamSubscription subscription1 = null;
                JetStreamSubscription subscription2 = null;
                JetStreamSubscription subscription3 = null;
                try {
                     subscription1 = js.subscribe("subject1", pushSubscribeOptions);
                     subscription2 = js.subscribe("subject2", pushSubscribeOptions1);
                     subscription3 = js.subscribe("subject3", pushSubscribeOptions2);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (JetStreamApiException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("Subscriber connected and ready to receive messages...");


                // Fetch and process messages
                while (true) {
                    Message m1 = null;  // Wait for up to 5 seconds for a message
                    Message m2 = null;
                    Message m3 = null;
                    try {
                        m1 = subscription1.nextMessage(Duration.ofSeconds(2));
                        m2 = subscription2.nextMessage(Duration.ofSeconds(2));
                        m3 = subscription3.nextMessage(Duration.ofSeconds(2));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (m1 != null) {
                        String msgData = new String(m1.getData(), StandardCharsets.UTF_8);
                      //  System.out.println("Received message: " + msgData);
                        m1.ack();  // Acknowledge the message to ensure it is not redelivered
                        natsConnection.publish("subject1_ack", msgData.getBytes());
                        System.out.println(m1.metaData().streamSequence());
//                        natsConnection.publish("subject1_ack", msgData.getBytes());
                        createPojoOfJson(msgData);
                    }
                    if (m2 != null) {
                        String msgData = new String(m2.getData(), StandardCharsets.UTF_8);
                       // System.out.println("Received message: " + msgData);
                        m2.ack();  // Acknowledge the message to ensure it is not redelivered
                        natsConnection.publish("subject1_ack", msgData.getBytes());
                        System.out.println(m2.metaData().streamSequence());
//                        natsConnection.publish("subject2_ack", msgData.getBytes());
                        createPojoOfJson(msgData);
                    }
                    if (m3 != null) {
                        String msgData = new String(m3.getData(), StandardCharsets.UTF_8);
                      //  System.out.println("Received message: " + msgData);
                        m3.ack();  // Acknowledge the message to ensure it is not redelivered
                        natsConnection.publish("subject1_ack", msgData.getBytes());
                        System.out.println(m3.metaData().streamSequence());
//                        natsConnection.publish("subject3_ack", msgData.getBytes());
                        createPojoOfJson(msgData);
                    }
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void createPojoOfJson(String msg){
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            Gateway gateway = objectMapper.readValue(msg, Gateway.class);
            String jsonString = objectMapper.writeValueAsString(gateway);
            System.out.println(jsonString);
//            System.out.println("Gateway: " + gateway.toString());
            System.out.println("interface: " + gateway.getInterfaces());
            System.out.println(gateway.getInterfaces().getLan().get(0).getDnsServers().get(0));
            try {
                // Convert the POJO to a JSON string
                String json = objectMapper.writeValueAsString(gateway);
                System.out.println(jsonString);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            gateway.toString();
            // Print other properties as needed
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}



/*

            for (String subject : topics) {
                Subscription subscription = natsConnection.subscribe(subject);

                new Thread(() -> {
                    while (true) {
                        try {
                            Message message = subscription.nextMessage(1);
                            if (message != null) {
                                String msgData = new String(message.getData());
                                System.out.println("Received message on " + subject + ": " + msgData);
                                // Acknowledge the message (send acknowledgment)
                                natsConnection.publish(message.getReplyTo(), msgData.getBytes());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
*/

/*

//            for (String subject : topics) {
//                Subscription subscription = natsConnection.subscribe(subject);
//                System.out.println("Subscribed to subject: " + subject);
//
//                // Optionally, listen for messages
//                Message message = subscription.nextMessage(Duration.ofSeconds(10000));
//                if (message != null) {
//                    System.out.println("Received message on " + subject + ": " + new String(message.getData()));
//                    if (message.getReplyTo() != null) {
//                        String ackMessage = "Acknowledged from: " + subject;
//                        natsConnection.publish(message.getReplyTo(), ackMessage.getBytes());
//                        System.out.println("Sent acknowledgment to: " + subject);
//                    } else {
//                        System.out.println("No reply-to subject for acknowledgment.");
//                    }
//                }
//            }



            // Step 3: Receive a message
            //Message receivedMessage = subscription.nextMessage(100000);
            if (receivedMessage != null) {
                String receivedText = new String(receivedMessage.getData());
                System.out.println("Received message: " + receivedText);

                // Step 4: Send acknowledgment if reply subject exists
                if (receivedMessage.getReplyTo() != null) {
                    String ackMessage = "Acknowledged from: " + subject;
                    natsConnection.publish(receivedMessage.getReplyTo(), ackMessage.getBytes());
                    System.out.println("Sent acknowledgment to: " + subject);
                } else {
                    System.out.println("No reply-to subject for acknowledgment.");
                }
            } else {
                System.out.println("No message received within the timeout.");
            }

 */
//            Message receivedMessage = subscription.nextMessage(10000000);
//            if (receivedMessage != null) {
//                String receivedText = new String(receivedMessage.getData());
//                System.out.println("Received message: " + receivedText);
//            } else {
//                System.out.println("No message received within the timeout.");
//            }

// Step 4: Close the connection
//            natsConnection.close();
//System.out.println("Subscriber connection closed.");