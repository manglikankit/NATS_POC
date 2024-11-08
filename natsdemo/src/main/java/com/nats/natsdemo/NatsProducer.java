package com.nats.natsdemo;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class NatsProducer {
    public static int counter = 0;
    public static void main(String[] args) {
        myMethod();
    }

    public static void myMethod() {
        try {
            //docker run -p 4222:4222 -ti nats:latest
            // Step 1: Connect to NATS server
            Options options = new Options.Builder()
                    .server("nats://localhost:4222")
                    .connectionTimeout(Duration.ofSeconds(5))  // Adjust the timeout as needed
                    .build();
            // Step 1: Connect to NATS server
            String natsUrl = "nats://localhost:4222";
            Connection natsConnection = Nats.connect(options);
            System.out.println("Subscriber connected to NATS!");
            // Create a JetStream context
            JetStream jetStream = natsConnection.jetStream();
            // Create a JetStreamManagement context to manage streams
            JetStreamManagement jsm = natsConnection.jetStreamManagement();
            // Stream and subject configuration
            String streamName = "mystream1";
            String subject = "subject1";
            // Create stream configuration for persistent storage of messages
            StreamConfiguration streamConfig ;
            if (jsm.getStreamInfo(streamName) == null) {
                streamConfig = StreamConfiguration.builder()
                        .name(streamName)
                        .subjects(subject)
                        .maxMessages(2)
                        .storageType(StorageType.File)
                        .build();
                jsm.addStream(streamConfig);
            }

            String[] subjects = {"subject1", "subject2", "subject3"};

            // Publish a message to the JetStream
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {

                        publishMsg("subject1", natsConnection);
                        Thread.sleep(1); // Simulating a time-consuming task (2 seconds)
//                        publishMsg("subject2", natsConnection);
//                        Thread.sleep(1000); // Simulating a time-consuming task (2 seconds)
//                        publishMsg("subject3", natsConnection);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });


            Subscription ackSubscription1 = natsConnection.subscribe("subject1_ack");
//            Subscription ackSubscription2 = natsConnection.subscribe("subject2_ack");
//            Subscription ackSubscription3 = natsConnection.subscribe("subject3_ack");
// Receive acknowledgments in a loop
            while (true) {
                Message ackMessage1 = ackSubscription1.nextMessage(Duration.ofSeconds(5));
                if (ackMessage1 != null) {
                    System.out.println("Received acknowledgment of subject1: " + new String(ackMessage1.getData()));
                }
//                Message ackMessage2 = ackSubscription2.nextMessage(Duration.ofSeconds(5));
//                if (ackMessage2 != null) {
//                    System.out.println("Received acknowledgment of subject2: " + new String(ackMessage2.getData()));
//                }
//                Message ackMessage3 = ackSubscription3.nextMessage(Duration.ofSeconds(5));
//                if (ackMessage3 != null) {
//                    System.out.println("Received acknowledgment of subject3: " + new String(ackMessage3.getData()));
//                }
            }

        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    static void publishMsg(String topic, Connection natsConnection){
        /*
        String localTime = LocalTime.now().toString();
            String message = String.format("""
                    {
                    "IP": "192,168,0,50",
                    "Port": "4222",
                    "Time": "%s",
                    "Topic":"%s"
                    }
                    """, localTime, topic);
            */
        String message = """
                {
                    "gatewayId": "OpEdge-8D-Belden_03",
                    "location": {
                      "latitude": 55.7055,
                      "longitude": -115.4055
                    },
                    "manufacturer": "Belden Industrial Solutions Inc.",
                    "model": "OpEdge-8D",
                    "serialNumber": "00:91:0C:95:05:36",
                    "firmwareVersion": "1.5.0",
                    "installationDate": "2024-08-29",
                    "installedSoftware": [
                      {
                        "name": "Edge Agent",
                        "version": "2.1.0"
                      },
                      {
                        "name": "Monitoring Module",
                        "version": "1.8.0"
                      },
                      {
                        "name": "BHNI",
                        "version": "1.2.0"
                      }
                    ],
                    "interfaces": {
                        "lan": [
                          {
                            "name": "lan07",
                            "address": "192.168.1.234",
                            "subnetMask": "255.255.255.0",
                            "gateway": "192.168.1.1",
                            "dnsServers": [
                              "8.8.8.8",
                              "8.8.4.4"
                            ]
                          },
                          {
                            "name": "lan02",
                            "address": "192.168.1.235",
                            "subnetMask": "255.255.255.0",
                            "gateway": "192.168.1.1",
                            "dnsServers": [
                              "8.8.8.8",
                              "8.8.4.4"
                            ]
                          },
                          {
                            "name": "dhcp",
                            "enabled": true,
                            "leaseDuration": "12h",
                            "server": "192.168.1.1"
                          }
                        ],
                        "wan": [
                          {
                            "name": "wan01",
                            "address": "203.0.113.403",
                            "subnetMask": "255.255.255.0",
                            "gateway": "203.0.113.1",
                            "dnsServers": [
                              "8.8.8.8",
                              "8.8.4.4"
                            ]
                          },
                          {
                            "name": "dhcp",
                            "enabled": false,
                            "leaseDuration": "24h",
                            "server": "ISP DHCP Server"
                          }
                        ]
                  }
                }
                """;
                natsConnection.publish(topic,"subject1_ack" ,message.getBytes());
            System.out.println("Published to: " + topic);
        }
}


