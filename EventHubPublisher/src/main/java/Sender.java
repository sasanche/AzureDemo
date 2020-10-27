package org.example.EventHubPublisher;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import java.time.*;
//import java.lang.Integer;

public class Sender {
    public static void main(String[] args) {

        final String connectionString = "EventHub-Connection-String";
        final String eventHubName = "EventHub-Name";

        // create a producer using the namespace connection string and event hub name
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();

        int count = Integer.parseInt(args[0]);

        System.out.println("\n\nSending events in batches...");
        System.out.println("Batch Count: " + count);

        // prepare a batch of events to send to the event hub

        for (int n=0; n < count; n++)
        {
            int batchsize = 5;
            String partitionKey = "Universe " + n;
            System.out.println("\nBatch Size: " + batchsize + " BatchNumber: " + (n+1) + " Partition Key: " + partitionKey);

            CreateBatchOptions options = new CreateBatchOptions().setPartitionKey(partitionKey);
            EventDataBatch batch = producer.createBatch(options);

            for (int i=0; i < batchsize; i++) {
                String s = "Galaxy " + (n*batchsize+i) + " " + Instant.now().toString();
                String eventType;
                if (i%2 == 0)
                {
                    eventType = "TimeBasedEvent";
                }
                else
                {
                    eventType = "SpaceBasedEvent";
                }

                System.out.printf("Event: %s Type: %s %n", s, eventType);

                EventData event = new EventData(s);
                event.getProperties().put("EventType", eventType);

                batch.tryAdd(event);
            }

            // send the batch of events to the event hub
            producer.send(batch);
            try {
                // thread to sleep for 1000 milliseconds
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        // close the producer
        producer.close();

    }
}
