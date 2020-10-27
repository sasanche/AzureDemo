package org.example.EventHubConsumer;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;

public class Receiver {

    private static final String EH_NAMESPACE_CONNECTION_STRING = "eventhub-connection-string";
    private static final String eventHubName = "eventhub-name";
    private static final String STORAGE_CONNECTION_STRING = "storage-connection-string";
    private static final String STORAGE_CONTAINER_NAME = "checkpoint-container-name";
    private static final String CONSUMER_NAME = EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME;
    //private static final String CONSUMER_NAME = "other-consumer-name";

    public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
        System.out.printf("Processing event: partition %s partitionKey: %s with sequence no. %d with body: %s EventType: %s, %n",
                eventContext.getPartitionContext().getPartitionId(), eventContext.getEventData().getPartitionKey(),
                eventContext.getEventData().getSequenceNumber(), eventContext.getEventData().getBodyAsString(),
                (String)eventContext.getEventData().getProperties().get("EventType"));

        if (eventContext.getEventData().getSequenceNumber() % 10 == 0) {
            eventContext.updateCheckpoint();
        }
    };

    public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
        System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable());
    };

    public static void main(String[] args) throws Exception {
	
	/*  Define positionMap to choose eventPosition to start, delete checkpoint to use this  
        Map<String, EventPosition> positionMap = new HashMap<String, EventPosition>() {{
            put("0", EventPosition.fromSequenceNumber(30, true));
            put("1", EventPosition.earliest());
        }};
	*/

        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
                .connectionString(STORAGE_CONNECTION_STRING)
                .containerName(STORAGE_CONTAINER_NAME)
                .buildAsyncClient();

        EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .connectionString(EH_NAMESPACE_CONNECTION_STRING, eventHubName)
                .consumerGroup(CONSUMER_NAME)
                .processEvent(PARTITION_PROCESSOR)
                .processError(ERROR_HANDLER)
                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));
	// Delete checkpoint and uncomment to override event Position
                //.initialPartitionEventPosition(positionMap);

        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

        System.out.println("Starting event processor");
        eventProcessorClient.start();

        System.out.println("Press enter to stop.");
        System.in.read();

        System.out.println("Stopping event processor");
        eventProcessorClient.stop();
        System.out.println("Event processor stopped.");

        System.out.println("Exiting process");
    }

}
