
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using AzureEventHub;
using Newtonsoft.Json;
using System.Text;

//string connectionString = "Endpoint=sb://anisheventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tekrUCbhqiTTHJEzgWsmgbqvWqQb9DWSIhTcFWVtfps=";
string connectionString = "Endpoint=sb://anisheventhub.servicebus.windows.net/;SharedAccessKeyName=anishManagePolicy;SharedAccessKey=KdP2rF1v4hv5WrkblbK/Tfn3vclMrgFOxXdbeZ4VcvY=;EntityPath=anisheventhub";
string eventHubName = "anishEventhub";
string consumerGroup = "$Default";

List<Device> deviceList = new List<Device>()
{
    new Device() { deviceId = "D11",temperature=40.0f},
    new Device() { deviceId = "D12",temperature=39.9f},
    new Device() { deviceId = "D22",temperature=36.4f},
    new Device() { deviceId = "D23",temperature=37.4f},
    new Device() { deviceId = "D34",temperature=38.9f},
    new Device() { deviceId = "D45",temperature=35.4f},
};

await SendData(deviceList);
//await ReadEvents();
await ReadEventsFromPartition();

async Task SendData(List<Device> deviceList)
{
    EventHubProducerClient eventHubProducerClient = new EventHubProducerClient(connectionString, eventHubName);
    EventDataBatch eventBatch = await eventHubProducerClient.CreateBatchAsync();

    foreach (Device device in deviceList)
    {
        EventData eventData = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(device)));
        if (!eventBatch.TryAdd(eventData))
            Console.WriteLine("Error has occured");
    }

    await eventHubProducerClient.SendAsync(eventBatch);
    Console.WriteLine("Events sent");
    await eventHubProducerClient.DisposeAsync();
}
async Task GetPartitionIds()
{
    EventHubConsumerClient eventHubConsumerClient = new EventHubConsumerClient(consumerGroup, connectionString);

    string[] partitionIds = await eventHubConsumerClient.GetPartitionIdsAsync();
    foreach (string partitionId in partitionIds)
        Console.WriteLine("Partition Id {0}", partitionId);
}

async Task ReadEvents()
{
    EventHubConsumerClient eventHubConsumerClient = new EventHubConsumerClient(consumerGroup, connectionString);
    var cancellationSource = new CancellationTokenSource();
    cancellationSource.CancelAfter(TimeSpan.FromSeconds(300));

    await foreach (PartitionEvent partitionEvent in eventHubConsumerClient.ReadEventsAsync(cancellationSource.Token))
    {
        Console.WriteLine($"Partition ID {partitionEvent.Partition.PartitionId}");
        Console.WriteLine($"Data Offset {partitionEvent.Data.Offset}");
        Console.WriteLine($"Sequence Number {partitionEvent.Data.SequenceNumber}");
        Console.WriteLine($"Partition Key {partitionEvent.Data.PartitionKey}");
        Console.WriteLine(Encoding.UTF8.GetString(partitionEvent.Data.EventBody));

    }
}

async Task ReadEventsFromPartition()
{
    EventHubConsumerClient eventHubConsumerClient = new EventHubConsumerClient(consumerGroup, connectionString);
    var cancellationSource = new CancellationTokenSource();
    cancellationSource.CancelAfter(TimeSpan.FromSeconds(300));

    string partitionID = (await eventHubConsumerClient.GetPartitionIdsAsync()).First();
    await foreach (PartitionEvent _event in eventHubConsumerClient.ReadEventsFromPartitionAsync(partitionID, EventPosition.Latest))
    {
        Console.WriteLine($"Partition ID {_event.Partition.PartitionId}");
        Console.WriteLine($"Data Offset {_event.Data.Offset}");
        Console.WriteLine($"Sequence Number {_event.Data.SequenceNumber}");
        Console.WriteLine($"Partition Key {_event.Data.PartitionKey}");
        Console.WriteLine(Encoding.UTF8.GetString(_event.Data.EventBody));
    }
}