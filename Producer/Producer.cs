using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Text.Json;

namespace Events
{
    public class Producer
    {
        private readonly ProducerConfig _producerConfig;

        public Producer(string bootstrapServer)
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServer,
                EnableDeliveryReports = true,
                ClientId = Dns.GetHostName(),
                Debug = "msg",

                // retry settings:
                // Receive acknowledgement from all sync replicas
                Acks = Acks.All,
                // Number of times to retry before giving up
                MessageSendMaxRetries = 3,
                // Duration to retry before next attempt
                RetryBackoffMs = 1000,
                // Set to true if you don't want to reorder messages on retry
                EnableIdempotence = true
            };
        }

        public async Task StartSendingMessages(string topicName)
        {
            using var producer = new ProducerBuilder<string, string>(_producerConfig)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(Serializers.Utf8)
                .SetLogHandler((_, message) =>
                    Console.WriteLine($"Facility: {message.Facility}-{message.Level} Message: {message.Message}"))
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}. Is Fatal: {e.IsFatal}"))
                .Build();
            try
            {
                // Associate the instance of 'EventLog' with local System Log.
                EventLog myEventLog = new EventLog("System", ".");

                EventLogEntryCollection myLogEntryCollection=myEventLog.Entries;
                int myCount =myLogEntryCollection.Count;
                // Iterate through all 'EventLogEntry' instances in 'EventLog'.
                for(int i=myCount-1;i>-1;i--)
                {
                    EventLogEntry myLogEntry = myLogEntryCollection[i];
                        // Display Source of the event.


                    //var message = myLogEntry.Source+" was the source of last event of type " +myLogEntry.EntryType;
                    
                    //+myLogEntry.TimeGenerated +myLogEntry.MachineName;

                    Event event1 = new Event
                    {
                        Source = myLogEntry.Source, 
                        EntryType = myLogEntry.EntryType.ToString()
                    };

                    string json = JsonSerializer.Serialize(event1);
                    
                    //Console.WriteLine(json);

                    //Console.WriteLine(message.ToString());

                    var deliveryReport = await producer.ProduceAsync(topicName,
                    new Message<string, string>
                    {
                        Key = "UUID",
                        Value = json
                    });

                    Console.WriteLine($"Message sent (value: '{event1}'). Delivery status: {deliveryReport.Status}");
                    if (deliveryReport.Status != PersistenceStatus.Persisted)
                    {
                        // delivery might have failed after retries. This message requires manual processing.
                        Console.WriteLine(
                            $"ERROR: Message not ack'd by all brokers (value: '{event1}'). Delivery status: {deliveryReport.Status}");
                    }
                }
            }
            catch (ProduceException<string, string> e)
            {
                // Log this message for manual processing.
                Console.WriteLine($"Permanent error: {e.Message} for message (value: '{e.DeliveryResult.Value}')");
                Console.WriteLine("Exiting producer...");
            }
        }
    }
}