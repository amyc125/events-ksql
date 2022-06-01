using System;
using System.Threading.Tasks;

namespace Events
{
    internal class Program
    {
        private const string BootstrapServer = "127.0.0.1:9092";
        private const string TopicName = "alphabets";

        private static async Task Main()
        {
            var producer = new Producer(BootstrapServer);
            await producer.StartSendingMessages(TopicName);
        }
    }
}
