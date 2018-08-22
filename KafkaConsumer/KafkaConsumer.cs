namespace KafkaConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using Confluent.Kafka;
    using Confluent.Kafka.Serialization;

    public class Program
    {

        static void processaMissatge(string message)
        {
            int dots = message.Split('.').Length - 1;
            Thread.Sleep(dots * 1000);
        }

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                System.Console.WriteLine("Enter client id as param");
                return;
            }

            var clientid = args[0];

            var config = new Dictionary<string, object>
            {
                { "client.id", clientid },
                { "group.id", "consumergroup" },
                { "bootstrap.servers", "localhost:9092" },
                { "enable.auto.commit", "false"}
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(new string[] { "hello-topic" });

                consumer.OnMessage += (_, msg) =>
                {
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    // En el tutorial hi afegeixen Wait() però no sé si cal...
                    processaMissatge(msg.Value);
                    consumer.CommitAsync(msg).Wait();
                };

                consumer.OnError += (_, msg) =>
                {
                    Console.WriteLine($"Error {msg}");
                };

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}.");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C per acabar.");

                while (cancelled == false)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}