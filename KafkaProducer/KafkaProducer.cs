namespace KafkaProducer
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Text;
    using Confluent.Kafka;
    using Confluent.Kafka.Serialization;

    public class Program
    {
        static void Main(string[] args)
        {
            var producerid = Dns.GetHostName();

            var config = new Dictionary<string, object>
            {
                { "client.id", producerid },
                { "bootstrap.servers", "localhost:9092" },
                // Màxima garantia que no es perd cap missatge
                { "default.topic.config", new Dictionary<string, object>
                    {
                        { "acks", "all" }
                    }
                }
            };

            var fitxer = "missatges.txt";

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                string text = "";

                System.IO.StreamReader file = new System.IO.StreamReader(fitxer);
                while ((text = file.ReadLine()) != null)
                {
                    // Sembla que m'ho podria estalviar amb .GetAwaiter().GetResult();
                    // Posant-hi al darrere .Result són missatges síncrons
                    System.Console.WriteLine($"Enviant: {text}");
                    var deliveryReportTask = producer.ProduceAsync("hello-topic", null, text);
                    deliveryReportTask.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
                file.Close();
                producer.Flush(-1);
            }
        }
    }
}