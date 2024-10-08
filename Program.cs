using Confluent.Kafka;

namespace Producer.Net
{
    internal class Program
    {
        static void Main(string[] args)
        {
            const string topic = "test-topic";

            var config = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:19094, 127.0.0.1:29094, 127.0.0.1:39094",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "local_kafka_user",
                SaslPassword = "local_pass",
                
                ClientId = "test_kafka_producer_local_net",

                Acks = Acks.All,

                LingerMs = 10,

                MaxInFlight = 5,
                MessageMaxBytes = 1048576,
                SocketReceiveBufferBytes = 1048576,
                SocketSendBufferBytes = 1048576,
                EnableIdempotence = false,
                
                CompressionType = CompressionType.Lz4,
                
                BatchSize = 32 * 1024,
            };

            DateTime startDt = DateTime.Now;

            using (var producer = new ProducerBuilder<string, string>(config).SetKeySerializer(Serializers.Utf8).SetValueSerializer(Serializers.Utf8).Build())
            {
                for (int i = 0; i < 1000; ++i)
                {
                    string value = $"hello_geeksforgeeks_{i}";
                    Message<string, string> mes = new Message<string, string> { Key = "test_key", Value = value };

                    producer.Produce(topic, new Message<string, string> { Key = "test_key", Value = value },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced event to topic {topic}: key = test_key value = {value}");
                            }
                        });
                }

                producer.Flush();
            }

            DateTime finishDt = DateTime.Now;

            Console.WriteLine($"Started at {startDt:O}");
            Console.WriteLine($"Finished at {finishDt:O}");
        }
    }
}
