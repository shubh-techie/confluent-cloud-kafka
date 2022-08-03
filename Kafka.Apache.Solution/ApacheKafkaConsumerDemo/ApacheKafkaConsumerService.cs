using ApacheKafkaConsumerDemo.Model;
using Confluent.Kafka;
using System.Diagnostics;
using System.Text.Json;

namespace ApacheKafkaConsumerDemo
{
    public class ApacheKafkaConsumerService : IHostedService
    {
        private readonly string _topicName = "test";
        private readonly string _groupId = "test_group";
        private readonly string _bootstrapserver = "localhost:9092";

        public Task StartAsync(CancellationToken cancellationToken)
        {
            ConsumerConfig config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapserver,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = _groupId
            };

            try
            {
                using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumerBuilder.Subscribe(_topicName);

                    var cancelToken = new CancellationTokenSource();

                    try
                    {
                        while (true)
                        {
                            var consumer = consumerBuilder.Consume(cancelToken.Token);

                            var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);

                            Debug.WriteLine($" Processing Order id {orderRequest?.OrderId}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumerBuilder.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
