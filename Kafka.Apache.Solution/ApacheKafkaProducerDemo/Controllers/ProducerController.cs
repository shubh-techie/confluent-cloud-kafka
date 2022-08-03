using ApacheKafkaProducerDemo.Model;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Net;
using System.Text.Json;

namespace ApacheKafkaProducerDemo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly string _bootstrapserver = "localhost:9092";
        private readonly string _topicName = "test";

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] OrderRequest orderRequest)
        {
            string message = JsonSerializer.Serialize(orderRequest);
            return Ok(await SendOrderRequest(_topicName, message));
        }

        private async Task<bool> SendOrderRequest(string topicName, string message)
        {
            ProducerConfig config = new ProducerConfig() { BootstrapServers = _bootstrapserver, ClientId = Dns.GetHostName() };

            try
            {
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(topicName, new Message<Null, string>() { Value = message });
                    Debug.WriteLine($"Delivery time stamp : {result.Timestamp.UtcDateTime}");

                    return await Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured : {ex.Message}");
            }

            return await Task.FromResult(false);
        }
    }
}
