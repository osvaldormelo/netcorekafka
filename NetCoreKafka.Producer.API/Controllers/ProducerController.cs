using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Kafka.Producer.API.Models;
using System.Text.Json;

namespace Kafka.Producer.API.Controllers
{
    

    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        public ProducerController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpPost]
        [ProducesResponseType(typeof(string), 201)]
        [ProducesResponseType(400)]
        [ProducesResponseType(500)]
        public IActionResult Post(Product msg)
        {
            return Created("", SendMessageByKafka(msg));
        }

        private string SendMessageByKafka(Product message)
        {
            var config = new ProducerConfig { BootstrapServers = _configuration.GetValue<string>("KAFKA_BOOTSTRAP") };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer
                                        .ProduceAsync(_configuration.GetValue<string>("KAFKA_TOPIC"), new Message<Null, string> { Value = JsonSerializer.Serialize(message) })
                                            .GetAwaiter()
                                                .GetResult();

                    return $"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'";
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            return string.Empty;
        }

    }
}