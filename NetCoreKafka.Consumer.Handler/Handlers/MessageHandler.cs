using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using Kafka.Consumer.Handler.Models;
using Kafka.Consumer.Handler.Data;

namespace Kafka.Consumer.Handler.Handlers
{
    public class MessageHandler : IHostedService
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        public MessageHandler(ILogger<MessageHandler> logger,IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = _configuration.GetValue<string>("KAFKA_BOOTSTRAP"),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(_configuration.GetValue<string>("KAFKA_TOPIC"));
                var cts = new CancellationTokenSource();

                try
                {
                    Console.WriteLine("Connected to Kafka");
                    while (true)
                    {
                        var message = c.Consume(cts.Token);                        
                        Console.WriteLine($"Mensagem: {message.Value} recebida de {message.TopicPartitionOffset}");
                        Product produto = JsonSerializer.Deserialize<Product>(message.Value);
                        MongoDBClient mongo = new MongoDBClient(_configuration);
                        mongo.IncluirProduto(produto);
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }

            return Task.CompletedTask;

        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}       