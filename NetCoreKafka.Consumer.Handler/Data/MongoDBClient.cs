using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using Kafka.Consumer.Handler.Models;

namespace Kafka.Consumer.Handler.Data
{
    public class MongoDBClient
    {
        private readonly IConfiguration _configuration;

        public MongoDBClient(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        public void IncluirProduto(Product produto)
        {
            try
            {
                MongoClient client = new MongoClient(
                    _configuration.GetValue<string>("MONGO_DB"));
                IMongoDatabase db = client.GetDatabase("DBCatalogo");

                Console.WriteLine("Incluindo produto...");

                var catalogoProdutos = db.GetCollection<Product>("Catalogo");
                catalogoProdutos.InsertOne(produto);

                Console.WriteLine("Produto Incluido com sucesso");
            }
            catch (Exception e)
            {
                Console.WriteLine("Erro ao incluir produto" + e.Message);
            }
        }

    }
}