using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace MongoDB.NET
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().Wait();
            Console.WriteLine($"Done");
            Console.ReadLine();
        }

        static async Task MainAsync()
        {
            var connectionString = "mongodb://localhost:27017";
            MongoClient client;
            //client = new MongoClient(connectionString);
            //client = new MongoClient(new MongoUrl("mongodb://localhost:27017"));
            //client = new MongoClient(MongoUrl.Create("mongodb://localhost:27017"));
            client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("blog");
        }
    }
}
