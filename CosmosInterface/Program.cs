using System;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace ConsoleApp
{
    /// <summary>
    /// Example BSON Model Declaration
    /// </summary>
    public class User
    {
        [MongoDB.Bson.Serialization.Attributes.BsonId]
        public ObjectId Id { get; set; }
        [BsonElement("name")]
        public string Name { get; set; }
        [BsonElement("blog")]
        public string Blog { get; set; }
        [BsonElement("age")]
        public int Age { get; set; }
        [BsonElement("location")]
        public string Location { get; set; }
    }

    /// <summary>
    /// Main Program
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().Wait();
            Console.ReadLine();
        }

        static async Task MainAsync()
        {

        }
    }
}
