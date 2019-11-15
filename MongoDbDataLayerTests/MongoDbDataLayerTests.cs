using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using System.Security.Authentication;
using System.Collections.Generic;
using MongoDB.Bson;
using System;
using System.Threading.Tasks;
using System.Linq;

namespace MongoDbDataLayerTests
{
    [TestClass]
    public class MongoDbDataLayerTests
    {
        #region Server Configurations
        MongoDbDataLayer.MongoDbDataLayer mongoDbCore;
        MongoClientSettings TestSettings = new MongoClientSettings
        {
            Server = new MongoServerAddress("localhost", 27017),
            SslSettings = new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 }
        };
        string testDbName = "Test";
        string testCollectionName = "MongoDbCoreTests";
        #endregion Server Configurations

        #region CRUD Operation Variables
        BsonDocument testRecord = new BsonDocument("number", 0);
        List<BsonDocument> testRecords = Enumerable.Range(1, 11).Select(i => new BsonDocument("number", i)).ToList();
        #endregion CRUD Operation Variables

        #region Test Functions
        /// <summary>
        /// Tests all functions in MongoDbDataLayer.cs
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task CRUD_Operations_Test()
        {
            // Create instance of MongoDbCore
            Console.WriteLine($"DB Setup:");
            mongoDbCore = new MongoDbDataLayer.MongoDbDataLayer(TestSettings);
            Assert.IsTrue(mongoDbCore != null, "Failed to create instance of MongoDbCore.");

            // Get list of DB names in the server
            List<string> dbNames = await mongoDbCore.GetAllDbNamesAsync();
            Assert.IsTrue(dbNames != null, "Failed to get list of DB names.");

            // Get list of DBs in the server
            List<BsonDocument> dbs = await mongoDbCore.GetAllDbsAsync();
            Assert.IsTrue(dbs != null, "Failed to get list of DBs in servernames.");

            // Get this test DB
            IMongoDatabase database = mongoDbCore.GetDatabase(testDbName);
            Assert.IsTrue(database != null, $"Failed to get test db {testDbName}.");
            Console.WriteLine($"\tDatabase:\t'{testDbName}'");

            // Get this test collection
            IMongoCollection<BsonDocument> collection = mongoDbCore.GetCollection(database, testCollectionName);
            Assert.IsTrue(collection != null, $"Failed to get test collection {testCollectionName}.");
            Console.WriteLine($"\tCollection:\t'{testCollectionName}'");

            // Delete all records from the Test DB
            DeleteResult deleteResult = await mongoDbCore.DeleteManyAsync(collection, FilterDefinition<BsonDocument>.Empty);
            Assert.IsTrue(deleteResult.IsAcknowledged);

            // Get the count for the test collection
            long collectionCount = await mongoDbCore.GetCollectionCountAsync(collection);
            Assert.IsTrue(collectionCount == 0, $"Test collection count for {testCollectionName} should be 0.");
            Console.WriteLine($"\tRecords in {testCollectionName}:\t{collectionCount}.");

            // Insert 1 record
            Console.WriteLine($"\n\nInsert One:");
            bool insertOne = await mongoDbCore.InsertOneAsync(collection, testRecord);
            Assert.IsTrue(insertOne, $"Failed to insert one record.");
            Console.WriteLine($"\tInserted one record: {testRecord}");

            // Get the count for the test collection
            collectionCount = await mongoDbCore.GetCollectionCountAsync(collection);
            Assert.IsTrue(collectionCount == 1, $"Test collection count for {testCollectionName} should be 1.");
            Console.WriteLine($"\tRecords in {testCollectionName}:\t{collectionCount}.");

            // Find 
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("number", testRecord["number"]);
            List<BsonDocument> findResults = await mongoDbCore.FindAsync(collection, projection:null, filter:filter);
            Assert.IsTrue(findResults[0]["number"] == testRecord["number"], $"Inserted document number should equal {testRecord["number"]}");
            Console.WriteLine($"\tFiltered FindAsync Result: {findResults[0]}.");

            // Update
            Console.WriteLine($"\n\nUpdate One:");
            BsonDocument updateRecordLocal = new BsonDocument("number", 11);
            BsonDocument updateRecordDb = await mongoDbCore.UpdateOneAsync(collection, filter, updateRecordLocal);
            Assert.IsTrue(updateRecordDb != null, $"Updated document number should equal {updateRecordLocal["number"]}");
            Console.WriteLine($"\tUpdated one record: {updateRecordDb}");

            // Find
            filter = Builders<BsonDocument>.Filter.Eq("number", updateRecordLocal["number"]);
            findResults = await mongoDbCore.FindAsync(collection, projection: null, filter: filter);
            Assert.IsTrue(findResults[0]["number"] == updateRecordLocal["number"], $"Inserted document number should equal {updateRecordLocal["number"]}");
            Console.WriteLine($"\tFiltered FindAsync Result: {findResults[0]}.");

            // Delete
            Console.WriteLine($"\n\nDelete One:");
            BsonDocument deletedDoc = await mongoDbCore.DeleteOneAsync(collection, filter);
            Console.WriteLine($"\tDeleted one record: {deletedDoc}");

            // Count
            collectionCount = await mongoDbCore.GetCollectionCountAsync(collection);
            Assert.IsTrue(collectionCount == 0, $"Test collection count for {testCollectionName} should be 0.");
            Console.WriteLine($"\tRecords in {testCollectionName}:\t{collectionCount}.");

            // Insert Many
            Console.WriteLine($"\n\nInsert Many:");
            bool insertMany = await mongoDbCore.InsertManyAsync(collection, testRecords);
            Assert.IsTrue(insertMany, $"Failed to insert many records.");
            Console.WriteLine($"\tInserted {testRecords.Count} records.");

            // Count
            collectionCount = await mongoDbCore.GetCollectionCountAsync(collection);
            Assert.IsTrue(collectionCount == testRecords.Count, $"Test collection count for {testCollectionName} should be {testRecords.Count}.");
            Console.WriteLine($"\tRecords in {testCollectionName}:\t{collectionCount}.");

            // Find Many
            findResults = await mongoDbCore.FindAsync(collection, projection: null, filter: null);
            Assert.IsTrue(findResults.Count == collectionCount, $"findResults.Count should equal {collectionCount}");
            Console.WriteLine($"\tFiltered FindAsync Result: \n\t\t{String.Join(",\n\t\t", findResults)}.");

            // Update Many
            Console.WriteLine($"\n\nUpdate Many:");
            filter = Builders<BsonDocument>.Filter.Lte("number", 5);
            UpdateDefinition<BsonDocument> updateDefinition = Builders<BsonDocument>.Update.Inc("number", 10);
            UpdateResult updateMany = await mongoDbCore.UpdateManyAsync(collection, filter, updateDefinition);
            Assert.IsTrue(updateMany.IsAcknowledged, $"Failed to update many records.");
            Console.WriteLine($"\tUpdated {updateMany.ModifiedCount} records.");

            // Find Many
            findResults = await mongoDbCore.FindAsync(collection, projection: null, filter: null);
            Assert.IsTrue(findResults.Count == collectionCount, $"findResults.Count should equal {collectionCount}");
            Console.WriteLine($"\tFiltered FindAsync Result: \n\t\t{String.Join(",\n\t\t", findResults)}.");

            // Delete Many
            Console.WriteLine($"\n\nDelete Many:");
            filter = Builders<BsonDocument>.Filter.Gte("number", 10);
            deleteResult = await mongoDbCore.DeleteManyAsync(collection, filter);
            Assert.IsTrue(deleteResult.IsAcknowledged);
            Console.WriteLine($"\tDeleted records where number >= 10.");

            // Find Many
            findResults = await mongoDbCore.FindAsync(collection, projection: null, filter: null);
            Assert.IsTrue(findResults.Count == 4, $"findResults.Count should equal 4");
            Console.WriteLine($"\tFiltered FindAsync Result: \n\t\t{String.Join(",\n\t\t", findResults)}.");

            // Delete all records from the Test DB
            deleteResult = await mongoDbCore.DeleteManyAsync(collection, FilterDefinition<BsonDocument>.Empty);
            Assert.IsTrue(deleteResult.IsAcknowledged, $"Failed to delete all records.");

            // Count
            collectionCount = await mongoDbCore.GetCollectionCountAsync(collection);
            Assert.IsTrue(collectionCount == 0, $"Test collection count for {testCollectionName} should be 0.");
            Console.WriteLine($"\tRecords in {testCollectionName}:\t{collectionCount}.");
        }
        #endregion Test Functions
    }
}
