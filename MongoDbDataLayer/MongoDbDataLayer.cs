using System;
using MongoDB.Driver;
using System.Threading.Tasks;
using MongoDB.Bson;
using System.Collections.Generic;
using System.Linq;

namespace MongoDbDataLayer
{
    public class MongoDbDataLayer
    {
        #region Variables
        private MongoClient Client;
        private MongoClientSettings Settings;
        private MongoClientSettings AzureSettingsExample = new MongoClientSettings
        {
            Server = new MongoServerAddress("url", 123),
            UseSsl = true,
            Credential = MongoCredential.CreateMongoCRCredential("dbname", "username", "pw"),
            ReplicaSetName = "replicaset"
        };
        #endregion Variables

        #region Constructors
        public MongoDbDataLayer(MongoClientSettings settings)
        {
            SetClientSettings(settings);
            ResetClient();
        }
        #endregion Constructors

        #region Public Functions
        public void ResetClient()
        {
            try
            {
                Client = new MongoClient(Settings);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"InitializeClient threw {ex.Message}\n{ex.StackTrace}");
            }
        }

        public void SetClientSettings(MongoClientSettings settings)
        {
            Settings = settings;
        }

        public async Task<List<string>> GetAllDbNamesAsync()
        {
            try
            {
                List<string> AllDbNames = (await Client.ListDatabaseNamesAsync()).ToList();
                return AllDbNames;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetAllDbNames threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<List<BsonDocument>> GetAllDbsAsync()
        {
            try
            {
                List<BsonDocument> AllDbs = (await Client.ListDatabasesAsync()).ToList();
                return AllDbs;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetAllDbs threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public IMongoDatabase GetDatabase(string dbName)
        {
            try
            {
                IMongoDatabase database = Client.GetDatabase(dbName);
                return database;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetDatabase threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public IMongoCollection<BsonDocument> GetCollection(IMongoDatabase database, string collectionName)
        {
            try
            {
                var collection = database.GetCollection<BsonDocument>(collectionName);
                return collection;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetCollection threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<long> GetCollectionCountAsync(IMongoCollection<BsonDocument> collection)
        {
            try
            {
                long count = await collection.CountDocumentsAsync(new BsonDocument());
                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GetCollectionCountAsync threw {ex.Message}\n{ex.StackTrace}");
                return -1;
            }
        }

        public async Task<bool> InsertOneAsync(IMongoCollection<BsonDocument> collection, BsonDocument record)
        {
            try
            {
                await collection.InsertOneAsync(record);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"InsertOne threw {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public async Task<bool> InsertManyAsync(IMongoCollection<BsonDocument> collection, IEnumerable<BsonDocument> records)
        {
            try
            {
                await collection.InsertManyAsync(records);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"InsertManyAsync threw {ex.Message}\n{ex.StackTrace}");
                return false;
            }
        }

        public async Task<List<BsonDocument>> FindAsync(IMongoCollection<BsonDocument> collection,
                                                        ProjectionDefinition<BsonDocument> projection = null,
                                                        FilterDefinition<BsonDocument> filter = null,
                                                        Nullable<int> limit = null)
        {
            try
            {
                // No projections
                if (projection == null)
                {
                    projection = Builders<BsonDocument>.Projection.Exclude("");
                }
                // No filters
                if (filter == null)
                {
                    filter = Builders<BsonDocument>.Filter.Empty;
                }
                // No limit
                if (limit == null || limit <= 0)
                {
                    limit = null;
                }

                List<BsonDocument> records = await collection.Find(filter).Project(projection).Limit(limit).ToListAsync();
                return records;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FindAsync threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<BsonDocument> UpdateOneAsync(IMongoCollection<BsonDocument> collection,
                                                        FilterDefinition<BsonDocument> filter,
                                                        UpdateDefinition<BsonDocument> update)
        {
            try
            {
                if (collection == null || filter == null || update == null)
                {
                    throw new Exception($"Collection, filter, or update parameter is null.");
                }
                BsonDocument updateResult = await collection.FindOneAndUpdateAsync(filter, update);
                return updateResult;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"UpdateOneAsync threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<UpdateResult> UpdateManyAsync(IMongoCollection<BsonDocument> collection,
                                                        FilterDefinition<BsonDocument> filter,
                                                        UpdateDefinition<BsonDocument> update)
        {
            try
            {
                if (collection == null || filter == null || update == null)
                {
                    throw new Exception($"Collection, filter, or update parameter is null.");
                }
                UpdateResult updateResult = await collection.UpdateManyAsync(filter, update);
                return updateResult;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"UpdateManyAsync threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<BsonDocument> DeleteOneAsync(IMongoCollection<BsonDocument> collection,
                                                       FilterDefinition<BsonDocument> filter)
        {
            try
            {
                if (collection == null || filter == null)
                {
                    throw new Exception($"Collection or filter parameter is null.");
                }
                BsonDocument deleteResult = await collection.FindOneAndDeleteAsync(filter);
                return deleteResult;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DeleteOneAsync threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }

        public async Task<DeleteResult> DeleteManyAsync(IMongoCollection<BsonDocument> collection,
                                                        FilterDefinition<BsonDocument> filter)
        {
            try
            {
                if (collection == null || filter == null)
                {
                    throw new Exception($"Collection or filter parameter is null.");
                }
                DeleteResult deleteResult = await collection.DeleteManyAsync(filter);
                return deleteResult;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DeleteManyAsync threw {ex.Message}\n{ex.StackTrace}");
                return null;
            }
        }
        #endregion Public Functions
    }
}
