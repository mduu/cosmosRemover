using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Cosmos;

namespace CosmosRemover
{
    class Program
    {
        private const string CosmosDbConnectionString = "<your_connection_string>";
        private const string CosmosDbDatabase = "<your_database_name>";
        private const string CosmosDbContainerName = "<your_container_name>";
        private const string CosmosDbSqlQuery = "<your_cosmosdb_sql_query>";
        private const int MaxDegreeOfParallelism = 30;

        static async Task Main(string[] args)
        {
            Feedback("Connecting to Cosmos DB ...");
            using var cosmosClient = new CosmosClient(CosmosDbConnectionString, new CosmosClientOptions());
            var db = cosmosClient.GetDatabase(CosmosDbDatabase);
            var container = db.GetContainer(CosmosDbContainerName);

            Feedback("Query UserLedger ...");

            var query = new QueryDefinition(CosmosDbSqlQuery);
            var queryResult = container.GetItemQueryIterator<dynamic>(query);

            var countDeleted = 0;
            var countErrors = 0;
            Feedback("Deleting documents ..");

            var deleteBlock = new ActionBlock<DocRef>(async docRef =>
                {
                    var result =
                        await container.DeleteItemStreamAsync(docRef.Id, new PartitionKey(docRef.PartitionKey));
                    if (result.IsSuccessStatusCode)
                    {
                        countDeleted++;
                        Feedback($"Deleted document #{countDeleted}");
                    }
                    else
                    {
                        countErrors++;
                        Feedback($"Failed deleting document. #{countErrors} errors. Message: {result.ErrorMessage}");
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = MaxDegreeOfParallelism
                });

            while (queryResult.HasMoreResults)
            {
                foreach (var response in await queryResult.ReadNextAsync())
                {
                    string id = response.id;
                    string partitionKey = response.AggregateId;
                    deleteBlock.Post(new DocRef {Id = id, PartitionKey = partitionKey});
                }
            }

            Feedback("All documents scheduled for deletion. Waiting for completion.");

            deleteBlock.Complete();
            await deleteBlock.Completion;

            Feedback($"Number of docs deleted: {countDeleted}");
            Feedback($"Number of docs failed to delete: {countErrors}");
            Feedback("Done.");
        }

        private static void Feedback(string message)
        {
            Console.WriteLine($"{DateTime.Now:G} | {message}");
        }

        class DocRef
        {
            public string Id { get; set; }
            public string PartitionKey { get; set; }
        }
    }
}