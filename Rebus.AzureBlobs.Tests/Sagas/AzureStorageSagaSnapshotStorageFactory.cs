using Rebus.Auditing.Sagas;
using Rebus.AzureBlobs.Sagas;
using Rebus.Logging;
using Rebus.Tests.Contracts.Sagas;
using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Storage.Blobs;

namespace Rebus.AzureBlobs.Tests.Sagas;

public class AzureStorageSagaSnapshotStorageFactory : ISagaSnapshotStorageFactory
{
    readonly AzureStorageSagaSnapshotStorage _storage;
    readonly BlobContainerClient _blobContainerClient;

    public AzureStorageSagaSnapshotStorageFactory()
    {
        var containerName = $"container{Guid.NewGuid():N}";
        _blobContainerClient = new BlobContainerClient(AzureConfig.ConnectionString, containerName);
        _storage = new AzureStorageSagaSnapshotStorage(_blobContainerClient, new ConsoleLoggerFactory(false));
    }

    public void Dispose() => _blobContainerClient.DeleteIfExists();

    public ISagaSnapshotStorage Create()
    {
        _storage.DropAndRecreateContainer();
        _storage.EnsureContainerExists();
        return _storage;
    }

    public IEnumerable<SagaDataSnapshot> GetAllSnapshots()
    {
        var allBlobs = _storage.ListAllBlobs()
            .Select(b => new
            {
                Parts = b.Name.Split('/')
            })
            .Where(x => x.Parts.Length == 3)
            .Select(b =>
            {
                var guid = Guid.Parse(b.Parts[0]);
                var revision = int.Parse(b.Parts[1]);
                var part = b.Parts[2];
                return new
                {
                    Id = guid,
                    Revision = revision,
                    Part = part,
                };
            })
            .GroupBy(b => new { b.Id, b.Revision })
            .Select(g => new SagaDataSnapshot
            {
                SagaData = _storage.GetSagaData(g.Key.Id, g.Key.Revision),
                Metadata = _storage.GetSagaMetaData(g.Key.Id, g.Key.Revision)
            })
            .ToList();

        return allBlobs;
    }
}