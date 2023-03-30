using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using NUnit.Framework;
using Rebus.AzureBlobs.Sagas;
using Rebus.AzureBlobs.Tests.Extensions;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;

namespace Rebus.AzureBlobs.Tests.Sagas;

[TestFixture]
public class TestAzureStorageSagaSnapshotStorage : FixtureBase
{
    AzureStorageSagaSnapshotStorage _storage;

    protected override void SetUp()
    {
        base.SetUp();

        var containerName = Guid.NewGuid().ToString("n");
        var blobContainerClient = new BlobContainerClient(AzureConfig.ConnectionString, containerName);

        Using(blobContainerClient.AsDisposable(c => c.DeleteIfExists()));

        _storage = new AzureStorageSagaSnapshotStorage(blobContainerClient, new ListLoggerFactory());
    }

    [Test]
    public async Task CanSaveSnapshot()
    {
        await _storage.Save(new RandomSagaData {Text = "hej"}, new Dictionary<string, string> {["test"] = "hej"});
    }

    class RandomSagaData : SagaData
    {
        public string Text { get; set; }
    }
}