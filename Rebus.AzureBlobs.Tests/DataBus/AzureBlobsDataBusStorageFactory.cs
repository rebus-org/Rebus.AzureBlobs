using Rebus.AzureBlobs.DataBus;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.Tests.Contracts.DataBus;
using System;
using Azure.Storage.Blobs;
using Rebus.Config;

namespace Rebus.AzureBlobs.Tests.DataBus;

public class AzureBlobsDataBusStorageFactory : IDataBusStorageFactory
{
    readonly string _containerName = $"container-{Guid.NewGuid().ToString().Substring(0, 3)}".ToLowerInvariant();
    readonly FakeRebusTime _fakeRebusTime = new();

    public IDataBusStorage Create()
    {
        Console.WriteLine($"Creating blobs data bus storage for container {_containerName}");

        var blobContainerClient = new BlobContainerClient(AzureConfig.ConnectionString, _containerName);

        return new AzureBlobsDataBusStorage(new ConsoleLoggerFactory(false), _fakeRebusTime, new AzureBlobsDataBusStorageOptions(), blobContainerClient);
    }

    public void CleanUp()
    {
        Console.WriteLine($"Deleting container {_containerName} (if it exists)");

        new BlobContainerClient(AzureConfig.ConnectionString, _containerName).DeleteIfExists();
    }

    public void FakeIt(DateTimeOffset fakeTime) => _fakeRebusTime.SetNow(fakeTime);
}