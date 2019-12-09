using Microsoft.Azure.Storage.Blob;
using Rebus.AzureBlobs.DataBus;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.Tests.Contracts.DataBus;
using System;
using Rebus.Config;

namespace Rebus.AzureBlobs.Tests.DataBus
{
    public class AzureBlobsDataBusStorageFactory : IDataBusStorageFactory
    {
        readonly string _containerName = $"container-{Guid.NewGuid().ToString().Substring(0, 3)}".ToLowerInvariant();
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public IDataBusStorage Create()
        {
            Console.WriteLine($"Creating blobs data bus storage for container {_containerName}");

            var container = AzureConfig.StorageAccount.CreateCloudBlobClient().GetContainerReference(_containerName);

            return new AzureBlobsDataBusStorage(new ConsoleLoggerFactory(false), _fakeRebusTime, new AzureBlobsDataBusStorageOptions(), container);
        }

        public void CleanUp()
        {
            Console.WriteLine($"Deleting container {_containerName} (if it exists)");

            AsyncHelpers.RunSync(() => AzureConfig.StorageAccount.CreateCloudBlobClient()
                .GetContainerReference(_containerName)
                .DeleteIfExistsAsync());
        }

        public void FakeIt(DateTimeOffset fakeTime) => _fakeRebusTime.SetNow(fakeTime);
    }
}