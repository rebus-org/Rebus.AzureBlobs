using Microsoft.Azure.Storage;
using Rebus.AzureBlobs.DataBus;
using Rebus.DataBus;
using Rebus.Logging;
using System;
using Microsoft.Azure.Storage.Blob;
using Rebus.Time;
// ReSharper disable UnusedMember.Global

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for Azure-based data bus storage
    /// </summary>
    public static class AzureBlobsDataBusStorageConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus' data bus to store data in/read data from Azure blobs in the given storage account and container
        /// </summary>
        public static AzureBlobsDataBusStorageOptions StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, CloudStorageAccount storageAccount, string containerName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (containerName == null) throw new ArgumentNullException(nameof(containerName));
            if (storageAccount == null) throw new ArgumentNullException(nameof(storageAccount));

            var createCloudBlobClient = storageAccount.CreateCloudBlobClient();

            return Configure(configurer, createCloudBlobClient.GetContainerReference(containerName));
        }

        /// <summary>
        /// Configures Rebus' data bus to store data in/read data from Azure blobs in the given storage account and container
        /// </summary>
        public static AzureBlobsDataBusStorageOptions StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, string storageAccountConnectionString, string containerName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (containerName == null) throw new ArgumentNullException(nameof(containerName));
            if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));

            var cloudStorageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            var createCloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            return Configure(configurer, createCloudBlobClient.GetContainerReference(containerName));
        }

        /// <summary>
        /// Configures Rebus' data bus to store data in/read data from Azure blobs in the blob container with the given <paramref name="containerUri"/>
        /// </summary>
        public static AzureBlobsDataBusStorageOptions StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, Uri containerUri)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (containerUri == null) throw new ArgumentNullException(nameof(containerUri));

            var createCloudBlobClient = new CloudBlobContainer(containerUri);

            return Configure(configurer, createCloudBlobClient);
        }

        static AzureBlobsDataBusStorageOptions Configure(StandardConfigurer<IDataBusStorage> configurer, CloudBlobContainer cloudBlobContainer)
        {
            var options = new AzureBlobsDataBusStorageOptions();

            configurer.OtherService<AzureBlobsDataBusStorage>().Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();

                var azureBlobsDataBusStorage = new AzureBlobsDataBusStorage(rebusLoggerFactory, rebusTime, options, cloudBlobContainer);

                return azureBlobsDataBusStorage;
            });

            configurer.Register(c => c.Get<AzureBlobsDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<AzureBlobsDataBusStorage>());

            return options;
        }
    }
}