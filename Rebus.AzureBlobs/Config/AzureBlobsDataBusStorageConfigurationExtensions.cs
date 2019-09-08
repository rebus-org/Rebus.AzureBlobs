using Microsoft.Azure.Storage;
using Rebus.AzureBlobs.DataBus;
using Rebus.DataBus;
using Rebus.Logging;
using System;
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
        /// Configures Rebus' data bus to storage data in Azure blobs in the given storage account and container
        /// </summary>
        public static void StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, CloudStorageAccount storageAccount, string containerName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (containerName == null) throw new ArgumentNullException(nameof(containerName));
            if (storageAccount == null) throw new ArgumentNullException(nameof(storageAccount));

            Configure(configurer, containerName, storageAccount);
        }

        /// <summary>
        /// Configures Rebus' data bus to storage data in Azure blobs in the given storage account and container
        /// </summary>
        public static void StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, string storageAccountConnectionString, string containerName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (containerName == null) throw new ArgumentNullException(nameof(containerName));
            if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));

            var cloudStorageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);

            Configure(configurer, containerName, cloudStorageAccount);
        }

        static void Configure(StandardConfigurer<IDataBusStorage> configurer, string containerName, CloudStorageAccount cloudStorageAccount)
        {
            configurer.OtherService<AzureBlobsDataBusStorage>().Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();

                return new AzureBlobsDataBusStorage(cloudStorageAccount, containerName, rebusLoggerFactory, rebusTime);
            });

            configurer.Register(c => c.Get<AzureBlobsDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<AzureBlobsDataBusStorage>());
        }
    }
}