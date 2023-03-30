using Rebus.AzureBlobs.DataBus;
using Rebus.DataBus;
using Rebus.Logging;
using System;
using Azure.Storage.Blobs;
using Rebus.Time;
// ReSharper disable UnusedMember.Global

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for Azure-based data bus storage
/// </summary>
public static class AzureBlobsDataBusStorageConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus' data bus to store data in/read data from Azure blobs in the given storage account and container
    /// </summary>
    public static AzureBlobsDataBusStorageOptions StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, string storageAccountConnectionString, string containerName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (containerName == null) throw new ArgumentNullException(nameof(containerName));
        if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));

        var blobContainerClient = new BlobContainerClient(storageAccountConnectionString, containerName);

        return Configure(configurer, blobContainerClient);
    }

    /// <summary>
    /// Configures Rebus' data bus to store data in/read data from Azure blobs in the blob container with the given <paramref name="containerUri"/>
    /// </summary>
    public static AzureBlobsDataBusStorageOptions StoreInBlobStorage(this StandardConfigurer<IDataBusStorage> configurer, Uri containerUri)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (containerUri == null) throw new ArgumentNullException(nameof(containerUri));

        var blobContainerClient = new BlobContainerClient(containerUri);

        return Configure(configurer, blobContainerClient);
    }

    static AzureBlobsDataBusStorageOptions Configure(StandardConfigurer<IDataBusStorage> configurer, BlobContainerClient blobContainerClient)
    {
        var options = new AzureBlobsDataBusStorageOptions();

        configurer.OtherService<AzureBlobsDataBusStorage>().Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var rebusTime = c.Get<IRebusTime>();

            var azureBlobsDataBusStorage = new AzureBlobsDataBusStorage(rebusLoggerFactory, rebusTime, options, blobContainerClient);

            return azureBlobsDataBusStorage;
        });

        configurer.Register(c => c.Get<AzureBlobsDataBusStorage>());

        configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<AzureBlobsDataBusStorage>());

        return options;
    }
}