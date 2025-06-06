using Rebus.Auditing.Sagas;
using Rebus.AzureBlobs.Sagas;
using Rebus.Logging;
using System;
using Azure.Storage.Blobs;
using Azure.Core;

// ReSharper disable UnusedMember.Global

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for Azure storage
/// </summary>
public static class AzureStorageSagaConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to store saga data snapshots in blob storage
    /// </summary>
    public static void StoreInBlobStorage(this StandardConfigurer<ISagaSnapshotStorage> configurer, string storageAccountConnectionString, string containerName = "RebusSagaStorage")
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));
        if (containerName == null) throw new ArgumentNullException(nameof(containerName));

        var blobContainerClient = new BlobContainerClient(storageAccountConnectionString, containerName);

        configurer.Register(c => new AzureStorageSagaSnapshotStorage(blobContainerClient, c.Get<IRebusLoggerFactory>()));
    }

    /// <summary>
    /// Configures Rebus to store saga data snapshots in blob storage
    /// </summary>
    public static void StoreInBlobStorage(this StandardConfigurer<ISagaSnapshotStorage> configurer, Uri containerUri, TokenCredential credentials)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (containerUri == null) throw new ArgumentNullException(nameof(containerUri));
        if (credentials == null) throw new ArgumentNullException(nameof(credentials));

        var blobContainerClient = new BlobContainerClient(containerUri, credentials);

        configurer.Register(c => new AzureStorageSagaSnapshotStorage(blobContainerClient, c.Get<IRebusLoggerFactory>()));
    }
}