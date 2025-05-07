using System;
using Azure.Core;
using Azure.Storage.Blobs;
using Rebus.AzureBlobs.Retries;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Transport;

namespace Rebus.Config;

public static class AzureBlobsErrorTrackerConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use blob storage for storing tracking errors
    /// </summary>
    public static void UseBlobStorageErrorTracker(this StandardConfigurer<IErrorTracker> configurer, string connectionString, string containerName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (containerName == null) throw new ArgumentNullException(nameof(containerName));

        configurer.Register(c =>
        {
            var blobContainerClient = new BlobContainerClient(connectionString, containerName);
            var retryStrategySettings = c.Get<RetryStrategySettings>();
            var transport = c.Get<ITransport>();
            var exceptionLogger = c.Get<IExceptionLogger>();
            return new AzureBlobsErrorTracker(blobContainerClient, retryStrategySettings, transport, exceptionLogger);
        });
    }

    /// <summary>
    /// Configures Rebus to use blob storage for storing tracking errors
    /// </summary>
    public static void UseBlobStorageErrorTracker(this StandardConfigurer<IErrorTracker> configurer, Uri containerUri, TokenCredential credentials)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (containerUri == null) throw new ArgumentNullException(nameof(containerUri));
        if (credentials == null) throw new ArgumentNullException(nameof(credentials));

        configurer.Register(c =>
        {
            var blobContainerClient = new BlobContainerClient(containerUri, credentials);
            var retryStrategySettings = c.Get<RetryStrategySettings>();
            var transport = c.Get<ITransport>();
            var exceptionLogger = c.Get<IExceptionLogger>();
            return new AzureBlobsErrorTracker(blobContainerClient, retryStrategySettings, transport, exceptionLogger);
        });
    }
}