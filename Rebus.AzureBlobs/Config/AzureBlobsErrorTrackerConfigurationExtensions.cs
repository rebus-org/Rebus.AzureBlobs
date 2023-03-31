using System;
using Azure.Storage.Blobs;
using Rebus.AzureBlobs.Retries;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Transport;

namespace Rebus.Config;

public static class AzureBlobsErrorTrackerConfigurationExtensions
{
    public static void UseBlobStorage(this StandardConfigurer<IErrorTracker> configurer, string connectionString, string containerName)
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
}