using System;
using System.Collections.Concurrent;
using Azure.Storage.Blobs;
using Rebus.AzureBlobs.Retries;
using Rebus.AzureBlobs.Tests.Extensions;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Tests.Contracts.Errors;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport.InMem;

namespace Rebus.AzureBlobs.Tests.Retries;

public class AzureBlobsErrorTrackerFactory : IErrorTrackerFactory
{
    readonly ConcurrentStack<IDisposable> _disposables = new();

    public IErrorTracker Create(RetryStrategySettings settings)
    {
        var blobContainerClient = new BlobContainerClient(AzureConfig.ConnectionString, Guid.NewGuid().ToString("n"));
        _disposables.Push(blobContainerClient.AsDisposable(c => c.DeleteIfExists()));
        return new AzureBlobsErrorTracker(blobContainerClient, settings, new InMemTransport(new(), "queue-name"));
    }

    public void Dispose() => _disposables.Dispose();
}