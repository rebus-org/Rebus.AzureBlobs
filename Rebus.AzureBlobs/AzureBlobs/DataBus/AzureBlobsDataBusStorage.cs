using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.RetryPolicies;
using Rebus.DataBus;
using Rebus.Extensions;
using Rebus.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Rebus.Config;
using Rebus.Time;
// ReSharper disable ArgumentsStyleOther
// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.AzureBlobs.DataBus;

/// <summary>
/// Implementation of <see cref="IDataBusStorage"/> that uses Azure blobs to store data
/// </summary>
public class AzureBlobsDataBusStorage : IDataBusStorage, IDataBusStorageManagement
{
    readonly AzureBlobsDataBusStorageOptions _options;
    readonly CloudBlobContainer _container;
    readonly IRebusTime _rebusTime;
    readonly string _containerName;
    readonly ILog _log;

    bool _containerInitialized;

    /// <summary>
    /// Creates the data bus storage
    /// </summary>
    public AzureBlobsDataBusStorage(IRebusLoggerFactory loggerFactory, IRebusTime rebusTime, AzureBlobsDataBusStorageOptions options, CloudBlobContainer cloudBlobContainer)
    {
        if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
        _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _container = cloudBlobContainer ?? throw new ArgumentNullException(nameof(cloudBlobContainer));
        _containerName = cloudBlobContainer.Name;
        _log = loggerFactory.GetLogger<AzureBlobsDataBusStorage>();
    }

    /// <summary>
    /// Saves the data from the given source stream under the given ID
    /// </summary>
    public async Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
    {
        if (_options.AutomaticallyCreateContainer)
        {
            if (!_containerInitialized)
            {
                if (!await _container.ExistsAsync())
                {
                    _log.Info("Container {containerName} does not exist - will create it now", _containerName);
                    await _container.CreateIfNotExistsAsync();
                }

                _containerInitialized = true;
            }
        }

        var blobName = GetBlobName(id);

        try
        {
            var blob = _container.GetBlockBlobReference(blobName);

            var standardMetadata = new Dictionary<string, string>
            {
                {MetadataKeys.SaveTime, _rebusTime.Now.ToString("O")}
            };

            var metadataToWrite = standardMetadata
                .MergedWith(metadata ?? new Dictionary<string, string>());

            foreach (var kvp in metadataToWrite)
            {
                blob.Metadata[kvp.Key] = kvp.Value;
            }

            await blob.UploadFromStreamAsync(
                source: source,
                accessCondition: AccessCondition.GenerateEmptyCondition(),
                options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
                operationContext: new OperationContext()
            );
        }
        catch (Exception exception)
        {
            throw new IOException($"Could not upload data to blob named '{blobName}' in the '{_containerName}' container", exception);
        }
    }

    /// <summary>
    /// Opens the data stored under the given ID for reading
    /// </summary>
    public async Task<Stream> Read(string id)
    {
        var blobName = GetBlobName(id);
        try
        {
            var blob = await _container.GetBlobReferenceFromServerAsync(
                blobName: blobName,
                accessCondition: AccessCondition.GenerateEmptyCondition(),
                options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
                operationContext: new OperationContext()
            );

            if (_options.UpdateLastReadTime)
            {
                await UpdateLastReadTime(blob);
            }

            // for some reason, the access condition passed to OpenRead does not apply to the second HTTP request issued
            // from the operation - therefore, we need to hook into this if-match part and do this: 
            var operationContext = new OperationContext();

            operationContext.SendingRequest += (_, e) =>
            {
                var request = e.Request;
                var headers = request.Headers;

                if (headers.Contains("if-match"))
                {
                    headers.Remove("if-match");
                }

                headers.Add("if-match", "*");
            };

            return await blob.OpenReadAsync(
                accessCondition: AccessCondition.GenerateEmptyCondition(),
                options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
                operationContext: operationContext
            );
        }
        catch (StorageException exception) when (exception.IsStatus(HttpStatusCode.NotFound))
        {
            throw new ArgumentException(
                $"Could not find blob named '{blobName}' in the '{_containerName}' container", exception);
        }
    }

    async Task UpdateLastReadTime(ICloudBlob blob)
    {
        blob.Metadata[MetadataKeys.ReadTime] = _rebusTime.Now.ToString("O");

        await blob.SetMetadataAsync(
            accessCondition: AccessCondition.GenerateEmptyCondition(),
            options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
            operationContext: new OperationContext()
        );
    }

    /// <summary>
    /// Loads the metadata stored with the given ID
    /// </summary>
    public async Task<Dictionary<string, string>> ReadMetadata(string id)
    {
        var blobName = GetBlobName(id);
        try
        {
            var blob = await _container.GetBlobReferenceFromServerAsync(
                blobName: blobName,
                accessCondition: AccessCondition.GenerateEmptyCondition(),
                options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
                operationContext: new OperationContext()
            );

            var metadata = new Dictionary<string, string>(blob.Metadata)
            {
                [MetadataKeys.Length] = blob.Properties.Length.ToString()
            };

            return metadata;
        }
        catch (StorageException exception) when (exception.IsStatus(HttpStatusCode.NotFound))
        {
            throw new ArgumentException($"Could not find blob named '{blobName}' in the '{_containerName}' container", exception);
        }
    }

    static string GetBlobName(string id) => $"data-{id.ToLowerInvariant()}.dat";

    /// <inheritdoc />
    public async Task Delete(string id)
    {
        var blobName = GetBlobName(id);

        try
        {
            var blob = await _container.GetBlobReferenceFromServerAsync(
                blobName: blobName,
                accessCondition: AccessCondition.GenerateEmptyCondition(),
                options: new BlobRequestOptions { RetryPolicy = new ExponentialRetry() },
                operationContext: new OperationContext()
            );

            await blob.DeleteAsync();
        }
        catch (StorageException exception) when (exception.IsStatus(HttpStatusCode.NotFound))
        {
            // it's ok
        }
    }

    /// <inheritdoc />
    public IEnumerable<string> Query(TimeRange readTime = null, TimeRange saveTime = null)
    {
        BlobContinuationToken blobContinuationToken = null;

        do
        {
            var results = _container.ListBlobsSegmented(blobContinuationToken);

            foreach (var result in results.Results.OfType<CloudBlockBlob>())
            {
                var fileName = Path.GetFileNameWithoutExtension(result.Name);
                if (fileName == null) continue;

                var id = fileName.Split('-').Last();

                if (string.IsNullOrWhiteSpace(id)) continue;

                // accelerate querying without criteria
                if (readTime == null && saveTime == null) yield return id;

                var metadata = AsyncHelpers.GetResult(() => ReadMetadata(id));

                if (readTime != null)
                {
                    if (metadata.TryGetValue(MetadataKeys.ReadTime, out var readTimeString))
                    {
                        if (DateTimeOffset.TryParseExact(readTimeString, "o", CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind, out var readTimeValue))
                        {
                            if (!IsWithin(readTime, readTimeValue)) continue;
                        }
                    }
                }

                if (saveTime != null)
                {
                    if (metadata.TryGetValue(MetadataKeys.SaveTime, out var saveTimeString))
                    {
                        if (DateTimeOffset.TryParseExact(saveTimeString, "o", CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind, out var saveTimeValue))
                        {
                            if (!IsWithin(saveTime, saveTimeValue)) continue;
                        }
                    }
                }

                yield return id;
            }

            blobContinuationToken = results.ContinuationToken;

        } while (blobContinuationToken != null);
    }

    static bool IsWithin(TimeRange timeRange, DateTimeOffset time)
    {
        return time >= (timeRange?.From ?? DateTimeOffset.MinValue)
               && time < (timeRange?.To ?? DateTimeOffset.MaxValue);
    }
}