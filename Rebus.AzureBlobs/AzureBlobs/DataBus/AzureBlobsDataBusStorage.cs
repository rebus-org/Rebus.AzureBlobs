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
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
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
    readonly BlobContainerClient _blobContainerClient;
    readonly IRebusTime _rebusTime;
    readonly string _containerName;
    readonly ILog _log;

    bool _containerInitialized;

    /// <summary>
    /// Creates the data bus storage
    /// </summary>
    public AzureBlobsDataBusStorage(IRebusLoggerFactory loggerFactory, IRebusTime rebusTime, AzureBlobsDataBusStorageOptions options, BlobContainerClient blobContainerClient)
    {
        if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
        _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _blobContainerClient = blobContainerClient ?? throw new ArgumentNullException(nameof(blobContainerClient));
        _containerName = blobContainerClient.Name;
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
                if (!await _blobContainerClient.ExistsAsync())
                {
                    _log.Info("Container {containerName} does not exist - will create it now", _containerName);
                    await _blobContainerClient.CreateIfNotExistsAsync();
                }

                _containerInitialized = true;
            }
        }

        var blobName = GetBlobName(id);

        try
        {
            var standardMetadata = new Dictionary<string, string>
            {
                {MetadataKeys.SaveTime, _rebusTime.Now.ToString("O")}
            };

            var metadataToWrite = standardMetadata
                .MergedWith(metadata ?? new Dictionary<string, string>());

            var blob = _blobContainerClient.GetBlobClient(blobName);
            await blob.UploadAsync(source, metadata: metadataToWrite);
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
            var blob = _blobContainerClient.GetBlobClient(blobName);

            if (_options.UpdateLastReadTime)
            {
                await UpdateLastReadTime(blob);
            }

            return await blob.OpenReadAsync(new BlobOpenReadOptions(allowModifications: true));

        }
        catch (RequestFailedException exception) when ((HttpStatusCode)exception.Status == HttpStatusCode.NotFound)
        {
            throw new ArgumentException($"Could not find blob named '{blobName}' in the '{_containerName}' container", exception);
        }
    }

    async Task UpdateLastReadTime(BlobClient blob)
    {
        await blob.SetMetadataAsync(new Dictionary<string, string>
        {
            [MetadataKeys.ReadTime] = _rebusTime.Now.ToString("O")
        });
    }

    /// <summary>
    /// Loads the metadata stored with the given ID
    /// </summary>
    public async Task<Dictionary<string, string>> ReadMetadata(string id)
    {
        var blobName = GetBlobName(id);
        try
        {
            var blob = _blobContainerClient.GetBlobClient(blobName);
            var response = await blob.GetPropertiesAsync();
            var properties = response.Value;
            var metadata = properties.Metadata.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            metadata[MetadataKeys.Length] = properties.ContentLength.ToString();

            return metadata;
        }
        catch (RequestFailedException exception) when ((HttpStatusCode)exception.Status == HttpStatusCode.NotFound)
        {
            throw new ArgumentException($"Could not find blob named '{blobName}' in the '{_containerName}' container", exception);
        }
    }

    static string GetBlobName(string id) => $"data-{id.ToLowerInvariant()}.dat";

    /// <inheritdoc />
    public async Task Delete(string id)
    {
        var blobName = GetBlobName(id);

        var blob = _blobContainerClient.GetBlobClient(blobName);

        await blob.DeleteIfExistsAsync();
    }

    /// <inheritdoc />
    public IEnumerable<string> Query(TimeRange readTime = null, TimeRange saveTime = null)
    {
        static bool IsWithin(TimeRange timeRange, DateTimeOffset time)
        {
            return time >= (timeRange?.From ?? DateTimeOffset.MinValue)
                   && time < (timeRange?.To ?? DateTimeOffset.MaxValue);
        }

        var blobs = _blobContainerClient.GetBlobs();

        foreach (var page in blobs.AsPages())
        {
            foreach (var item in page.Values)
            {
                var fileName = Path.GetFileNameWithoutExtension(item.Name);
                if (fileName == null) continue;

                var id = fileName.Split('-').Last();

                if (string.IsNullOrWhiteSpace(id)) continue;

                if (readTime == null && saveTime == null)
                {
                    yield return id;
                    continue;
                }

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

        }
    }
}