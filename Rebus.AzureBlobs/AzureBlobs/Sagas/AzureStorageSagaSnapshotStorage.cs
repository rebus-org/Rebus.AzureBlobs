using Newtonsoft.Json;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Rebus.AzureBlobs.Sagas;

/// <summary>
/// Implementation of <see cref="ISagaSnapshotStorage"/> that uses blobs to store saga data snapshots
/// </summary>
public class AzureStorageSagaSnapshotStorage : ISagaSnapshotStorage
{
    static readonly JsonSerializerSettings DataSettings = new() { TypeNameHandling = TypeNameHandling.All };
    static readonly JsonSerializerSettings MetadataSettings = new() { TypeNameHandling = TypeNameHandling.None };
    static readonly Encoding TextEncoding = Encoding.UTF8;

    readonly BlobContainerClient _blobContainerClient;
    readonly ILog _log;

    /// <summary>
    /// Creates the storage
    /// </summary>
    public AzureStorageSagaSnapshotStorage(BlobContainerClient blobContainerClient, IRebusLoggerFactory loggerFactory)
    {
        if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
        _blobContainerClient = blobContainerClient ?? throw new ArgumentNullException(nameof(blobContainerClient));
        _log = loggerFactory.GetLogger<AzureStorageSagaSnapshotStorage>();
    }

    /// <summary>
    /// Archives the given saga data under its current ID and revision
    /// </summary>
    public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
    {
        var dataRef = $"{sagaData.Id:N}/{sagaData.Revision:0000000000}/data.json";
        var metaDataRef = $"{sagaData.Id:N}/{sagaData.Revision:0000000000}/metadata.json";
        var dataBlob = _blobContainerClient.GetBlobClient(dataRef);
        var metaDataBlob = _blobContainerClient.GetBlobClient(metaDataRef);

        //dataBlob.Properties.ContentType = "application/json";
        //metaDataBlob.Properties.ContentType = "application/json";

        //await dataBlob.SetHttpHeadersAsync(new BlobHttpHeaders { ContentType = "application/json" });
        //await metaDataBlob.SetHttpHeadersAsync(new BlobHttpHeaders { ContentType = "application/json" });

        //await dataBlob.UploadTextAsync(JsonConvert.SerializeObject(sagaData, DataSettings), TextEncoding, DefaultAccessCondition, DefaultRequestOptions, new OperationContext());
        //await metaDataBlob.UploadTextAsync(JsonConvert.SerializeObject(sagaAuditMetadata, MetadataSettings), TextEncoding, DefaultAccessCondition, DefaultRequestOptions, new OperationContext());

        await dataBlob.UploadAsync(JsonConvert.SerializeObject(sagaData, DataSettings), new BlobHttpHeaders { ContentType = "application/json" });
        await metaDataBlob.UploadAsync(JsonConvert.SerializeObject(sagaAuditMetadata, MetadataSettings), new BlobHttpHeaders { ContentType = "application/json" });

        //await dataBlob.SetPropertiesAsync();
        //await metaDataBlob.SetPropertiesAsync();
    }

    //static BlobRequestOptions DefaultRequestOptions => new BlobRequestOptions { RetryPolicy = new ExponentialRetry() };

    //static AccessCondition DefaultAccessCondition => AccessCondition.GenerateEmptyCondition();

    /// <summary>
    /// Gets all blobs in the snapshot container
    /// </summary>
    public IEnumerable<BlobItem> ListAllBlobs()
    {
        foreach (var page in _blobContainerClient.GetBlobs().AsPages())
        {
            foreach (var item in page.Values)
            {
                yield return item;
            }
        }

        //BlobContinuationToken continuationToken = null;

        //while (true)
        //{
        //    var result = AsyncHelpers.GetResult(() => _blobContainerClient.ListBlobsSegmentedAsync("", true, BlobListingDetails.None, 100, continuationToken, DefaultRequestOptions, new OperationContext()));

        //    foreach (var item in result.Results)
        //    {
        //        yield return item;
        //    }

        //    continuationToken = result.ContinuationToken;

        //    if (continuationToken == null) break;
        //}
    }

    /// <summary>
    /// Creates the blob container if it doesn't exist
    /// </summary>
    public void EnsureContainerExists()
    {
        if (_blobContainerClient.Exists()) return;

        _log.Info("Container {containerName} did not exist - it will be created now", _blobContainerClient.Name);

        _blobContainerClient.CreateIfNotExists();
    }

    static string GetBlobData(BlobClient blob)
    {
        var response = blob.Download();
        var info = response.Value;

        using var reader = new StreamReader(info.Content, TextEncoding);

        return reader.ReadToEnd();
        //return info

        //return AsyncHelpers.GetResult(() => cloudBlockBlob.DownloadTextAsync(TextEncoding, new AccessCondition(),
        //    new BlobRequestOptions { RetryPolicy = new ExponentialRetry() }, new OperationContext()));
    }

    /// <summary>
    /// Loads the saga data with the given id and revision
    /// </summary>
    public ISagaData GetSagaData(Guid sagaDataId, int revision)
    {
        var dataRef = $"{sagaDataId:N}/{revision:0000000000}/data.json";
        var dataBlob = _blobContainerClient.GetBlobClient(dataRef);
        var json = GetBlobData(dataBlob);
        return (ISagaData)JsonConvert.DeserializeObject(json, DataSettings);
    }

    /// <summary>
    /// Loads the saga metadata for the saga with the given id and revision
    /// </summary>
    public Dictionary<string, string> GetSagaMetaData(Guid sagaDataId, int revision)
    {
        var metaDataRef = $"{sagaDataId:N}/{revision:0000000000}/metadata.json";
        var metaDataBlob = _blobContainerClient.GetBlobClient(metaDataRef);
        var json = GetBlobData(metaDataBlob);
        return JsonConvert.DeserializeObject<Dictionary<string, string>>(json, MetadataSettings);
    }

    /// <summary>
    /// Drops/recreates the snapshot container
    /// </summary>
    public void DropAndRecreateContainer()
    {
        AsyncHelpers.RunSync(async () =>
        {
            await _blobContainerClient.DeleteIfExistsAsync();
            await _blobContainerClient.CreateIfNotExistsAsync();
        });
    }
}