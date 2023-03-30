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
    const string ContentType = "application/json; charset=utf-8";
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
        _blobContainerClient.CreateIfNotExists();
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

        var options = new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = ContentType } };

        await dataBlob.UploadAsync(new BinaryData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(sagaData, DataSettings))), options);
        await metaDataBlob.UploadAsync(new BinaryData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(sagaAuditMetadata, MetadataSettings))), options);
    }

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