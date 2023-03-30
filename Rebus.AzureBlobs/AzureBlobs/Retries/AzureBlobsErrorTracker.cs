using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Newtonsoft.Json;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Transport;

namespace Rebus.AzureBlobs.Retries;

public class AzureBlobsErrorTracker : IErrorTracker
{
    /// <summary>
    /// Platform-independent line separator
    /// </summary>
    const string LineSeparator = "\r\n";
    readonly Lazy<Task<BlobContainerClient>> _blobContainerClient;
    readonly RetryStrategySettings _settings;
    readonly ITransport _transport;

    public AzureBlobsErrorTracker(BlobContainerClient blobContainerClient, RetryStrategySettings settings, ITransport transport)
    {
        if (blobContainerClient == null) throw new ArgumentNullException(nameof(blobContainerClient));
        _blobContainerClient = new(async () =>
        {
            await blobContainerClient.CreateIfNotExistsAsync();
            return blobContainerClient;
        });
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    }

    public async Task RegisterError(string messageId, Exception exception)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetAppendBlobClient(GetBlobName(messageId));

        var errorLog = new ErrorLog { Time = DateTimeOffset.Now, Details = exception.ToString() };
        var json = JsonConvert.SerializeObject(errorLog);
        var bytes = Encoding.UTF8.GetBytes(json + LineSeparator);

        using var source = new MemoryStream(bytes);

        await blob.CreateIfNotExistsAsync();
        await blob.AppendBlockAsync(source);
    }

    public async Task CleanUp(string messageId)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetAppendBlobClient(GetBlobName(messageId));

        await blob.DeleteIfExistsAsync();
    }

    public async Task<bool> HasFailedTooManyTimes(string messageId)
    {
        var (lines, _) = await GetLines(messageId);

        return lines.Count >= _settings.MaxDeliveryAttempts;
    }

    public async Task<string> GetFullErrorDescription(string messageId)
    {
        var (lines, _) = await GetLines(messageId);

        return string.Join(Environment.NewLine, lines);
    }

    public async Task<IReadOnlyList<Exception>> GetExceptions(string messageId)
    {
        var (lines, _) = await GetLines(messageId);

        return lines
            .Select(line => JsonConvert.DeserializeObject<Exception>(line))
            .ToList();
    }

    public async Task MarkAsFinal(string messageId)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetBlobClient(GetBlobName(messageId));

        await blob.SetMetadataAsync(new Dictionary<string, string> { ["IsFinal"] = "true" });
    }

    async Task<(IReadOnlyList<string>, bool)> GetLines(string messageId)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetBlobClient(GetBlobName(messageId));

        try
        {
            var response = await blob.DownloadAsync();

            using var reader = new StreamReader(response.Value.Content, Encoding.UTF8);

            var text = await reader.ReadToEndAsync();

            var lines = text.Split(new[] { LineSeparator }, StringSplitOptions.RemoveEmptyEntries);

            return (lines, false);
        }
        catch (RequestFailedException exception) when ((HttpStatusCode)exception.Status == HttpStatusCode.NotFound)
        {
            return (Array.Empty<string>(), false);
        }
    }

    async Task<BlobContainerClient> GetBlobContainerClient() => await _blobContainerClient.Value;

    string GetBlobName(string messageId) => $"{_transport.Address}/{messageId}-errors.jsonl";

    class ErrorLog
    {
        public DateTimeOffset Time { get; set; }
        public string Details { get; set; }
    }
}