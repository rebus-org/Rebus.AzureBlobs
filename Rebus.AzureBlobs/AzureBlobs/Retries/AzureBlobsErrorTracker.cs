using System;
using System.Collections.Generic;
using System.Globalization;
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
    readonly IExceptionLogger _exceptionLogger;

    public AzureBlobsErrorTracker(BlobContainerClient blobContainerClient, RetryStrategySettings settings,
        ITransport transport, IExceptionLogger exceptionLogger)
    {
        if (blobContainerClient == null) throw new ArgumentNullException(nameof(blobContainerClient));
        _blobContainerClient = new(async () =>
        {
            await blobContainerClient.CreateIfNotExistsAsync();
            return blobContainerClient;
        });
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _exceptionLogger = exceptionLogger ?? throw new ArgumentNullException(nameof(exceptionLogger));
    }

    public async Task RegisterError(string messageId, Exception exception)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetAppendBlobClient(GetBlobName(messageId));

        var exceptionInfo = ExceptionInfo.FromException(exception);
        var json = JsonConvert.SerializeObject(exceptionInfo);
        var bytes = Encoding.UTF8.GetBytes(json + LineSeparator);

        using var source = new MemoryStream(bytes);

        await blob.CreateIfNotExistsAsync();

        var properties = await blob.GetPropertiesAsync();
        var metadata = properties.Value.Metadata;
        var errorCount = metadata.TryGetValue("ErrorCount", out var value) && int.TryParse(value, out var result) ? result : 0;

        await blob.AppendBlockAsync(source);

        errorCount++;

        await blob.SetMetadataAsync(new Dictionary<string, string> { ["ErrorCount"] = errorCount.ToString(CultureInfo.InvariantCulture) });

        var isFinal = metadata.TryGetValue("IsFinal", out var str) && string.Equals(str, "true", StringComparison.OrdinalIgnoreCase)
                      || errorCount >= _settings.MaxDeliveryAttempts;

        _exceptionLogger.LogException(messageId, exception, errorCount, isFinal);
    }

    public async Task CleanUp(string messageId)
    {
        var blobContainerClient = await GetBlobContainerClient();
        var blob = blobContainerClient.GetAppendBlobClient(GetBlobName(messageId));

        await blob.DeleteIfExistsAsync();
    }

    public async Task<bool> HasFailedTooManyTimes(string messageId)
    {
        var (lines, isFinal) = await GetLines(messageId);

        return isFinal || lines.Count >= _settings.MaxDeliveryAttempts;
    }

    public async Task<string> GetFullErrorDescription(string messageId)
    {
        var (lines, _) = await GetLines(messageId);

        return string.Join(Environment.NewLine, lines);
    }

    public async Task<IReadOnlyList<ExceptionInfo>> GetExceptions(string messageId)
    {
        var (lines, _) = await GetLines(messageId);

        return lines
            .Select(JsonConvert.DeserializeObject<ExceptionInfo>)
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
            var info = response.Value;

            using var reader = new StreamReader(info.Content, Encoding.UTF8);

            var text = await reader.ReadToEndAsync();

            var lines = text.Split(new[] { LineSeparator }, StringSplitOptions.RemoveEmptyEntries);

            var isFinal = info.Details.Metadata.TryGetValue("IsFinal", out var result)
                          && string.Equals(result, "true", StringComparison.OrdinalIgnoreCase);

            return (lines, isFinal);
        }
        catch (RequestFailedException exception) when ((HttpStatusCode)exception.Status == HttpStatusCode.NotFound)
        {
            return (Array.Empty<string>(), false);
        }
    }

    async Task<BlobContainerClient> GetBlobContainerClient() => await _blobContainerClient.Value;

    string GetBlobName(string messageId) => $"{_transport.Address}/{messageId}-errors.jsonl";
}