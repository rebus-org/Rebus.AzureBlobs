using System;
using Microsoft.Azure.Storage;
using System.Net;
//using Microsoft.WindowsAzure.Storage.Table;

namespace Rebus.AzureBlobs;

static class AzureEx
{
    public static bool IsStatus(this StorageException exception, HttpStatusCode statusCode)
    {
        if (exception == null) throw new ArgumentNullException(nameof(exception));
        return exception.RequestInformation.HttpStatusCode == (int) statusCode;
    }
}