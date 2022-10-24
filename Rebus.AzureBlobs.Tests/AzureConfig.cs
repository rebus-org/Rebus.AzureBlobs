using Microsoft.Azure.Storage;
using System;

namespace Rebus.AzureBlobs.Tests;

public static class AzureConfig
{
    public static CloudStorageAccount StorageAccount => CloudStorageAccount.Parse(ConnectionString);

    public static string ConnectionString => ConnectionStringFromEnvironmentVariable("rebus2_storage_connection_string")
                                             ?? "UseDevelopmentStorage=true";

    static string ConnectionStringFromEnvironmentVariable(string environmentVariableName)
    {
        var value = Environment.GetEnvironmentVariable(environmentVariableName);

        if (value == null)
        {
            Console.WriteLine("Could not find env variable {0}", environmentVariableName);
            return null;
        }

        Console.WriteLine("Using Azure Storage connection string from env variable {0}", environmentVariableName);

        return value;
    }
}