# Rebus.AzureBlobs

[![install from nuget](https://img.shields.io/nuget/v/Rebus.AzureBlobs.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.AzureBlobs)

Provides Azure Blobs-based implementation for [Rebus](https://github.com/rebus-org/Rebus) of

* data bus storage (in blobs)
* saga auditing snapshots (blobs)

You can configure the blob-based data bus like this:

```csharp
var storageAccount = CloudStorageAccount.Parse(connectionString);

Configure.With(...)
	.(...)
	.DataBus(d => d.StoreInBlobStorage(storageAccount, "container-name"))
	.Start();

```

or, if you're using Rebus 5 or earlier, you need to do it like this:

```csharp
var storageAccount = CloudStorageAccount.Parse(connectionString);

Configure.With(...)
	.(...)
	.Options(o => o.EnableDataBus().StoreInBlobStorage(storageAccount, "container-name"))
	.Start();

```


![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---


