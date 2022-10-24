using NUnit.Framework;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.AzureBlobs.Tests.DataBus;

[TestFixture]
public class AzureBlobsDataBusStorageTest : GeneralDataBusStorageTests<AzureBlobsDataBusStorageFactory> { }