using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.AzureBlobs.Tests.Sagas;

[TestFixture]
public class AzureStorageSnapshotStorageTests : SagaSnapshotStorageTest<AzureStorageSagaSnapshotStorageFactory>
{
}