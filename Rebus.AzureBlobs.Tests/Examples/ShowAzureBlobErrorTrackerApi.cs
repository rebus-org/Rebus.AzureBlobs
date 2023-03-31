using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport.InMem;

namespace Rebus.AzureBlobs.Tests.Examples;

[TestFixture]
public class ShowAzureBlobErrorTrackerApi : FixtureBase
{
    [Test]
    public async Task CanDoIt()
    {
        var network = new InMemNetwork();

        using var activator = new BuiltinHandlerActivator();

        activator.Handle<string>(_ => throw new AbandonedMutexException("oh no the mutex has been abandoned"));

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(network, "blob-error-tracker"))
            .Errors(o => o.UseBlobStorage(AzureConfig.ConnectionString, "errors"))
            .Start();

        await activator.Bus.SendLocal("HEJ");

        _ = await network.WaitForNextMessageFrom("error");
    }
}