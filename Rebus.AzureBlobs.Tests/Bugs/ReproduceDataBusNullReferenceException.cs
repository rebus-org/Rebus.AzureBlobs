using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.DataBus.ClaimCheck;
using Rebus.Encryption;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;

namespace Rebus.AzureBlobs.Tests.Bugs;

[TestFixture]
public class ReproduceDataBusNullReferenceException : FixtureBase
{
    const string ValidEncryptionKey = "yFvUZZJaCLPjoQztB+UNzMbh21v7fa0BXo2b6db6Zz0=";

    [Test]
    public async Task CanSendBigMessageWithAutomaticDataBus()
    {
        var containerName = Guid.NewGuid().ToString("N");

        using var _ = AzureConfig.ContainerDeleter(containerName);

        using var bus = Configure.With(new BuiltinHandlerActivator())
            .Transport(t => t.UseInMemoryTransport(new(), "whatever"))
            .Options(o => o.EnableEncryption(ValidEncryptionKey))
            .DataBus(d =>
            {
                d.StoreInBlobStorage(AzureConfig.ConnectionString, containerName);

                // just send all messages via data bus
                d.SendBigMessagesAsAttachments(bodySizeThresholdBytes: 0);

                d.EnableEncryption();
            })
            .Start();

        await bus.SendLocal("HEJ MED DIG MIN VEN 🙂");
    }
}