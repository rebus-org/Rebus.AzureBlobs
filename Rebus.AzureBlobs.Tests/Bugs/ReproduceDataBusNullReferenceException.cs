using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
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

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanSendBigMessageWithAutomaticDataBus(bool createContainerManually)
    {
        var containerName = Guid.NewGuid().ToString("N");

        if (createContainerManually)
        {
            // ensure container exists
            await new BlobContainerClient(AzureConfig.ConnectionString, containerName).CreateIfNotExistsAsync();
        }

        // and remove it after the test
        using var _ = AzureConfig.ContainerDeleter(containerName);

        using var bus = Configure.With(new BuiltinHandlerActivator())
            .Transport(t => t.UseInMemoryTransport(new(), "whatever"))
            .Options(o => o.EnableEncryption(ValidEncryptionKey))
            .DataBus(d =>
            {
                var options = d.StoreInBlobStorage(AzureConfig.ConnectionString, containerName);

                if (createContainerManually)
                {
                    options.DoNotCreateContainer();
                }

                // just send all messages via data bus
                d.SendBigMessagesAsAttachments(bodySizeThresholdBytes: 0);

                d.EnableEncryption();
            })
            .Start();

        await bus.SendLocal("HEJ MED DIG MIN VEN 🙂");
    }
}