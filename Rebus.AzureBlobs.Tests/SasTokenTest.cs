using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.AzureBlobs.Tests.Extensions;
using Rebus.Config;
using Rebus.DataBus;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport.InMem;
// ReSharper disable RedundantArgumentDefaultValue
// ReSharper disable VariableHidesOuterVariable

namespace Rebus.AzureBlobs.Tests;

[TestFixture]
[Description("Verifies that endpoints can be started, even with the most restricted rights required to fulfill their purpose")]
public class SasTokenTest : FixtureBase
{
    BlobContainerClient _container;

    protected override void SetUp()
    {
        _container = new BlobContainerClient(AzureConfig.ConnectionString, Guid.NewGuid().ToString("N"));

        Using(_container.AsDisposable(c => c.DeleteIfExists()));
    }

    [Test]
    public async Task ItWorks()
    {
        // now that the container is not automatically created, we need to do this
        await _container.CreateIfNotExistsAsync();

        var sasWithReadAccess = GetSas(BlobContainerSasPermissions.Read);
        var sasWithWriteAccess = GetSas(BlobContainerSasPermissions.Write);

        var receivedMessages = new ConcurrentQueue<string>();
        var network = new InMemNetwork();

        // this is the receiver
        using var receiver = new BuiltinHandlerActivator();

        receiver.Handle<DataBusAttachment>(async attachment =>
        {
            await using var source = await attachment.OpenRead();
            
            using var reader = new StreamReader(source);

            receivedMessages.Enqueue(await reader.ReadToEndAsync());
        });

        Configure.With(receiver)
            .Transport(t => t.UseInMemoryTransport(network, "receiver"))
            .DataBus(d => d.StoreInBlobStorage(new Uri(sasWithReadAccess)).DoNotUpdateLastReadTime())
            .Start();

        // create a sender
        using var sender = new BuiltinHandlerActivator();

        Configure.With(sender)
            .Transport(t => t.UseInMemoryTransport(network, "sender"))
            .DataBus(d => d.StoreInBlobStorage(new Uri(sasWithWriteAccess)).DoNotCreateContainer())
            .Routing(t => t.TypeBased().Map<DataBusAttachment>("receiver"))
            .Start();

        // send attachment
        const string expectedText = "hey ven! 🍺";

        using var bytes = new MemoryStream(Encoding.UTF8.GetBytes(expectedText));
        var attachment = await sender.Bus.Advanced.DataBus.CreateAttachment(bytes);

        await sender.Bus.Send(attachment);

        await receivedMessages.WaitUntil(q => q.Count == 1, timeoutSeconds: 5);
    }

    string GetSas(BlobContainerSasPermissions permissions)
    {
        var uri = _container.GenerateSasUri(permissions, DateTimeOffset.Now.AddDays(1));

        return uri.ToString();
    }
}