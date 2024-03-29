﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.AzureBlobs.Tests.Extensions;
using Rebus.Config;
using Rebus.DataBus;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport.InMem;
using LogLevel = Rebus.Logging.LogLevel;
// ReSharper disable AccessToDisposedClosure

namespace Rebus.AzureBlobs.Tests.Bugs;

[TestFixture]
public class ReproduceRaceConditionWhenReadingDataBusAttachments : FixtureBase
{
    BlobContainerClient _container;
    string _containerName;
    private string _connectionString;

    protected override void SetUp()
    {
        base.SetUp();

        _connectionString = AzureConfig.ConnectionString;
        _containerName = Guid.NewGuid().ToString("N");
        _container = new BlobContainerClient(_connectionString, _containerName);

        Using(_container.AsDisposable(c => c.DeleteIfExists()));
    }

    [TestCase(20)]
    public async Task JustDoItWithTheBus(int count)
    {
        var failures = new ConcurrentQueue<Exception>();

        using var sharedCounter = new SharedCounter(initialValue: count);
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<DataBusAttachment>(async attachment =>
        {
            try
            {
                await using var source = await attachment.OpenRead();

                _ = await new StreamReader(source).ReadToEndAsync();
            }
            catch (Exception exception)
            {
                failures.Enqueue(exception);
            }

            sharedCounter.Decrement();
        });

        Configure.With(activator)
            .Logging(l => l.Console(minLevel: LogLevel.Warn))
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "who-cares"))
            .DataBus(d => d.StoreInBlobStorage(_connectionString, _containerName))
            .Options(o =>
            {
                o.SetNumberOfWorkers(10);
                o.SetMaxParallelism(100);
            })
            .Start();

        var data = Enumerable.Range(0, 10 * 1024 * 1024).Select(_ => (byte)Random.Shared.Next(256)).ToArray();

        using var source = new MemoryStream(data);

        var attachment = await activator.Bus.Advanced.DataBus.CreateAttachment(source);

        await Task.WhenAll(Enumerable.Range(0, count).Select(_ => activator.Bus.SendLocal(attachment)));

        sharedCounter.WaitForResetEvent(timeoutSeconds: 20);

        Assert.That(failures.Count, Is.EqualTo(0), $@"Number of failures was > 0 – here's the first one:

{failures.FirstOrDefault()}");
    }
}