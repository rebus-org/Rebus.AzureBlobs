using System;
using Rebus.Time;

namespace Rebus.AzureBlobs.Tests.DataBus;

class FakeRebusTime : IRebusTime
{
    DateTimeOffset? _fakeNow;

    public DateTimeOffset Now => _fakeNow ?? DateTimeOffset.Now;

    public void SetNow(DateTimeOffset time)
    {
        _fakeNow = time;
    }
}