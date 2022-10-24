using Rebus.DataBus;

namespace Rebus.Config;

/// <summary>
/// Options builder to configure additional settings
/// </summary>
public class AzureBlobsDataBusStorageOptions
{
    internal bool AutomaticallyCreateContainer { get; private set; } = true;
        
    internal bool UpdateLastReadTime { get; private set; } = true;

    /// <summary>
    /// Disables the automatic container creation. It's necessary to call this if connection strings without Manage rights
    /// are used, or if container URIs with SAS tokens with limited access are used
    /// </summary>
    public AzureBlobsDataBusStorageOptions DoNotCreateContainer()
    {
        AutomaticallyCreateContainer = false;
        return this;
    }

    /// <summary>
    /// Disables updating the <see cref="MetadataKeys.ReadTime"/> property of the blob every time it is read
    /// </summary>
    public AzureBlobsDataBusStorageOptions DoNotUpdateLastReadTime()
    {
        UpdateLastReadTime = false;
        return this;
    }
}