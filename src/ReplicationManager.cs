namespace Server;

public class ReplicationManager
{
    private static readonly ReplicationManager _instance = new();
    public static ReplicationManager Instance => _instance;

    private readonly List<ClientState> _slaves = new();

    public void RegisterSlave(ClientState slave)
    {
        if (!_slaves.Contains(slave))
            _slaves.Add(slave);
    }

    public void DispatchToSlaves(byte[] buffer)
    {
        foreach (var slave in _slaves)
        {
            slave.PendingWrites.Enqueue(buffer);
        }
    }
}