using System.Text;
using System.Text.Json;

namespace Server;

public class ClientStateManager
{
    private static readonly ClientStateManager _instance = new ClientStateManager();
    public static ClientStateManager Instance => _instance;

    private readonly Dictionary<string, Queue<ClientState>> _blockedKeys = new(StringComparer.Ordinal);

    public void RegisterBlocked(ClientState client, string key, long deadlineInMs, RedisCommand command)
    {
        if (client == null) throw new ArgumentNullException(nameof(client));
        if (string.IsNullOrEmpty(key)) throw new ArgumentException(nameof(key));

        client.BlockedCommand = command;
        client.IsBlocked = true;

        if (client.BlockedKeys == null)
            client.BlockedKeys = new();

        if (!client.BlockedKeys.Contains(key))
            client.BlockedKeys.Add(key);

        client.BlockExpiryInMs = deadlineInMs == 0 ? long.MaxValue : deadlineInMs;

        Console.WriteLine("Blocking client with \nkey: {0}\nState: {1} ", key, client.ToString());

        if (_blockedKeys.TryGetValue(key, out var clientQueue))
        {
            if (clientQueue.Contains(client))
                return;

            clientQueue.Enqueue(client);
        }
        else
        {
            var queue = new Queue<ClientState>();
            queue.Enqueue(client);
            _blockedKeys.Add(key, queue);
        }
    }

    public ClientState TryUnblockOneForKey(string key)
    {
        if (string.IsNullOrEmpty(key)) return null;
        if (!_blockedKeys.TryGetValue(key, out var clientQueue) || clientQueue.Count == 0) return null;

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Console.WriteLine("Unblocking clients for \nkey: {0}", key);

        DrainExpiredFromQueue(nowMs, key, clientQueue);

        if (clientQueue.Count == 0)
        {
            _blockedKeys.Remove(key);
            return null;
        }

        var client = clientQueue.Dequeue();
        if (clientQueue.Count == 0)
            _blockedKeys.Remove(key);

        Console.WriteLine("Unblocked client with\nkey:{0} \nstate: {1}", key, client.ToString());

        return client;
    }

    public List<ClientState> ScanAndExpire()
    {
        var expired = new List<ClientState>();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        foreach (var kv in _blockedKeys)
        {
            var key = kv.Key;
            var queue = kv.Value;
            var expiredFromQueue = DrainExpiredFromQueue(nowMs, key, queue);
            if (expiredFromQueue?.Count > 0)
                expired.AddRange(expiredFromQueue);

            if (queue.Count == 0)
                _blockedKeys.Remove(key);
        }

        return expired;
    }

    public void RemoveBlockedClientFromAllKeys(ClientState client)
    {
        if (client == null) return;

        foreach (var kv in _blockedKeys)
        {
            var key = kv.Key;
            var queue = kv.Value;

            if (queue?.Count == 0) continue;

            var filtered = queue.Where(c => !ReferenceEquals(c, client)).ToList();
            if (filtered.Count == 0)
                _blockedKeys.Remove(key);
            else
                _blockedKeys[key] = new Queue<ClientState>(filtered);
        }

        client.IsBlocked = false;
        client.BlockedKeys?.Clear();
        client.BlockedCommand = null;
        client.BlockExpiryInMs = long.MaxValue;
    }

    private List<ClientState> DrainExpiredFromQueue(long nowMs, string key, Queue<ClientState> queue)
    {
        var expired = new List<ClientState>();
        while (queue.Count > 0)
        {
            var front = queue.Peek();
            if (front == null)
            {
                queue.Dequeue();
                continue;
            }

            if (front.BlockExpiryInMs <= nowMs)
            {
                var c = queue.Dequeue();
                c.IsBlocked = false;
                c.BlockedKeys?.Clear();
                c.BlockExpiryInMs = long.MaxValue;
                c.BlockedCommand = null;
                expired.Add(c);
                continue;
            }

            break;
        }

        return expired;
    }
    
}

public class ClientState
{
    public StringBuilder InputBuffer { get; set; } = new();
    public Queue<RedisCommand> PendingReplies { get; } = new();
    public bool IsBlocked { get; set; } = false;
    public List<string> BlockedKeys { get; set; }
    public long BlockExpiryInMs { get; set; } = long.MaxValue;
    public RedisCommand BlockedCommand { get; set; }
    public Queue<byte[]> PendingWrites { get; } = new();

    public override string ToString()
    {
        return $"Value for current state: {JsonSerializer.Serialize(this)}";
    }
}