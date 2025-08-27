using System.Text;
using System.Text.Json;

namespace Server;

public class ClientStateManager
{
    private static readonly ClientStateManager _instance = new ClientStateManager();
    public static ClientStateManager Instance => _instance;

    private readonly Dictionary<string, List<BlockedClient>> _blockedClients = new();
    
    private readonly Dictionary<ClientState, ClientTransactions> _clientTransactions = new();

    public void BlockClient(ClientState state, string[] keys, string commandType, long timeoutMs,
        Dictionary<string, object> parameters = null)
    {
        state.IsBlocked = true;
        var expiryMs = timeoutMs == 0
            ? long.MaxValue
            : DateTimeOffset.UtcNow.AddMilliseconds(timeoutMs).ToUnixTimeMilliseconds();

        var blockedClient = new BlockedClient()
        {
            Client = state,
            CommandType = commandType,
            BlockExpiryInMs = expiryMs,
            Parameters = parameters ?? []
        };

        foreach (var key in keys)
        {
            if (!_blockedClients.ContainsKey(key))
                _blockedClients.Add(key, []);

            _blockedClients[key].Add(blockedClient);
        }
    }

    public void NotifyKeyChanged(string key, string changeType = null)
    {
        if (!_blockedClients.ContainsKey(key))
            return;

        var clients = _blockedClients[key];
        var clientsToRemove = new List<BlockedClient>();

        foreach (var client in clients)
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (client.BlockExpiryInMs <= nowMs)
            {
                client.Client.PendingReplies.Enqueue(RedisResponse.NullString());
                clientsToRemove.Add(client);
                continue;
            }

            var response = TryGenerateResponse(client, key);
            if (response != null)
            {
                client.Client.PendingReplies.Enqueue(response);
                clientsToRemove.Add(client);
            }
        }

        foreach (var client in clientsToRemove)
        {
            RemoveClientFromAllKeys(client);
        }

        if (!clients.Any())
            _blockedClients.Remove(key);
    }

    public void StartTransactionForClient(ClientState state)
    {
        if (!state.IsBlocked)
        {
            var transaction = new ClientTransactions();
            state.IsInTransaction = true;
            _clientTransactions.Add(state, transaction);
        }
    }

    public void AddTransactionForClient(ClientState state, RedisCommand commands)
    {
        if(!state.IsInTransaction) return;

        if (_clientTransactions.TryGetValue(state, out var transaction))
        {
            transaction.Transactions.Enqueue(commands);
            state.PendingReplies.Enqueue(RedisResponse.SimpleString("QUEUED"));
        }
    }

    public bool TryGetTransactionForClient(ClientState state, out Queue<RedisCommand> commands)
    {
        commands = null;
        
        if (!state.IsInTransaction) return false;

        if (_clientTransactions.TryGetValue(state, out var transaction))
        {
            state.IsInTransaction = false;
            commands = transaction.Transactions;
            return true;
        }

        return false;
    }

    private RedisCommand TryGenerateResponse(BlockedClient blockedClient, string changedKey)
    {
        switch (blockedClient.CommandType)
        {
            case "XREAD":
                return TryUnblockXRead(blockedClient, changedKey);
            case "BLPOP":
                return TryUnblockBLPop(blockedClient, changedKey);
            default:
                return null;
        }
    }

    private RedisCommand TryUnblockXRead(BlockedClient blockedClient, string changedKey)
    {
        var streamAndIds = (Dictionary<string, string>)blockedClient.Parameters["StreamAndIds"];
        var count = (int)blockedClient.Parameters.GetValueOrDefault("Count", -1);

        var result = Database.Instance.RangeStreamMultiple(streamAndIds, count);
        bool hasData = result.Any(x => x.Value != null && x.Value.Count > 0);

        return hasData ? StreamResponse.XRead(result) : null;
    }

    private RedisCommand TryUnblockBLPop(BlockedClient blockedClient, string changedKey)
    {
        var keys = (List<string>)blockedClient.Parameters["Keys"];

        foreach (var key in keys)
        {
            var val = Database.Instance.ListPop(key, 1);
            if (val != null && val.Count > 0)
            {
                return RedisResponse.Array(RedisResponse.String(key), RedisResponse.String(val[0]));
            }
        }

        return null;
    }

    public void RemoveClientFromAllKeys(BlockedClient blockedClient)
    {
        foreach (var kvp in _blockedClients.ToList())
        {
            var key = kvp.Key;
            var clients = kvp.Value;

            clients.RemoveAll(c => ReferenceEquals(c.Client, blockedClient.Client));

            if (!clients.Any())
                _blockedClients.Remove(key);
        }

        blockedClient.Client.IsBlocked = false;
    }

    public List<ClientState> GetAndRemoveExpiredClients()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var expiredClients = new List<ClientState>();

        foreach (var kvp in _blockedClients.ToList())
        {
            var clients = kvp.Value;
            var expired = clients.Where(c => c.BlockExpiryInMs <= nowMs).ToList();

            foreach (var expiredClient in expired)
            {
                expiredClients.Add(expiredClient.Client);
            }

            clients.RemoveAll(c => c.BlockExpiryInMs <= nowMs);

            if (!clients.Any())
                _blockedClients.Remove(kvp.Key);
        }

        foreach (var client in expiredClients)
        {
            client.IsBlocked = false;
            client.PendingReplies.Enqueue(RedisResponse.NullString());
        }

        return expiredClients;
    }
}

public class ClientState
{
    public StringBuilder InputBuffer { get; set; } = new();
    public Queue<RedisCommand> PendingReplies { get; } = new();
    public bool IsBlocked { get; set; } = false;
    public bool IsInTransaction { get; set; } = false;
    public Queue<byte[]> PendingWrites { get; } = new();

    public override string ToString()
    {
        return $"Value for current state: {JsonSerializer.Serialize(this)}";
    }
}

public class BlockedClient
{
    public ClientState Client { get; set; }
    public string CommandType { get; set; }
    public long BlockExpiryInMs { get; set; }
    public Dictionary<string, object> Parameters { get; set; } = new();
}

public class ClientTransactions
{
    public Queue<RedisCommand> Transactions { get; set; } = new();
}