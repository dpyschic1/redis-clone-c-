using System.Reflection.Metadata;

namespace Server;

public class CommandExecutor
{
    private readonly ClientStateManager _clientManager = ClientStateManager.Instance;

    public RedisCommand Execute(RedisCommand node, ClientState clientState)
    {
        if (node == null) throw new ArgumentNullException(nameof(node));
        if (!node.IsArray) return RedisResponse.Error("Protocol error: expected array");

        return ExecuteArrayCommand(node, clientState);
    }

    private RedisCommand ExecuteArrayCommand(RedisCommand arrayNode, ClientState clientState)
    {
        if (arrayNode.Items?.Count == 0)
        {
            return RedisResponse.Error("Protocol error: empty array");
        }
        var command = arrayNode.Items[0];
        var cmdName = command.ToString();
        if (string.IsNullOrEmpty(cmdName))
        {
            return RedisResponse.Error("Protocol error: command name is empty");
        }

        var args = new List<string>();
        foreach (var argNode in arrayNode.Items.Skip(1))
        {
            args.Add(Eval(argNode));
        }

        switch (cmdName.ToUpperInvariant())
        {
            case "ECHO": return HandleEcho(args);
            case "PING": return HandlePing(args);
            case "SET": return HandleSet(args);
            case "GET": return HandleGet(args);
            case "LLEN": return HandleLlen(args);
            case "RPUSH": return HandleRPush(args);
            case "LPUSH": return HandleLPush(args);
            case "LPOP": return HandleLPop(args);
            case "LRANGE": return HandleLRange(args);
            case "BLPOP": return HandleBLPop(args, clientState);
            case "TYPE": return HandleType(args);
            case "XADD": return HandleXAdd(args);
            case "XRANGE": return HandleXRange(args);
            case "XREAD": return HandleXRead(args, clientState);
            default: return RedisResponse.Error($"ERR unknown command '{cmdName}'");
        }
    }

    private string Eval(RedisCommand argNode)
    {
        if (argNode == null) return null;
        if (argNode.IsArray)
        {
            var nestedReply = ExecuteArrayCommand(argNode, null);
            return nestedReply.ToString();
        }
        return argNode.ToString();
    }

    private RedisCommand HandleXRead(List<string> args, ClientState clientState)
    {
        if (args.Count < 2) return RedisResponse.Error("ERR wrong number of arguments for XRange command");
        if (clientState == null) return RedisResponse.Error("ERR internal: missing client state for blocking command");

        int i = 0;
        int count = 0;
        long blockMs = 0;
        var streamAndIds = new Dictionary<string, string>();
        if (i < args.Count && args[i].ToUpper() == "COUNT")
        {
            i++;
            if (i < args.Count && int.TryParse(args[i], out int countVal))
            {
                count = countVal;
                i++;
            }
        }

        if (i < args.Count && args[i].ToUpper() == "BLOCK")
        {
            i++;
            if (i < args.Count && long.TryParse(args[i], out long blockMsVal))
            {
                blockMs = blockMsVal;
                i++;
            }
        }

        int streamIndex = -1;

        for (int j = i; j < args.Count; j++)
        {
            if (args[j].ToUpper() == "STREAMS")
            {
                streamIndex = j;
                break;
            }
        }

        int remainingArgs = args.Count - streamIndex - 1;
        int streamCount = remainingArgs / 2;

        for (int k = 0; k < streamCount; k++)
        {
            var key = args[streamIndex + 1 + k];
            var id = args[streamIndex + 1 + streamCount + k];
            streamAndIds.Add(key, id);
        }

        var result = Database.Instance.RangeStreamMultiple(streamAndIds);

        if (result.Where(x => x.Value != null).Count() > 0)
        {
            return StreamResponse.XRead(result);
        }

        var now = DateTimeOffset.UtcNow.AddMilliseconds(blockMs).ToUnixTimeMilliseconds();
        var deadline = blockMs == 0 ? long.MaxValue : now;

        foreach (var key in streamAndIds.Keys)
        {
            _clientManager.RegisterBlocked(clientState, key, deadline, null);
        }

        return null;
    }

    private RedisCommand HandleXRange(List<string> args)
    {
        if (args.Count < 3) return RedisResponse.Error("ERR wrong number of arguments for XRange command");

        var key = args[0];
        var startId = args[1];
        var endId = args[2];

        try
        {
            var rangedResult = Database.Instance.RangeStream(key, startId, endId);
            return StreamResponse.XRange(rangedResult ?? []);
        }
        catch (Exception ex)
        {
            return RedisResponse.Error(ex.Message);
        }
    }

    private RedisCommand HandleXAdd(List<string> args)
    {
        if (args.Count < 3) return RedisResponse.Error("ERR wrong number of arguments for XAdd command");
        var key = args[0];
        var id = args[1];
        Dictionary<string, string> kvPair = new();
        for (int i = 2; i < args.Count; i += 2)
        {
            kvPair.Add(args[i], args[i + 1]);
        }

        try
        {
            var addedId = Database.Instance.AddStream(key, id, kvPair);
            var client = _clientManager.TryUnblockOneForKey(key);
            if (client != null)
            {
                var reply = StreamResponse.XReadSingle(key, addedId, kvPair);
                client.PendingReplies.Enqueue(reply);
                _clientManager.RemoveBlockedClientFromAllKeys(client);
            }
            return RedisResponse.String(addedId);
        }
        catch (Exception ex) when (ex is RedisStreamException || ex is InvalidOperationException)
        {
            return RedisResponse.Error(ex.Message);
        }
    }

    private RedisCommand HandleType(List<string> args)
    {
        if (args.Count != 1) return RedisResponse.Error("Error wrong number of arguments for Type command");

        var key = args[0];

        var value = Database.Instance.GetDataTypeString(key);

        return RedisResponse.SimpleString(value);

    }

    private RedisCommand HandleBLPop(List<string> args, ClientState clientState)
    {
        if (args.Count < 2) return RedisResponse.Error("ERR wrong number of arguments for 'blpop' command");

        if (clientState == null) return RedisResponse.Error("ERR internal: missing client state for blocking command");

        if (!double.TryParse(args.Last(), out var timeoutSec))
            return RedisResponse.Error("ERR timeout must be a number");

        var keys = args.Take(args.Count - 1).ToList();

        foreach (var key in keys)
        {
            var val = Database.Instance.ListPop(key, 1);
            if (val != null && val.Count > 0)
            {
                return RedisResponse.Array(RedisResponse.String(key), RedisResponse.String(val[0]));
            }
        }
        var now = DateTimeOffset.UtcNow.AddSeconds(timeoutSec).ToUnixTimeMilliseconds();
        var deadline = timeoutSec == 0 ? long.MaxValue : now;
        foreach (var key in keys)
        {
            _clientManager.RegisterBlocked(clientState, key, deadline, null);
        }

        return null;
    }

    private RedisCommand HandleLPop(List<string> args)
    {
        if (args.Count > 2) return RedisResponse.Error("ERR wrong number of arguments for 'lpop' commnad");
        var key = args[0];
        var num = args.Count > 1 ? int.Parse(args[1]) : 1;
        var value = Database.Instance.ListPop(key, num);
        return value switch
        {
            { Count: > 1 } => RedisResponse.Array(value),
            { Count: 1 } => RedisResponse.String(value[0]),
            null => RedisResponse.NullString(),
            _ => RedisResponse.NullString()
        };
    }

    private RedisCommand HandleLlen(List<string> args)
    {
        if (args.Count != 1) return RedisResponse.Error("ERR wrong number of arguments for 'llen' command");
        var key = args[0];
        var length = Database.Instance.ListLength(key);
        return RedisResponse.Integer(length);
    }

    private RedisCommand HandleLRange(List<string> args)
    {
        if (args.Count != 3) return RedisResponse.Error("ERR wrong number of arguments for 'lrange' command");
        var key = args[0];
        var values = args.Skip(1).ToList();
        var list = Database.Instance.ListRange(key, int.Parse(values[0]), int.Parse(values[1]));
        return RedisResponse.Array(list);
    }

    private RedisCommand HandleLPush(List<string> args)
    {
        if (args.Count < 2) return RedisResponse.Error("ERR wrong number of arguments for 'lpush' command");
        var key = args[0];
        var values = args.Skip(1).ToList();
        int count = 0;

        while (values.Count > 0)
        {
            var client = _clientManager.TryUnblockOneForKey(key);
            if (client == null) break;
            var item = values[0];
            values.RemoveAt(0);
            count++;

            var reply = RedisResponse.Array(RedisResponse.String(key), RedisResponse.String(item));
            client.PendingReplies.Enqueue(reply);

            _clientManager.RemoveBlockedClientFromAllKeys(client);
        }

        if (values.Count > 0)
        {
            count += Database.Instance.ListLeftPush(key, values);
        }
        else
        {
            count += Database.Instance.ListLength(key);
        }

        return RedisResponse.Integer(count);
    }

    private RedisCommand HandleRPush(List<string> args)
    {
        if (args.Count < 2) return RedisResponse.Error("ERR wrong number of arguments for 'rpush' command");
        var key = args[0];
        var values = args.Skip(1).ToList();
        int count = 0;

        while (values.Count > 0)
        {
            var client = _clientManager.TryUnblockOneForKey(key);
            if (client == null) break;
            var item = values[0];
            values.RemoveAt(0);
            count++;

            var reply = RedisResponse.Array(RedisResponse.String(key), RedisResponse.String(item));
            client.PendingReplies.Enqueue(reply);

            _clientManager.RemoveBlockedClientFromAllKeys(client);
        }


        if (values.Count > 0)
        {
            count += Database.Instance.ListRightPush(key, values);
        }
        else
        {
            count += Database.Instance.ListLength(key);
        }

        return RedisResponse.Integer(count);
    }

    private RedisCommand HandleSet(List<string> args)
    {
        if (args.Count < 2 || args.Count > 4) return RedisResponse.Error("ERR wrong number of arguments for 'set' command");
        var key = args[0];
        var value = args[1];
        if (args.Count == 4 && args[2].ToLowerInvariant() == "px" && long.TryParse(args[3], out var expiry))
        {
            Database.Instance.Set(key, value, TimeSpan.FromMilliseconds(expiry));
        }
        else
        {
            Database.Instance.Set(key, value);
        }
        return RedisResponse.SimpleString("OK");
    }

    private RedisCommand HandleGet(List<string> args)
    {
        if (args.Count != 1) return RedisResponse.Error("ERR wrong number of arguments for 'get' command");
        var key = args[0];
        var value = Database.Instance.Get(key);
        if (value == null) return RedisResponse.NullString(); // Null bulk string for non-existent key
        return RedisResponse.String(value);
    }

    private RedisCommand HandleEcho(List<string> args)
    {
        if (args.Count == 0) return RedisResponse.Error("ERR wrong number of arguments for 'echo' command");
        return RedisResponse.String(string.Join(" ", args));
    }

    private RedisCommand HandlePing(List<string> args)
    {
        if (args.Count == 0) return RedisResponse.SimpleString("PONG");
        return RedisResponse.SimpleString(args[0]);
    }
}