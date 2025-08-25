using System.Reflection.Metadata;

namespace Server;

public class CommandExecutor
{
    private readonly ClientStateManager _clientManager = ClientStateManager.Instance;

    public RedisCommand Execute(RedisCommand node, ClientState clientState)
    {
        if (node == null) throw new ArgumentNullException(nameof(node));
        if (!node.IsArray) return MakeError("Protocol error: expected array");

        return ExecuteArrayCommand(node, clientState);
    }

    private RedisCommand ExecuteArrayCommand(RedisCommand arrayNode, ClientState clientState)
    {
        if (arrayNode.Items?.Count == 0)
        {
            return MakeError("Protocol error: empty array");
        }
        var command = arrayNode.Items[0];
        var cmdName = command.ToString();
        if (string.IsNullOrEmpty(cmdName))
        {
            return MakeError("Protocol error: command name is empty");
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
            default: return MakeError($"ERR unknown command '{cmdName}'");
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

    private RedisCommand HandleXAdd(List<string> args)
    {
        if (args.Count < 3) return MakeError("ERR wrong number of arguments for XAdd command");
        var key = args[0];
        var id = args[1];
        Dictionary<string, string> kvPair = new();
        for (int i = 2; i < args.Count; i += 2)
        {
            kvPair.Add(args[i], args[i + 1]);
        }

        var addedId = Database.Instance.AddStream(key, id, kvPair);

        return MakeBulkString(addedId);
    }

    private RedisCommand HandleType(List<string> args)
    {
        if (args.Count != 1) return MakeError("Error wrong number of arguments for Type command");

        var key = args[0];

        var value = Database.Instance.GetDataTypeString(key);

        return MakeSimpleString(value);

    }

    private RedisCommand HandleBLPop(List<string> args, ClientState clientState)
    {
        if (args.Count < 2) return MakeError("ERR wrong number of arguments for 'blpop' command");

        if (clientState == null) return MakeError("ERR internal: missing client state for blocking command");

        if (!double.TryParse(args.Last(), out var timeoutSec))
            return MakeError("ERR timeout must be a number");

        var keys = args.Take(args.Count - 1).ToList();

        foreach (var key in keys)
        {
            var val = Database.Instance.ListPop(key, 1);
            if (val != null && val.Count > 0)
            {
                return MakeArray(new List<string> { key, val[0] });
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
        if (args.Count > 2) return MakeError("ERR wrong number of arguments for 'lpop' commnad");
        var key = args[0];
        var num = args.Count > 1 ? int.Parse(args[1]) : 1;
        var value = Database.Instance.ListPop(key, num);
        return value switch
        {
            { Count: > 1 } => MakeArray(value),
            { Count: 1 } => MakeBulkString(value[0]),
            null => MakeNullBulkString(),
            _ => MakeNullBulkString()
        };
    }

    private RedisCommand HandleLlen(List<string> args)
    {
        if (args.Count != 1) return MakeError("ERR wrong number of arguments for 'llen' command");
        var key = args[0];
        var length = Database.Instance.ListLength(key);
        return MakeInteger(length);
    }

    private RedisCommand HandleLRange(List<string> args)
    {
        if (args.Count != 3) return MakeError("ERR wrong number of arguments for 'lrange' command");
        var key = args[0];
        var values = args.Skip(1).ToList();
        var list = Database.Instance.ListRange(key, int.Parse(values[0]), int.Parse(values[1]));
        return MakeArray(list);
    }

    private RedisCommand HandleLPush(List<string> args)
    {
        if (args.Count < 2) return MakeError("ERR wrong number of arguments for 'lpush' command");
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

            var reply = MakeArray(new List<string> { key, item });
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

        return MakeInteger(count);
    }

    private RedisCommand HandleRPush(List<string> args)
    {
        if (args.Count < 2) return MakeError("ERR wrong number of arguments for 'rpush' command");
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

            var reply = MakeArray(new List<string> { key, item });
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

        return MakeInteger(count);
    }

    private RedisCommand HandleSet(List<string> args)
    {
        if (args.Count < 2 || args.Count > 4) return MakeError("ERR wrong number of arguments for 'set' command");
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
        return MakeSimpleString("OK");
    }

    private RedisCommand HandleGet(List<string> args)
    {
        if (args.Count != 1) return MakeError("ERR wrong number of arguments for 'get' command");
        var key = args[0];
        var value = Database.Instance.Get(key);
        if (value == null) return MakeBulkString(null); // Null bulk string for non-existent key
        return MakeBulkString(value);
    }

    private RedisCommand HandleEcho(List<string> args)
    {
        if (args.Count == 0) return MakeError("ERR wrong number of arguments for 'echo' command");
        return MakeBulkString(string.Join(" ", args));
    }

    private RedisCommand HandlePing(List<string> args)
    {
        if (args.Count == 0) return MakeSimpleString("PONG");
        return MakeSimpleString(args[0]);
    }

    private RedisCommand MakeSimpleString(string s)
    {
        return new RedisCommand
        {
            Type = RedisType.SimpleString,
            StringValue = s
        };
    }

    private RedisCommand MakeError(string s)
    {
        return new RedisCommand
        {
            Type = RedisType.Error,
            StringValue = s
        };
    }

    private RedisCommand MakeBulkString(string s)
    {
        return new RedisCommand
        {
            Type = s == null ? RedisType.NullBulkString : RedisType.BulkString,
            StringValue = s
        };
    }

    private RedisCommand MakeNullBulkString()
    {
        return new RedisCommand
        {
            Type = RedisType.NullBulkString
        };
    }

    private RedisCommand MakeInteger(long value)
    {
        return new RedisCommand
        {
            Type = RedisType.Integer,
            IntegerValue = value
        };
    }

    private RedisCommand MakeArray(List<string> values)
    {
        return new RedisCommand
        {
            Type = RedisType.Array,
            Items = values.Select(v => new RedisCommand
            {
                Type = RedisType.BulkString,
                StringValue = v
            }).ToList()
        };
    }
}