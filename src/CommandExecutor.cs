using System.Reflection.Metadata;

namespace Server;

public class CommandExecutor
{
    public RedisCommand Execute(RedisCommand node)
    {
        if (node == null) throw new ArgumentNullException(nameof(node));
        if (!node.IsArray) return MakeError("Protocol error: expected array");

        return ExecuteArrayCommand(node);
    }

    private RedisCommand ExecuteArrayCommand(RedisCommand arrayNode)
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
            case "LRANGE": return HandleLRange(args);
            default: return MakeError($"ERR unknown command '{cmdName}'");
        }
    }

    private string Eval(RedisCommand argNode)
    {
        if (argNode == null) return null;
        if (argNode.IsArray)
        {
            var nestedReply = ExecuteArrayCommand(argNode);
            return nestedReply.ToString();
        }
        return argNode.ToString();
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
        var count = Database.Instance.ListLeftPush(key, values);
        return MakeInteger(count);
    }

    private RedisCommand HandleRPush(List<string> args)
    {
        if (args.Count < 2) return MakeError("ERR wrong number of arguments for 'rpush' command");
        var key = args[0];
        var values = args.Skip(1).ToList();
        var count = Database.Instance.ListRightPush(key, values);
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