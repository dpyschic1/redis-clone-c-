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
            case "SET" : return HandleSet(args);
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

    private RedisCommand HandleSet(List<string> args)
    {
        if (args.Count != 2) return MakeError("ERR wrong number of arguments for 'set' command");
        var key = args[0];
        var value = args[1];
        Database.Instance.Set(key, value);
        return MakeSimpleString("OK");
    }

    private RedisCommand HandleGet(List<string> args)
    {
        if (args.Count != 1) return MakeError("ERR wrong number of arguments for 'get' command");
        var key = args[0];
        var value = Database.Instance.Get(key);
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
}