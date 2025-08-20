using System.Collections.Concurrent;

namespace Server;

public class Database
{
    private static readonly Lazy<Database> _instance = new(new Database());
    public static Database Instance => _instance.Value;
    private readonly ConcurrentDictionary<string, RedisValue> _dataStore;

    private Database()
    {
        _dataStore = new ConcurrentDictionary<string, RedisValue>();
    }

    public bool Set(string key, string value)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (value == null) throw new ArgumentNullException(nameof(value));

        _dataStore[key] = new RedisValue(RedisDataType.String, value);
        return true;
    }

    public string Get(string key)
    {
        if (_dataStore.TryGetValue(key, out var value))
        {
            return value.StringValue;
        }

        return null;
    }
}

public class RedisValue
{
    public RedisDataType Type { get; }
    public string StringValue { get; }

    public RedisValue(RedisDataType type, object value)
    {
        Type = type;
        switch (type)
        {
            case RedisDataType.String:
                StringValue = value as string;
                break;
            default:
                throw new ArgumentException("Invalid Redis data type");
        }
    }
}

public enum RedisDataType
{
    String
}