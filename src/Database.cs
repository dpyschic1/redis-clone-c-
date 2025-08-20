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

    public bool Set(string key, string value, TimeSpan? expiry = null)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (value == null) throw new ArgumentNullException(nameof(value));

        _dataStore[key] = new RedisValue(RedisDataType.String, value, expiry);
        return true;
    }

    public string Get(string key)
    {
        if (_dataStore.TryGetValue(key, out var value))
        {
            if (value.IsExpired)
            {
                _dataStore.TryRemove(key, out _);
                return null;
            }
            return value.StringValue;
        }

        return null;
    }

    public int ListRightPush(string key, List<string> values)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (values == null || values.Count == 0) throw new ArgumentNullException(nameof(values));

        if (_dataStore.TryGetValue(key, out var existingValue) && existingValue.Type == RedisDataType.List)
        {
            existingValue.ListValue.AddRange(values);
            return existingValue.ListValue.Count;
        }

        _dataStore[key] = new RedisValue(RedisDataType.List, values);
        return values.Count;
    }
    
    public int ListLeftPush(string key, List<string> values)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (values == null || values.Count == 0) throw new ArgumentNullException(nameof(values));

        if (_dataStore.TryGetValue(key, out var existingValue) && existingValue.Type == RedisDataType.List)
        {
            existingValue.ListValue.InsertRange(0, values);
            return existingValue.ListValue.Count;
        }

        _dataStore[key] = new RedisValue(RedisDataType.List, values);
        return values.Count;
    }

    public List<string> ListRange(string key, int startIndex, int endIndex)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List)
        {
            if (endIndex < 0)
            {
                endIndex += value.ListValue.Count;
                endIndex = endIndex > value.ListValue.Count - 1 ? value.ListValue.Count - 1 : endIndex;
            }

            if (startIndex < 0)
            {
                startIndex += value.ListValue.Count;
                startIndex = startIndex < 0 ? 0 : startIndex;
            }

            if (startIndex > endIndex) return new List<string>();

            return value.ListValue
                .Skip(startIndex)
                .Take(endIndex - startIndex + 1)
                .ToList();
        }

        return new List<string>();
    }
}

public class RedisValue
{
    public RedisDataType Type { get; }
    public string StringValue { get; }
    public List<string> ListValue { get; }
    public long ExpiryTime { get; }
    public bool IsExpired => ExpiryTime < DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public RedisValue(RedisDataType type, object value, TimeSpan? expiry = null)
    {
        Type = type;
        switch (type)
        {
            case RedisDataType.String:
                StringValue = value as string;
                ExpiryTime = expiry.HasValue ? DateTimeOffset.UtcNow.Add(expiry.Value).ToUnixTimeMilliseconds() : long.MaxValue;
                break;
            case RedisDataType.List:
                ListValue = value as List<string>;
                ExpiryTime = expiry.HasValue ? DateTimeOffset.UtcNow.Add(expiry.Value).ToUnixTimeMilliseconds() : long.MaxValue;
                break;
            default:
                throw new ArgumentException("Invalid Redis data type");
        }
    }
}

public enum RedisDataType
{
    String,
    List
}