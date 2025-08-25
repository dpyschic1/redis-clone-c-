using System.Collections.Concurrent;

namespace Server;

public class Database
{
    private static readonly Database _instance = new Database();
    public static Database Instance => _instance;
    private readonly ConcurrentDictionary<string, RedisValue> _dataStore;

    private Database()
    {
        _dataStore = new ConcurrentDictionary<string, RedisValue>();
    }

    public string GetDataTypeString(string key)
    {
        var type = GetDataType(key);
        return type.ToString().ToLower();
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

    public int ListLength(string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List)
        {
            return value.ListValue.Count;
        }

        return 0;
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

        values.Reverse();


        if (_dataStore.TryGetValue(key, out var existingValue) && existingValue.Type == RedisDataType.List)
        {
            existingValue.ListValue.InsertRange(0, values);
            return existingValue.ListValue.Count;
        }

        _dataStore[key] = new RedisValue(RedisDataType.List, values);
        return values.Count;
    }

    public List<string> ListPop(string key, int num)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List && value.ListValue.Count > 0)
        {
            var elements = value.ListValue[0..num];
            value.ListValue.RemoveRange(0, num);
            return elements;
        }

        return null;
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

    public string AddStream(string key, string id, Dictionary<string, string> keyValuePairs)
    {
        if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(id)) throw new ArgumentNullException(nameof(key), nameof(id));

        var idParts = id.Split('-');
        var idMilliSeconds = int.Parse(idParts[0]);
        var idSequenceNumber = int.Parse(idParts[1]);

        if (idMilliSeconds == 0 && idSequenceNumber == 0)
            throw new RedisStreamException("ERR The ID specified in XADD must be greater than 0-0");

        if (_dataStore.TryGetValue(key, out var value))
        {
            if (value.Type != RedisDataType.Stream) throw new ArgumentException("ERR value is not of type Stream");



            foreach (var keyInStream in value.StreamValues.Keys)
            {
                var keyInStreamParts = keyInStream.Split('-');
                var keyInStreamMs = int.Parse(keyInStreamParts[0]);
                var keyInStreamSequence = int.Parse(keyInStreamParts[1]);

                if (idMilliSeconds < keyInStreamMs)
                    throw new RedisStreamException("ERR The ID specified in XADD is equal or smaller than the target stream top item");

                if (keyInStreamMs == idMilliSeconds && keyInStreamSequence > idSequenceNumber)
                    throw new RedisStreamException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }

            value.StreamValues.Add(id, keyValuePairs);
            return id;
        }

        var stream = new Dictionary<string, Dictionary<string, string>>()
        {
            {
                id, new(keyValuePairs)
            }
        };

        _dataStore[key] = new RedisValue(RedisDataType.Stream, stream);

        return id;
    }

    private RedisDataType GetDataType(string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        if (_dataStore.TryGetValue(key, out var value))
        {
            return value.Type;
        }

        return RedisDataType.None;
    }
}

public class RedisValue
{
    public RedisDataType Type { get; }
    public string StringValue { get; }
    public List<string> ListValue { get; }
    public Dictionary<string, Dictionary<string, string>> StreamValues { get; }
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
            case RedisDataType.Stream:
                StreamValues = value as Dictionary<string, Dictionary<string, string>>;
                break;
            default:
                throw new ArgumentException("Invalid Redis data type");
        }
    }
}

public enum RedisDataType
{
    String,
    List,
    Stream,
    None
}