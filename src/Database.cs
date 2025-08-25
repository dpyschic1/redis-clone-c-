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
            return value.AsString();
        }

        return null;
    }

    public int ListLength(string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List)
        {
            return value.AsList().Count;
        }

        return 0;
    }

    public int ListRightPush(string key, List<string> values)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (values == null || values.Count == 0) throw new ArgumentNullException(nameof(values));

        if (_dataStore.TryGetValue(key, out var existingValue) && existingValue.Type == RedisDataType.List)
        {
            var valueList = existingValue.AsList();
            valueList.AddRange(values);
            return valueList.Count;
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
            var listValues =  existingValue.AsList();
            listValues.InsertRange(0, values);
            return listValues.Count;
        }

        _dataStore[key] = new RedisValue(RedisDataType.List, values);
        return values.Count;
    }

    public List<string> ListPop(string key, int num)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List && value.AsList().Count > 0)
        {
            var listValues = value.AsList();
            var elements = listValues[0..num];
            listValues.RemoveRange(0, num);
            return elements;
        }

        return null;
    }

    public List<string> ListRange(string key, int startIndex, int endIndex)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        if (_dataStore.TryGetValue(key, out var value) && value.Type == RedisDataType.List)
        {
            var listValues = value.AsList();
            if (endIndex < 0)
            {
                endIndex += listValues.Count;
                endIndex = endIndex > listValues.Count - 1 ? listValues.Count - 1 : endIndex;
            }

            if (startIndex < 0)
            {
                startIndex += listValues.Count;
                startIndex = startIndex < 0 ? 0 : startIndex;
            }

            if (startIndex > endIndex) return new List<string>();

            return listValues
                .Skip(startIndex)
                .Take(endIndex - startIndex + 1)
                .ToList();
        }

        return new List<string>();
    }

    public string AddStream(string key, string id, Dictionary<string, string> keyValuePairs)
    {
        if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(id)) throw new ArgumentNullException(nameof(key), nameof(id));

        RedisStream stream;
        if (_dataStore.TryGetValue(key, out var existingValue))
        {
            if (existingValue.Type != RedisDataType.List)
                throw new InvalidOperationException("ERR value is not of type Stream");
            
            stream = existingValue.AsStream();
        }
        else
        {
            stream = new RedisStream();
            _dataStore[key] = RedisValue.FromStream(stream);
        }

        StreamId newId;

        if (id == "*")
        {
            newId = stream.Add(keyValuePairs);
        }
        else
        {
            var parts = id.Split('-');
            if(parts.Length != 2)
                throw new RedisStreamException("ERR invalid stream id");

            if (!long.TryParse(parts[0], out var ms))
                throw new RedisStreamException("ERR invalid millisecond part in ID");

            long seq;
            if (parts[1] == "*") seq = 0;
            else if (!long.TryParse(parts[1], out seq))
                throw new RedisStreamException("Err invalid sequence part in ID");

            var streamId = new StreamId(ms, seq);
            newId = stream.Add(keyValuePairs, streamId, autoId: false);
        }

        return newId.ToString();
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
    public object Value { get; }
    public long ExpiryTime { get; }
    public bool IsExpired => ExpiryTime < DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public RedisValue(RedisDataType type, object value, TimeSpan? expiry = null)
    {
        Type = type;
        Value = value;
    }

    public static RedisValue FromString(string s) => new(RedisDataType.String, s);
    public static RedisValue FromList(List<string> l) => new(RedisDataType.List, l);
    public static RedisValue FromStream(RedisStream stream) => new(RedisDataType.Stream, stream);
    public string AsString() => (string)Value;
    public long AsInt() =>  (long)Value;
    public List<string> AsList() => (List<string>)Value;
    public RedisStream AsStream() => (RedisStream)Value;
}

public class RedisStream
{
    private readonly SortedDictionary<StreamId, StreamEntry> _entries = new();
    private StreamId _lastId = new(0, 0);

    public StreamId Add(Dictionary<string, string> fields, StreamId? id = null, bool autoId = true)
    {
        if(fields == null || fields.Count == 0)
            throw new ArgumentNullException(nameof(fields));

        StreamId newId;

        if (autoId || id == null)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (now > _lastId.ms)
                newId = new StreamId(now, 0);
            else
                newId = new StreamId(_lastId.ms, _lastId.seq + 1);
        }
        else
        {
            if (id.CompareTo(_lastId) <= 0)
                throw new InvalidOperationException(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item");
            newId = id;
        }
        
        _entries[newId] = new StreamEntry(newId, fields);
        _lastId = newId;
        
        return newId;
    }
}

public class StreamEntry
{
    public StreamId Id { get; }
    public Dictionary<string, string> Fields { get; }

    public StreamEntry(StreamId id, Dictionary<string, string> fields)
    {
        Id = id;
        Fields = fields;
    }
}

public record StreamId(long ms, long seq) : IComparable<StreamId>
{
    public int CompareTo(StreamId? other)
    {
        if (other == null) return 1;
        int cmp = ms.CompareTo(other.ms);
        return cmp != 0 ? cmp : seq.CompareTo(other.seq);
    }

    public override string ToString()
    {
        return $"{ms}-{seq}";
    }
}

public enum RedisDataType
{
    String,
    List,
    Stream,
    None
}