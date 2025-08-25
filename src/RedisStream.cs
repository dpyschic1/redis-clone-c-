namespace Server;

public class RedisStream
{
    private readonly SortedDictionary<StreamId, StreamEntry> _entries = new();
    private StreamId _lastId = new(0, 0);

    public StreamId Add(Dictionary<string, string> fields, StreamId? id = null, bool isSequenceWildCard = false)
    {
        if(fields == null || fields.Count == 0)
            throw new ArgumentNullException(nameof(fields));

        StreamId newId = GenerateId(id, isSequenceWildCard);
        
        _entries[newId] = new StreamEntry(newId, fields);
        _lastId = newId;
        
        return newId;
    }

    private StreamId GenerateId(StreamId? id, bool isSequenceWildCard)
    {
        if (id == null)
            return GenerateAutoId();

        if (isSequenceWildCard)
            return GenerateSequenceWildcardId(id.ms);

        return ValidateExplicitId(id);
    }

    private StreamId GenerateAutoId()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return now >= _lastId.ms
            ? new StreamId(now, 0)
            : new StreamId(_lastId.ms, _lastId.seq + 1);
    }

    private StreamId GenerateSequenceWildcardId(long ms)
    {
        return ms < _lastId.ms 
            ? new StreamId(_lastId.ms, _lastId.seq + 1) 
            : new StreamId(ms, 0);
    }

    private StreamId ValidateExplicitId(StreamId id)
    {
        if (id.ms == 0 && id.seq == 0)
            throw new InvalidOperationException("ERR The ID specified in XADD must be greater than 0-0");

        if (id.CompareTo(_lastId) <= 0)
            throw new InvalidOperationException(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        return id;
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