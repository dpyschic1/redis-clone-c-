namespace Server;

public static class RedisResponse
{
    public static RedisCommand Ok() => new() { Type = RedisType.SimpleString, StringValue = "OK" };
    public static RedisCommand Error(string message) => new() { Type = RedisType.Error, StringValue = message };
    public static RedisCommand String(string value) => new() { Type = RedisType.BulkString, StringValue = value };
    public static RedisCommand SimpleString(string value) => new() { Type = RedisType.SimpleString, StringValue = value };
    public static RedisCommand NullString() => new() { Type = RedisType.NullBulkString };
    public static RedisCommand Integer(long value) => new() { Type = RedisType.Integer, IntegerValue = value };
    public static RedisCommand EmptyArray() => new() { Type = RedisType.Array , Items = new List<RedisCommand>() };
    
    public static RedisCommand Array(params RedisCommand[] items) => new()
    {
        Type = RedisType.Array,
        Items = items?.ToList() ?? []
    };

    public static RedisCommand Array(IEnumerable<string> strings) => new()
    {
        Type = RedisType.Array,
        Items = strings?.Select(String).ToList() ?? []
    };

    public static RedisCommand Array<T>(IEnumerable<T> items, Func<T, RedisCommand> converter) => new()
    {
        Type = RedisType.Array,
        Items = items?.Select(converter).ToList() ?? new List<RedisCommand>()
    };
}

public static class StreamResponse
{
    public static RedisCommand XRange(Dictionary<string, Dictionary<string, string>> entries)
    {
        if (entries == null || !entries.Any())
            return RedisResponse.Array();

        var items = entries.Select(entry =>
            RedisResponse.Array(
                RedisResponse.String(entry.Key),
                RedisResponse.Array(entry.Value.SelectMany(field =>
                new[] { RedisResponse.String(field.Key), RedisResponse.String(field.Value) }).ToArray())
            )
        );

        return RedisResponse.Array(items.ToArray());
    }

    public static RedisCommand XRead(Dictionary<string, Dictionary<string, Dictionary<string, string>>> streamsData)
    {
        if (streamsData == null || !streamsData.Any())
            return RedisResponse.Array();

        var streamItems = streamsData
            .Where(stream => stream.Value != null && stream.Value.Any())
            .Select(stream =>
                RedisResponse.Array(
                    RedisResponse.String(stream.Key),
                    XRange(stream.Value)
                )
            );

        return RedisResponse.Array(streamItems.ToArray());
    }

    public static RedisCommand XReadSingle(string key, string entryId, Dictionary<string, string> fields)
    {
        var streamEntries = new Dictionary<string, Dictionary<string, string>>
        {
            [entryId] = fields
        };

        var streamsData = new Dictionary<string, Dictionary<string, Dictionary<string, string>>>
        {
            [key] = streamEntries
        };

        return XRead(streamsData);
    }
}