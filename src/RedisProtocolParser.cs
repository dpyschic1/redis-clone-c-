using System.Text;

namespace Server;

public class RedisProtocolParser
{
    private const byte ASTERISK = (byte)'*';
    private const byte PLUS = (byte)'+';
    private const byte DOLLAR = (byte)'$';
    private const byte MINUS = (byte)'-';
    private const byte INTEGER = (byte)':';

    public bool TryParse(string input, out RedisCommand output, out int consumed)
    {
        var inputBytes = Encoding.ASCII.GetBytes(input);
        using var memStream = new RedisMemoryStream(inputBytes);
        output = ParseNode(memStream);
        consumed = (int)memStream.Position;
        return output != null;
    }

    public RedisCommand Parse(byte[] input)
    {
        using var memStream = new RedisMemoryStream(input);
        return ParseNode(memStream);

    }

    private RedisCommand ParseNode(RedisMemoryStream memStream)
    {
        byte b = (byte)memStream.ReadByte();
        return b switch
        {
            DOLLAR => ParseBulkString(memStream),
            ASTERISK => ParseArray(memStream),
            PLUS => ParseSimpleString(memStream),
            MINUS => ParseError(memStream),
            INTEGER => ParseInteger(memStream),
            _ => throw new NotSupportedException($"Unsupported Redis type: {b}"),
        };
    }

    private RedisCommand ParseArray(RedisMemoryStream memStream)
    {
        int num = memStream.ReadIntCrLF();
        List<RedisCommand> readList = new(num);
        for (int i = 0; i < num; i++)
        {
            readList.Add(ParseNode(memStream));
        }
        return new RedisCommand
        {
            Type = RedisType.Array,
            Items = readList
        };
    }

    private RedisCommand ParseBulkString(RedisMemoryStream memStream)
    {
        int num = memStream.ReadIntCrLF();
        if (num == -1)
        {
            return new RedisCommand {Type = RedisType.NullBulkString, StringValue = null};
        }
        byte[] buffer = new byte[num];
        memStream.Read(buffer, 0, num);
        memStream.SkipCrLf();
        return new RedisCommand
        {
            Type = RedisType.BulkString,
            StringValue = Encoding.UTF8.GetString(buffer)
        };
    }

    private RedisCommand ParseSimpleString(RedisMemoryStream memStream)
    {
        string s = memStream.ReadLine();
        return new RedisCommand
        {
            Type = RedisType.SimpleString,
            StringValue = s
        };
    }

    private RedisCommand ParseError(RedisMemoryStream memStream)
    {
        string s = memStream.ReadLine();
        return new RedisCommand
        {
            Type = RedisType.Error,
            StringValue = s
        };
    }

    private RedisCommand ParseInteger(RedisMemoryStream memStream)
    {
        long v = memStream.ReadIntCrLF();
        return new RedisCommand
        {
            Type = RedisType.Integer,
            IntegerValue = v
        };
    }
}

public class RedisCommand
{
    public RedisType Type { get; set; }
    public string StringValue { get; set; }
    public long? IntegerValue { get; set; }
    public List<RedisCommand> Items { get; set; } = new();
    public bool IsArray => Type == RedisType.Array;
    public bool IsBulkString => Type == RedisType.BulkString || Type == RedisType.NullBulkString;
    public bool IsSimpleString => Type == RedisType.SimpleString;
    public bool IsError => Type == RedisType.Error;
    public bool IsInteger => Type == RedisType.Integer;

    public override string ToString()
    {
        if (IsBulkString || IsSimpleString || IsError)
            return StringValue;
        if (IsInteger)
            return IntegerValue?.ToString();
        return null;
    }
}

public enum RedisType
{
    Array,
    BulkString,
    SimpleString,
    Error,
    Integer,
    NullBulkString
}