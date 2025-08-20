namespace Server;

public class RedisMemoryStream : MemoryStream
{
    public RedisMemoryStream(byte[] buffer) : base(buffer)
    {
    }

    public int ReadIntCrLF()
    {
        int num = base.ReadByte();
        base.Position += 2;
        return num - '0';
    }

    public string ReadLine()
{
    var bytes = new List<byte>();
    while (true)
    {
        int b = ReadByte();
        if (b == -1)
            break;
        if (b == '\r')
        {
            int next = ReadByte();
            if (next == '\n')
                break;
            bytes.Add((byte)b);
            bytes.Add((byte)next);
        }
        else
        {
            bytes.Add((byte)b);
        }
    }
    return System.Text.Encoding.UTF8.GetString(bytes.ToArray());
}

    public void SkipCrLf()
    {
        base.Position += 2;
    }
}