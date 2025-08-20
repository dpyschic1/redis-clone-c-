using System.Text;

namespace Server;

public class RedisMemoryStream : MemoryStream
{
    public RedisMemoryStream(byte[] buffer) : base(buffer)
    {
    }

    public int ReadIntCrLF()
    {
        var bytes = new List<byte>();

        while (true)
        {
            int b = ReadByte();
            if (b == -1)
                throw new EndOfStreamException("Unexpected end of stream while reading integer.");
            if(b == '\r')
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

        var num = Encoding.ASCII.GetString(bytes.ToArray());

        return int.Parse(num);
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
    return Encoding.ASCII.GetString(bytes.ToArray());
}

    public void SkipCrLf()
    {
        base.Position += 2;
    }
}