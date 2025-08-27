using System.Text;

namespace Server;

public class RedisSerializer
{
    private static readonly byte[] CRLF = Encoding.UTF8.GetBytes("\r\n");

    public byte[] Serialize(RedisCommand node)
    {
        using var ms = new MemoryStream();
        WriteNode(ms, node);
        return ms.ToArray();
    }

    private void WriteNode(Stream s, RedisCommand node)
    {
        switch (node.Type)
        {
            case RedisType.BulkString:
                s.WriteByte((byte)'$');
                if (node.StringValue == null)
                {
                    WriteStringAndCrLf(s, "-1");
                }
                else
                {
                    var payload = Encoding.UTF8.GetBytes(node.StringValue);
                    WriteStringAndCrLf(s, payload.Length.ToString());
                    s.Write(payload, 0, payload.Length);
                    s.Write(CRLF, 0, CRLF.Length);
                }
                break;

            case RedisType.SimpleString:
                s.WriteByte((byte)'+');
                WriteStringAndCrLf(s, node.StringValue ?? "");
                break;

            case RedisType.Error:
                s.WriteByte((byte)'-');
                WriteStringAndCrLf(s, node.StringValue ?? "");
                break;

            case RedisType.Integer:
                s.WriteByte((byte)':');
                WriteStringAndCrLf(s, node.IntegerValue?.ToString() ?? "0");
                break;
            
            case RedisType.NullBulkString:
                s.WriteByte((byte)'$');
                WriteStringAndCrLf(s, "-1");
                break;
            
            case RedisType.Array:
                s.WriteByte((byte)'*');
                if (node.Items == null)
                {
                    WriteStringAndCrLf(s, "-1");
                    return;
                }
                else if(node.Items.Count == 0)
                {
                    WriteStringAndCrLf(s, "0");
                }
                else
                {
                    WriteStringAndCrLf(s, node.Items.Count.ToString());
                    foreach (var item in node.Items)
                        WriteNode(s, item);
                }
                break;
            case RedisType.Binary:
                s.WriteByte((byte)'$');
                if (node.BinaryValue == null)
                {
                    WriteStringAndCrLf(s, "");
                    return;
                }
                WriteStringAndCrLf(s, node.BinaryValue?.Length.ToString());
                s.Write(node.BinaryValue, 0, node.BinaryValue.Length);
                break;
            default:
                throw new NotSupportedException($"Unsupported Redis type: {node.Type}");
        }
    }

    private void WriteStringAndCrLf(Stream s, string str)
    {
        var bytes = Encoding.UTF8.GetBytes(str);
        s.Write(bytes, 0, bytes.Length);
        s.Write(CRLF, 0, CRLF.Length);
    }
}