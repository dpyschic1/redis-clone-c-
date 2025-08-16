using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 7979);
server.Start();
string response = "+PONG\r\n";
Byte[] bytes = Encoding.UTF8.GetBytes(response);
while (true)
{
    byte[] buff = [];
    var sock = server.AcceptSocket();
    while (true)
    {
        int bytesRead = sock.Receive(buff);
        if (bytesRead == 0)
            break;
        sock.Send(bytes);
    }
}