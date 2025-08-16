using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Byte[] bytes = [];
String data = null;

while (true)
{
    Console.WriteLine("Waiting for connection... ");

    using var sock = server.AcceptSocket(); // wait for client
    Console.WriteLine("Socket accepted");
    string response = "+PONG\r\n";

    bytes = Encoding.UTF8.GetBytes(response);

    sock.Send(bytes);

    Console.WriteLine("Sent : {0}", response);
}