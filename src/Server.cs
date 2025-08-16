using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6381);
server.Start();
var sock = server.AcceptSocket();
while (true)
{
    Console.WriteLine("Waiting for connection... ");
    Console.WriteLine("Socket accepted");
    string response = "+PONG\r\n";
    var bytes = Encoding.UTF8.GetBytes(response);
    sock.Send(bytes);
    Console.WriteLine("Sent : {0}", response);
}