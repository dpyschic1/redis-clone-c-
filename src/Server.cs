using System.Net;
using System.Net.Sockets;
using Server;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
List<Socket> sockets = [];

while (true)
{
    if (server.Pending())
    {
        var incomingSocket = server.AcceptSocket();
        incomingSocket.Blocking = false;
        sockets.Add(incomingSocket);
    }


    foreach (var sock in sockets)
    {
        byte[] buff = new byte[1024];
        if (sock.Available > 0)
        {
            int bytesRead = sock.Receive(buff);

            if (bytesRead == 0)
            { 
                sock.Disconnect(false);
                sockets.Remove(sock);
                continue;   
            }

            var redisParser = new RedisProtocolParser();
            var commandExecutor = new CommandExecutor();
            var redisSerializer = new RedisSerializer();
            var parsedCommands = redisParser.Parse(buff);
            var responseCommand = commandExecutor.Execute(parsedCommands);
            var responseByte = redisSerializer.Serialize(responseCommand);
            sock.Send(responseByte);
        }
    }
}