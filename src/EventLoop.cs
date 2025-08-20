using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Server;

public class EventLoop
{
    private readonly Socket _listener;
    private readonly List<Socket> _clients = new();
    private readonly Dictionary<Socket, ClientState> _clientStates = new();
    private bool _running = true;

    private readonly RedisProtocolParser _redisParser;
    private readonly CommandExecutor _commandExecutor;
    private readonly RedisSerializer _serializer;

    public EventLoop(int port, RedisProtocolParser redisParser , CommandExecutor commandExecutor, RedisSerializer serializer)
    {
        _redisParser = redisParser;
        _commandExecutor = commandExecutor;
        _serializer = serializer;

        // Initialize the listener socket
        _listener = new Socket(AddressFamily.InterNetwork,
            SocketType.Stream, ProtocolType.Tcp);

        _listener.Bind(new IPEndPoint(IPAddress.Any, port));
        _listener.Listen(512);
        _listener.Blocking = false;
        Console.WriteLine("Listenting on port {0}", port);
    }

    public void Run()
    {
        while (_running)
        {
            var readList = new List<Socket>(_clients) { _listener };
            var writeList = new List<Socket>();

            foreach (var kv in _clientStates)
            {
                if (kv.Value.PendingWrites.Count > 0)
                {
                    writeList.Add(kv.Key);
                }
            }

            var errorList = new List<Socket>();

            Socket.Select(readList, writeList, errorList, 100_000);

            if (readList.Contains(_listener))
            {
                Socket client = _listener.Accept();
                client.Blocking = false;
                _clients.Add(client);
                _clientStates[client] = new ClientState();
                readList.Remove(_listener);
            }

            foreach (var client in readList)
            {
                HandleRead(client);
            }

            foreach (var client in writeList)
            {
                HandleWrite(client);
            }
            
            foreach (var client in errorList)
            {
                Console.WriteLine("Error on client {0}", client.RemoteEndPoint);
                CloseClient(client);
            }
        }
    }

    public void HandleRead(Socket client)
    {
        try
        {
            var state = _clientStates[client];
            var buffer = new byte[1024];
            int received = client.Receive(buffer);


            if (received == 0)
            {
                CloseClient(client);
                return;
            }

            state.InputBuffer.Append(Encoding.UTF8.GetString(buffer, 0, received));

            while (true)
            {
                var parseResult = _redisParser.TryParse(state.InputBuffer.ToString(), out var command, out var consumed);
                if (!parseResult)
                {
                    break;
                }
                state.InputBuffer.Remove(0, consumed);

                var result = _commandExecutor.Execute(command);

                var response = _serializer.Serialize(result);

                state.PendingWrites.Enqueue(response);
            }

        }
        catch(SocketException ex)
        {
            Console.WriteLine("Socket exception: {0}", ex.Message);
            CloseClient(client);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception: {0}", ex.Message);
            CloseClient(client);
        }
   }

    public void HandleWrite(Socket client)
    {
        var state = _clientStates[client];
        
        while(state.PendingWrites.Count > 0)
        {
            var data = state.PendingWrites.Dequeue();
            try
            {
                int sent = client.Send(data);
                if (sent < data.Length)
                {
                    var remaining = new byte[data.Length - sent];
                    Buffer.BlockCopy(data, sent, remaining, 0, remaining.Length);

                    state.PendingWrites.Enqueue(remaining);
                    break;
                }
            }
            catch (SocketException ex)
            {
                Console.WriteLine("Socket exception during send: {0}", ex.Message);
                return;
            }
        }
    }
    private void CloseClient(Socket client)
    {
        _clients.Remove(client);
        try
        {
            client.Shutdown(SocketShutdown.Both);
        }
        catch { }
        _clientStates.Remove(client);
        client.Close();
    }

    public void Stop() => _running = false;
}

public class ClientState
{
    public StringBuilder InputBuffer { get; set; } = new();
    public Queue<byte[]> PendingWrites { get; } = new();
}