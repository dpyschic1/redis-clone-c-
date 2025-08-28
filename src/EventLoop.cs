using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Server;

public class EventLoop
{
    private readonly Socket _listener;
    private readonly List<Socket> _clients = new();
    private readonly Dictionary<Socket, ClientState> _clientStates = new();
    private readonly ClientStateManager _clientManager = ClientStateManager.Instance;
    private Socket? _master;
    private bool _running = true;

    private readonly RedisProtocolParser _redisParser;
    private readonly CommandExecutor _commandExecutor;
    private readonly RedisSerializer _serializer;

    public EventLoop(int port, RedisProtocolParser redisParser, CommandExecutor commandExecutor, RedisSerializer serializer)
    {
        _redisParser = redisParser;
        _commandExecutor = commandExecutor;
        _serializer = serializer;

        _listener = new Socket(AddressFamily.InterNetwork,
            SocketType.Stream, ProtocolType.Tcp);

        _listener.Bind(new IPEndPoint(IPAddress.Any, port));
        _listener.Listen(512);
        _listener.Blocking = false;
        Console.WriteLine("Listenting on port {0}", port);
    }

    public void Run()
    {
        if (!ServerInfo.IsMaster)
        {
            HandleHandshake();
        }
        while (_running)
        {
            var readList = new List<Socket>(_clients) { _listener };
            var writeList = new List<Socket>();

            if (!ServerInfo.IsMaster && _master != null)
            {
                readList.Add(_master);
            }

            foreach (var kv in _clientStates)
            {
                if (kv.Value.PendingReplies.Count > 0 || kv.Value.PendingWrites.Count > 0)
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

            if (_master != null && readList.Contains(_master))
            {
                if (!_clientStates.ContainsKey(_master))
                {
                    _clientStates[_master] = new ClientState();
                }
            }

            foreach (var client in readList)
            {
                if (_clientStates.TryGetValue(client, out var state) && state.IsBlocked)
                    continue;

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

            _clientManager.GetAndRemoveExpiredClients();
        }
    }

    private void HandleRead(Socket client)
    {
        try
        {
            var state = _clientStates[client];
            var buffer = new byte[1024];
            int received = client.Receive(buffer);


            if (received == 0)
            {
                if (client == _master)
                {
                    Console.WriteLine("Master connection closed.");
                    return;
                }
                CloseClient(client);
                return;
            }
            var actualData = new byte[received];
            Array.Copy(buffer, 0, actualData, 0, received);
            state.InputBuffer.Append(Encoding.UTF8.GetString(actualData, 0, received));

            while (true)
            {
                var parseResult = _redisParser.TryParse(state.InputBuffer.ToString(), out var command, out var consumed);
                if (!parseResult)
                {
                    break;
                }

                state.InputBuffer.Remove(0, consumed);

                if (_clientManager.IsTransactionContinue(state, command))
                {
                    _clientManager.AddTransactionForClient(state, command);
                    continue;
                }
                
                
                var result = _commandExecutor.Execute(command, state);

                if (result == null) continue;
                if(client ==  _master) continue;
                
                state.PendingReplies.Enqueue(result);
                
                if(command.IsWrite && !command.IsHandShake && ServerInfo.IsMaster)
                    ReplicationManager.Instance.DispatchToSlaves(actualData);
                    
            }

        }
        catch (SocketException ex)
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

    private void HandleWrite(Socket client)
    {
        if (!_clientStates.TryGetValue(client, out var state)) return;

        while (state.PendingReplies.Count > 0)
        {
            var command = state.PendingReplies.Dequeue();
            var bytes = _serializer.Serialize(command);
            state.PendingWrites.Enqueue(bytes);
        }

        while (state.PendingWrites.Count > 0)
        {
            var data = state.PendingWrites.Peek();
            try
            {
                int sent = client.Send(data);
                if (sent < data.Length)
                {
                    var remaining = new byte[data.Length - sent];
                    Buffer.BlockCopy(data, sent, remaining, 0, remaining.Length);
                    state.PendingWrites.Dequeue();
                    state.PendingWrites.Enqueue(remaining);
                    break;
                }
                state.PendingWrites.Dequeue();
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
        if (_clientStates.TryGetValue(client, out var state))
        {
            if (state.IsBlocked)
            {
                var blockedClient = new BlockedClient() { Client = state };
                _clientManager.RemoveClientFromAllKeys(blockedClient);
            }
        }

        _clients.Remove(client);
        try
        {
            client.Shutdown(SocketShutdown.Both);
        }
        catch { }
        _clientStates.Remove(client);
        client.Close();
    }

    private void HandleHandshake()
    {
        var buffer = new byte[1024];
        var host = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        var hostIp = ServerInfo.MasterAddress == "localhost" ? IPAddress.Loopback : Dns.GetHostAddresses(ServerInfo.MasterAddress)[0];
        var ipEndpoint = new IPEndPoint(hostIp, ServerInfo.MasterPort.Value);
        host.Connect(ipEndpoint);

        host.Send(_serializer.Serialize(HandShakeResponse.Ping()));
        host.Receive(buffer);
        var parsedHost = _redisParser.Parse(buffer);
        if (parsedHost.StringValue != "PONG")
        {
            host.Close();
            _running = false;
            return;
        }

        host.Send(_serializer.Serialize(HandShakeResponse.ReplConfPort()));
        host.Receive(buffer);
        if (_redisParser.Parse(buffer).StringValue != "OK")
        {
            host.Close();
            _running = false;
            return;
        }

        host.Send(_serializer.Serialize(HandShakeResponse.ReplConfCapa()));
        host.Receive(buffer);
        if (_redisParser.Parse(buffer).StringValue != "OK")
        {
            host.Close();
            _running = false;
            return;
        }

        host.Send(_serializer.Serialize(HandShakeResponse.PSync()));
        host.Receive(buffer);

        if (_redisParser.Parse(buffer).StringValue.StartsWith("FULLRESYNC"))
        {
            string bulkHeader = ReadLine(host);
            if (bulkHeader.StartsWith("$"))
            {
                int length = int.Parse(bulkHeader.Substring(1));
                DiscardExact(host, length);
            }
        }

        _master = host;
    }
    private string ReadLine(Socket socket)
    {
        var lineBuffer = new List<byte>();
        var single = new byte[1];
        while (true)
        {
            int read = socket.Receive(single, 0, 1, SocketFlags.None);
            if (read == 0) throw new Exception("Socket closed while reading line");
            lineBuffer.Add(single[0]);
            if (lineBuffer.Count >= 2 &&
                lineBuffer[^2] == (byte)'\r' &&
                lineBuffer[^1] == (byte)'\n')
            {
                break;
            }
        }
        return Encoding.UTF8.GetString(lineBuffer.ToArray()).TrimEnd('\r', '\n');
    }

    private void DiscardExact(Socket socket, int length)
    {
        var buffer = new byte[4096];
        int readTotal = 0;
        while (readTotal < length)
        {
            int toRead = Math.Min(buffer.Length, length - readTotal);
            int read = socket.Receive(buffer, 0, toRead, SocketFlags.None);
            if (read == 0) throw new Exception("Socket closed early while discarding");
            readTotal += read;
        }
    }
    public void Stop() => _running = false;
    
}

