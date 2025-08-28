using System.Net;
using System.Net.Sockets;
using Server;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

var redisParser = new RedisProtocolParser();
var commandExecutor = new CommandExecutor();
var redisSerializer = new RedisSerializer();

var cmdArgs = new Dictionary<string, string>();

for (int i = 0; i < args.Length; i++)
{
    if(args[i].StartsWith("--"))
    {
        var key =  args[i].Substring(2);
        var val = (i + 1 < args.Length && !args[i + 1].StartsWith("--")) 
            ? args[++i] 
            : "true";
        cmdArgs.Add(key, val);
    }
}

ServerInfo.Port = cmdArgs.TryGetValue("port", out var portStr) ? int.Parse(portStr) : 6379;

var isReplicaOf = cmdArgs.TryGetValue("replicaof", out var hostAddress);

if (isReplicaOf)
{
    var hostAddressParts = hostAddress.Split(' ');
    ServerInfo.MasterHost = "slave";
    ServerInfo.MasterAddress = hostAddressParts[0];
    ServerInfo.MasterPort = int.Parse(hostAddressParts[1]);
}


var eventLoop = new EventLoop(ServerInfo.Port, redisParser, commandExecutor, redisSerializer);
eventLoop.Run();

public static class ServerInfo
{
    public static string MasterHost { get; set; } = "master";
    public static string MasterReplicaId { get;} = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    public static int MasterReplicaOffset {get;set;} = 0;
    
    public static string? MasterAddress  {get;set;}
    public static int? MasterPort {get;set;}
    public static bool IsMaster => MasterHost == "master";
    
    public static int Port { get; set; }

    public static string ToStringReplication()
    {
        return $"role:{MasterHost}\nmaster_replid:{MasterReplicaId}\nmaster_repl_offset:{MasterReplicaOffset}";
    }
}