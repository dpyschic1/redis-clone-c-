using System.Net;
using System.Net.Sockets;
using Server;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

var redisParser = new RedisProtocolParser();
var commandExecutor = new CommandExecutor();
var redisSerializer = new RedisSerializer();

var port = args.Length > 0 ? int.Parse(args[0]) : 6379;


var eventLoop = new EventLoop(port, redisParser, commandExecutor, redisSerializer);
eventLoop.Run();