namespace Server;

public static class HandShakeResponse
{
    public static RedisCommand Ping() => RedisResponse.Array(RedisResponse.String("PING"));

    public static RedisCommand ReplConfPort()
    {
        var replconf = RedisResponse.String("REPLCONF");
        var listeningPort = RedisResponse.String("listening-port");
        var portString = RedisResponse.String(ServerInfo.Port.ToString());
        return RedisResponse.Array(replconf, listeningPort, portString);
    }
    
    public static RedisCommand ReplConfCapa()
    {
        var replconf = RedisResponse.String("REPLCONF");
        var capa = RedisResponse.String("capa");
        var capaString = RedisResponse.String("psync2");
        return RedisResponse.Array(replconf, capa, capaString);
    }

    public static RedisCommand PSync()
    {
        var psync = RedisResponse.String("PSYNC");
        var psyncMasterId = RedisResponse.String("?");
        var psyncMasterOffset = RedisResponse.String("-1");
        return RedisResponse.Array(psync, psyncMasterId, psyncMasterOffset);
    }
}