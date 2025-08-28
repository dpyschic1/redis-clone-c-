### This is a redis like server, implemented in c# with resp compliance.

- The clone implements all the basic db commands such as SET, GET, LPOP, RPOP, LRANGE, INCR etc.
- The clone also supports blocking commands such as blpop and XRead with BLOCK.
- Replication support is not complete, db snapshot boiler plate is in place but no snapshot persistence.
- All the basic data types are supported, except for Sets.
- Transactions are also supported.


Some refacotring and cleaning is required, but the application is functional. 
To run it, just build and execute the executable, optional arguments are ``` --port``` and ``` --replicaof <MASTERHOSTADDR> <MASTERPORT>```
Where the host address and port are optional.