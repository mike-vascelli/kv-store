## Overview

A distributed key-value storage system that runs across multiple node servers coordinated by a single master server. 
The system is implemented to use recursive queries and iterative replication.
All the servers are provided a set-associative, write-through cache to increase system performance.
Communication between the clients and the server takes place over the network through the use of sockets. 

## Request handling

The master uses its cache to serve the clients' GET requests, and it only goes to the slave servers in case of a miss.
The master forwards PUT and DEL requests over to multiple slave servers, and uses the 2-Phase-Commit protocol to guarantee atomic operations, and consistency.
In case of slave failure, and data loss, the server is brought back up and its data is restored by using a logging system.

<img src='http://www-inst.eecs.berkeley.edu/~cs162/fa14/static/projects/kvstoreimgs/proj4-arch3.png' alt='schema'>
