# README

# Description

### GoDistributeFS: Distributed File System

Developed **GoDistributeFS**, a distributed file system in Go that facilitates seamless multi-user data sharing with local-like file operations, including read, write, and create functionalities. Key features include:

- **Centralized Naming Server**: Implemented a naming server for file indexing and storage management, ensuring fault tolerance and optimizing client queries through data replication across multiple servers.
- **Efficient Remote Communication**: Designed remote communication using gRPC and Protocol Buffers, achieving stable, low-latency data transmission even under high concurrency.
- **User Space Mounting with FUSE**: Leveraged FUSE to mount the file system in user space, enabling familiar file operations with standard tools (e.g., Neovim) without disrupting user workflows.
- **Automated Deployment**: Utilized Docker Compose for automated system deployment, simplifying environment setup and ensuring consistency across development and production, resulting in reliable performance.

This project demonstrates a robust solution for managing distributed data with a focus on user experience and operational efficiency.

# Implementation

The language chosen for system's implementation is Go, because it has efficient tools for working with networking and concurrency, aside from an overall simplicity of the language.

## Architectural Diagram

![Architecture schema](Images/GoStore_Arch_(1).png)

## Client:

Client uses ***FUSE*** to interact with the user. This allows to create a transparency level that makes user be able to use traditional tools that they are used to. For example, we used ***Neovim*** to test our file system.

FUSE (File System in User-Space) is a feature of Linux kernel that allows to mount a file system driver to file system node without modifying the kernel or requiring root permission.

It fits our use case perfectly.

## Naming Server:

Implements such administrative functions as registration of a new Storage Server and discovering of Storage Servers storing a requested file. It also help to manage client requests for some file operations by. The operation for listing files in a directory is fully executed by the Naming Server using Index Tree to reduce overhead of excessive connection to Storage Server.

In addition, it stores some useful metadata: local address of the server, list of aliases and IP addresses of registered Storage Servers, tree data structure for files indexing (Index Tree on the diagram).  

## Storage Server:

Storage server has a simple yet crucial role: storing the files themselves. Any number of storage servers is supported in our file system. Only 2 storage servers will be used for a particular file, though.

## Communication Protocols

As communication protocols we used ***gRPC*** Framework and ***Protocol Buffers (protobuf)***. The reasoning for doing so can be easily inferred from following description of these technologies.

gRPC is a high performance, open-source remote procedure call (RPC) framework that can run anywhere. It enables client and server applications to communicate transparently, and makes it easier to build connected systems. 

Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. You define how you want your data to be structured once, then you can use special generated source code to easily write and read your structured data to and from a variety of data streams and using a variety of languages.

# How to launch (for end user)

- [link to DockerHub repo](https://hub.docker.com/u/iammaxim)
- [link to GitHub project](https://github.com/Grupka/project-dfs/tree/master)

---

If you want to test the system on a local machine, you can use the docker-compose file to start server infrastructure in one command. [Here's the link](https://github.com/Grupka/project-dfs/blob/master/docker-compose.yml).

Then, just do a `docker-compose up`.

You can use pre-built `Client` binary (for Linux) right out of the box.

```bash
# Create a mount point
mkdir -p mnt_point
# Start the FUSE client
./Client
# After you are done working with FS, execute
sudo umount mnt_point
```