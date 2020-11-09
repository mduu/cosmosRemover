# Cosmos Remover
A small .Net Core CLI tool that does remove documents from a Azure Cosmos DB using a SQL query and multithreading. It uses C# `ActionBlock<>` to parallelize execution in multiple thread. I used 30 threads but this depends on the max. RU's you have as well as on your network and CPU.

## How to use

1. Open `Program.cs`
1. Change the parameters on the top of the file to your values
1. Run the tool

Hope this tool helps!
Marc
