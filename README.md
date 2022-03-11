# .NET SDK for Bolt

This .NET solution provides a .NET Client Library for Bolt.

It can be built using any of the standard .NET IDEs (including Microsoft Visual Studio and Jetbrains Rider) using the included project files.

## Using .NET SDK for Bolt

* Install the SDK package into your project using dotnet CLI:
   ```bash
   dotnet add package ProjectN.Bolt --version 1.0.1
   ```
   
* Add the configurable parameters to app.config

   ```<?xml version="1.0" encoding="UTF-8" ?>
   <configuration>
     <appSettings>
       <add key="SERVICE_URL" value="https://quicksilver.{region}.{subdomain}" />
       <add key="BOLT_HOSTNAME" value="bolt.{region}.{subdomain}" />
     </appSettings>
   </configuration>
   ```
