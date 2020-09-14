# .NET SDK for Bolt

This .NET solution provides a .NET Client Library for Bolt.

It can be built using any of the standard .NET IDEs (including Microsoft Visual Studio and Jetbrains Rider) using the included project files.

## Local linking for testing

To link locally for testing:
 - Build the package in a .NET IDE.
 - Run `nuget pack`
 - Link locally like so in client projects:
   ```bash
   nuget add /path/to/ProjectN.Bolt/ProjectN.Bolt.x.y.z.nupkg -Source ./packages
   dotnet add package ProjectN.Bolt -s ./packages
   ```
