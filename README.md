# .NET SDK for Bolt

This .NET solution provides a .NET Client Library for Bolt.

It can be built using any of the standard .NET IDEs (including Microsoft Visual Studio and Jetbrains Rider) using the included project files.

## Using .NET SDK for Bolt

* Clone and compile the SDK repo into your project :
   ```bash
    git clone https://github.com/project-n-oss/bolt-sdk-net
   ```
   
* Set the bolt custom domain configuration

   ```cs
    BoltConfiguration.CustomDomain = "{Bolt Custom Domain}";
   ```
    
   This configuration option should be placed in somewhere like Starup.cs so as to set it up before the BoltS3Client instance creation.

   Ex:  
   ```cs
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            BoltConfiguration.CustomDomain = "dev.bolt.projectn.co";
            ...
        }
        else
        {
            BoltConfiguration.CustomDomain = "prod.bolt.projectn.co";
            ...
        }

        ...
        ...
    }
    ```

   Bolt custom domain can also be set through the environment variable 'BOLT_CUSTOM_DOMAIN'
