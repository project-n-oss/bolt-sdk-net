# .NET SDK for Bolt

This .NET solution provides a .NET Client Library for Bolt.

It can be built using any of the standard .NET IDEs (including Microsoft Visual Studio and Jetbrains Rider) using the included project files.

## Using .NET SDK for Bolt

* Clone and compile the SDK repo into your project :
   ```bash
    git clone https://github.com/project-n-oss/bolt-sdk-net
   ```
   
* Configure the bolt custom domain, this can be done through either one of the below.
    * Set up environment variable 'BOLT_CUSTOM_DOMAIN'
    * Set up BoltConfiguration.CustomDomain

   ```cs
    BoltConfiguration.CustomDomain = "{Bolt Custom Domain}";
   ```
    
   This configuration option should be placed in somewhere like Startup.cs so as to set it up before the BoltS3Client instance creation.

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

* Configure the AWS Region in case if it's not available in the EC2InstanceMetadata, this can be done through either one of the below.
    * Set up environment variable 'AWS_REGION'
    * Set up BoltConfiguration.Region

* (Optional) Configure the AWS Zone Id in case if it's not available in the EC2InstanceMetadata, this can be done through either one of the below.
    * Set up environment variable 'AWS_ZONE_ID'
    * Set up BoltConfiguration.ZoneId
