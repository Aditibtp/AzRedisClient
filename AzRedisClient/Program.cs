using System;
using System.Threading.Tasks;
using StackExchange.Redis;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.KeyVault;
using System.IO;
using System.Text;

namespace AzRedisClient
{
    class Program
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() => GetNewMux().Result);

        private static async Task<ConnectionMultiplexer> GetNewMux()
        {
            string kvUri = Environment.GetEnvironmentVariable("KVURL");
            string cacheKey = Environment.GetEnvironmentVariable("CAKEY");
            string cacheUrl = Environment.GetEnvironmentVariable("CAURL");
            var tokenProvider = new AzureServiceTokenProvider();
            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(tokenProvider.KeyVaultTokenCallback));

            var pass = await keyVaultClient.GetSecretAsync(kvUri, cacheKey);
            var url = await keyVaultClient.GetSecretAsync(kvUri, cacheUrl);
            string connectionStr = url.Value + " ,password=" + pass.Value;

            return ConnectionMultiplexer.Connect(connectionStr);
        }

        public static ConnectionMultiplexer Connection
        {
            get
            {
               return lazyConnection.Value;
            }
        }

        public static IDatabase cache;

        private static int AsyncMethod()
        {
            //await Task.Yield();
            int lineNum = 301001;
            for(int i=0; i < 2048; i++)
            {
                try
                {
                    // Create an instance of StreamReader to read from a file.
                    // The using statement also closes the StreamReader.
                    using (StreamReader sr = new StreamReader(@"C:\Users\Public\SampleFile.txt"))
                    {
                        string line;
                        // Read and display lines from the file until the end of
                        // the file is reached.
                        while ((line = sr.ReadLine()) != null)
                        {
                            Console.WriteLine(lineNum);
                            cache.StringSet("Message" + lineNum+" "+i, line);
                            lineNum++;
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("The file could not be read:");
                    Console.WriteLine(e.Message);
                }
            }
            

            return lineNum;
        }

        public static async void readBigFile()
        {
            Task<int> task = new Task<int>(AsyncMethod);
            
            task.Start();
            int totalLinesWritten = await task;
            Console.WriteLine("Finished reading big file!!");
            lazyConnection.Value.Dispose();
        }

        static void Main(string[] args)
        { 

            try
            {
                var mux = Connection;
            }
            catch (RedisTimeoutException)
            {
                lazyConnection = new Lazy<ConnectionMultiplexer>(() => GetNewMux().Result);
            }

            cache = Connection.GetDatabase();

            // Perform cache operations using the cache object...

            // Simple PING command
            string cacheCommand = "PING";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            Console.WriteLine("Cache response : " + cache.Execute(cacheCommand).ToString());

            // Simple get and put of integral data types into the cache
            cacheCommand = "GET Message";
            Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
            Console.WriteLine("Cache response : " + cache.StringGet("Message").ToString());

            cacheCommand = "SET Message \"Hello! The cache is working from a .NET console app!\"";
            Console.WriteLine("\nCache command  : " + cacheCommand + " or StringSet()");
            Console.WriteLine("Cache response : " + cache.StringSet("Message", "Hello! The cache is working from a .NET console app!").ToString());

            // Demonstrate "SET Message" executed as expected...
            cacheCommand = "GET Message";
            Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
            Console.WriteLine("Cache response : " + cache.StringGet("Message").ToString());

            cacheCommand = "GET Message from previous key write";
            //Console.WriteLine("\nCache command  : " + cacheCommand + " or StringGet()");
           // Console.WriteLine("Cache response : " + cache.StringGet("Message100001").ToString());

            // Get the client list, useful to see if connection list is growing...
            cacheCommand = "CLIENT LIST";
            Console.WriteLine("\nCache command  : " + cacheCommand);
            Console.WriteLine("Cache response : \n" + cache.Execute("CLIENT", "LIST").ToString().Replace("id=", "id="));

            readBigFile();

            //lazyConnection.Value.Dispose();

            Console.ReadLine();

        }
    }
}
