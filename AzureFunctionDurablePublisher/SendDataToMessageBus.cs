using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Shared.Fileprocessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Fileprocessing
{
    
    public class SendData
    {
        
        [FunctionName("Filepublisher")]
        //[return: ServiceBus("sourcefile", Connection = "ServiceBus")]
        public static string Filepublisher([ActivityTrigger] TransferFileInfo item
            ,[ServiceBus("sourcefile", Connection = "ServiceBus")] out Message message
            , ILogger log            
            )
        {
            

            var msg = new Message();
            //message.TimeToLive = TimeSpan.FromSeconds(10);
            var systemProperties = new Message.SystemPropertiesCollection();

            // systemProperties.EnqueuedTimeUtc = DateTime.UtcNow.AddMinutes(1);
            var bindings = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.SetProperty;
            var value = DateTime.UtcNow.AddMinutes(1);
            systemProperties.GetType().InvokeMember("EnqueuedTimeUtc", bindings, Type.DefaultBinder, systemProperties, new object[] { value });
            // workaround "ThrowIfNotReceived" by setting "SequenceNumber" value
            systemProperties.GetType().InvokeMember("SequenceNumber", bindings, Type.DefaultBinder, systemProperties, new object[] { 1 });

            // message.systemProperties = systemProperties;
            bindings = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.SetProperty;
            msg.GetType().InvokeMember("SystemProperties", bindings, Type.DefaultBinder, msg, new object[] { systemProperties });


            //send the data to Azure service bus publisher
            log.LogInformation($"Send message {item.FileName} text");
            msg.ContentType = "text/plain";   
            msg.Body = Encoding.UTF8.GetBytes(item.TextLine);    
            msg.UserProperties.Add("filename",item.FileName);
            msg.UserProperties.Add("IsPatternMatched", item.IsPatternMatched);
            message = msg;
            return  $"{{file:{item.FileName},text:{item.TextLine}}}";            
        }

        [FunctionName("FanOutIn_ActivityFunction")]
        public static string SayHello([ActivityTrigger] Greeting greeting, ILogger log)
        {
            // simulate longer processing delay to demonstrate parallelism
            Thread.Sleep(15000);

            return $"{greeting.Message} {greeting.CityName}";
        }

        [FunctionName("Filespliter")]
        public static async Task<List<TransferFileInfo>> Filespliter([ActivityTrigger] TransferFileInfo file, ILogger log)
        {

            var reader = new StringReader(file.TextLine);
            string line;
            int countLine = 0, samefilenumber = 1;
            int SPLIT_BY_LINES = Convert.ToInt32(Environment.GetEnvironmentVariable("MaxLineSize"));
            string currentLines = string.Empty;


            List<TransferFileInfo> transferFileInfos = new List<TransferFileInfo>();

            while ((line = reader.ReadLine()) != null)
            {
                countLine++;
                Console.WriteLine($"{line}");
                currentLines += line + "\r\n";
                if (countLine >= SPLIT_BY_LINES)
                {

                    transferFileInfos.Add(new TransferFileInfo
                    {
                        FileName = $"{file.FileName}_{samefilenumber}",
                        TextLine = currentLines
                    });
                    samefilenumber++;
                    currentLines = string.Empty;
                    countLine = 0;
                }

            }

            if (countLine <= SPLIT_BY_LINES)
            {
                samefilenumber++;
                transferFileInfos.Add(new TransferFileInfo
                {
                    FileName = $"{file.FileName}_{samefilenumber}",
                    TextLine = currentLines
                });
            }

            await Task.Delay(1);
            return transferFileInfos;

        }



        [FunctionName("FileShareReader")]
        public static async Task<List<TransferFileInfo>> FileAndContent([ActivityTrigger] string root, ILogger log)
        {

            log.LogInformation($"************** Get list of file in directory ********************");
            // simulate longer processing delay to demonstrate parallelism
            ShareClient share = new ShareClient(System.Environment.GetEnvironmentVariable("AzureWebJobsStorage"),
                    "filestoprocess");

            await share.CreateIfNotExistsAsync();
            List<TransferFileInfo> files = new List<TransferFileInfo>();

            if (await share.ExistsAsync())
            {


                // Get a reference to the sample directory
                ShareDirectoryClient directory = share.GetDirectoryClient("DailyProcessingData");

                //Create the directory if it doesn't already exist
                await directory.CreateIfNotExistsAsync();

                //Ensure that the directory exists
                if (await directory.ExistsAsync())
                {
                    //var outputs = new List<Task<string>>();

                    int i = 0;
                    await foreach (ShareFileItem item in directory.GetFilesAndDirectoriesAsync())
                    {
                        //copy the file to Staging folder                        

                        ShareFileClient file = directory.GetFileClient(item.Name);
                        ShareFileDownloadInfo downloadInfo = await file.DownloadAsync();
                        using (StreamReader reader = new StreamReader(downloadInfo.Content))
                        {
                            string text = reader.ReadToEnd();
                            var input = new TransferFileInfo { FileName = item.Name, TextLine = text };
                            files.Add(input);
                        }
                    }
                }
            }
            log.LogInformation($"************** Get list of file in directory ********************");
            return files;
        }
    }
}
