using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Shared.Fileprocessing;

namespace AzureFunctionDurableSubscriber
{
    public static class ReceiveFileFromMessageBus
    {
        [FunctionName("ProcessFilesOrchestrator")]        
        
        public static async Task<string> ProcessFilesOrchestrator(            
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var parallelActivities = new List<Task<string>>();
            TransferFileInfo transferFileInfo= context.GetInput<TransferFileInfo>();

            string result=await context.CallActivityAsync<string>("ParseFiles", transferFileInfo);    

            //var sb = new StringBuilder();
            //foreach (var completedParallelActivity in parallelActivities)
            //{
            //    sb.AppendLine(completedParallelActivity.Result);
            //}

            return result;
        }

        

        [FunctionName("ReceiveFileFromMessageBus")]
        public static async Task Run(
            [ServiceBusTrigger("%TopicName%", "%SubscriberName%", Connection="ServiceBusConnection")] Message message ,                       
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            

            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("ProcessFilesOrchestrator",
                new TransferFileInfo
                {
                    FileName = message.UserProperties["filename"].ToString(),
                    TextLine= Encoding.UTF8.GetString(message.Body)
                }
                );


            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");            
        }

        //This will clean up the file in share folder with unmatched pattern  from the Azure Service bus queue
        [FunctionName("CheckAndCleanUp")]
        public static void CheckAndCleanUp(
            [ServiceBusTrigger("processedfilereceiver", Connection = "ServiceBusConnection")]
            string filenametodelete, ILogger log)
        {
            log.LogInformation($"check and clean up  {filenametodelete}.");            
        }

    }
}