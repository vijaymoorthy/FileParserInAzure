using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using Shared.Fileprocessing;

namespace Fileprocessing
{
    public static  class FileProcessingOrch
    {
        [FunctionName("FilePublishOrchestrator")]      
            public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {


            log.LogInformation($"************** RunOrchestrator method executing ********************");
            List<TransferFileInfo> files = await context.CallActivityAsync<List<TransferFileInfo>>(
              "FileShareReader",
              null);
            log.LogInformation($"************** Splitter  fan out and Function chain********************");
            List<TransferFileInfo> splitfiles = new List<TransferFileInfo>();
            foreach (var file in files)
            {
              splitfiles = await context.CallActivityAsync<List<TransferFileInfo>>(
              "Filespliter",
              file);              
            }
            log.LogInformation($"************** send message with Fanning out ********************");
            var parallelActivities = new List<Task<string>>();
            foreach (var file in splitfiles)
            {
                file.IsPatternMatched = false; //initilized
                // Start a new activity function and capture the task reference
                Task<string> task = context.CallActivityAsync<string>("Filepublisher", file);

                // Store the task reference for later
                parallelActivities.Add(task);
            }
            // Wait until all the activity functions have done their work
            log.LogInformation($"************** 'Waiting' for parallel results ********************");
            await Task.WhenAll(parallelActivities);
            log.LogInformation($"************** All activity functions complete ********************");

            // Now that all parallel activity functions have completed,
            // fan in AKA aggregate the results, in this case into a single
            // string using a StringBuilder
            log.LogInformation($"************** fanning in ********************");
            var sb = new StringBuilder();
            foreach (var completedParallelActivity in parallelActivities)
            {
                sb.AppendLine(completedParallelActivity.Result);
            }

            return sb.ToString();



        }

       



        //ToDo: Change from HTTPS trigger to Timertrigger 
        [FunctionName("GetOutputData")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("FilePublishOrchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}