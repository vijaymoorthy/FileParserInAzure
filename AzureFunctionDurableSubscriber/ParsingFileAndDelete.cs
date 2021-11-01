using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Shared.Fileprocessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AzureFunctionDurableSubscriber
{
    public class ParsingFileAndDelete
    {
        //Todo :seggerate the parser logic and the Service Bus queue message posting
        [FunctionName("ParseFiles")]
        [return: ServiceBus("processedfilereceiver", Connection = "ServiceBusConnection")]
        public static string ParseFiles([ActivityTrigger] TransferFileInfo transferFileInfo, ILogger log)
        {
            string PATTERN =Environment.GetEnvironmentVariable("PartenMatching");
            string line = string.Empty;
            var allLineReader = new StringReader(transferFileInfo.TextLine);
            while ((line = allLineReader.ReadLine()) != null)
            {
                //if any line in the file is not matched the entire file is rejected
                if (!Regex.IsMatch(line, WildCardToRegular(PATTERN)))
                {
                    transferFileInfo.IsPatternMatched = false;                    
                    break;
                }
            }
            log.LogInformation($"ParseFiles {transferFileInfo.FileName}.");
            if (!transferFileInfo.IsPatternMatched)
            {
                return $"filename:{transferFileInfo.FileName}";
            }
            else
                return null;            
            
        }


       
        static String WildCardToRegular(String value)
        {
            return "^" + Regex.Escape(value).Replace("\\?", ".").Replace("\\*", ".*") + "$";
        }
    }
}
