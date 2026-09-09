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
            bool isPatternMatched = ValidatePattern(new StringReader(transferFileInfo.TextLine), PATTERN);
            if (!isPatternMatched)
            {
                transferFileInfo.IsPatternMatched = false;
            }
            log.LogInformation($"ParseFiles {transferFileInfo.FileName}.");
            if (!transferFileInfo.IsPatternMatched)
            {
                return $"filename:{transferFileInfo.FileName}";
            }
            else
                return null;            
            
        }

        internal static bool ValidatePattern(TextReader reader, string pattern)
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                if (!Regex.IsMatch(line, WildCardToRegular(pattern)))
                {
                    return false;
                }
            }

            return true;
        }

        internal static String WildCardToRegular(String value)
        {
            return "^" + Regex.Escape(value).Replace("\\?", ".").Replace("\\*", ".*") + "$";
        }
    }
}
