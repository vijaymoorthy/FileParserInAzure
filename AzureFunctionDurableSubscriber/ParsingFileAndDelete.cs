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
using System.Threading;
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
            transferFileInfo.IsPatternMatched = ValidatePattern(new StringReader(transferFileInfo.TextLine), PATTERN, CancellationToken.None);
            log.LogInformation($"ParseFiles {transferFileInfo.FileName}.");
            if (!transferFileInfo.IsPatternMatched)
            {
                return $"filename:{transferFileInfo.FileName}";
            }
            else
                return null;            
            
        }

        internal static bool ValidatePattern(TextReader reader, string pattern, CancellationToken cancellationToken)
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!Regex.IsMatch(line, WildCardToRegular(pattern)))
                {
                    return false;
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            return true;
        }

        internal static String WildCardToRegular(String value)
        {
            return "^" + Regex.Escape(value).Replace("\\?", ".").Replace("\\*", ".*") + "$";
        }
    }
}
