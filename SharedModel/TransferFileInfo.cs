using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared.Fileprocessing
{
    public class TransferFileInfo
    {
        public string FileName { get; set; }
        public string TextLine { get; set; }

        public bool IsPatternMatched { get; set; }
    }

    public class Greeting
    {
        public string CityName { get; set; }
        public string Message { get; set; }
    }

    public class GreetingsRequest
    {
        public List<Greeting> Greetings { get; set; }
    }
}
