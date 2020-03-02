using System;
using CoenM.ExifToolLib.Logging;

namespace ImageDetailsCataloger
{
    public class ConsoleLogger : ILogger
    {
        public ConsoleLogger()
        {
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log(LogEntry entry)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("** LOG: {0} : {1}", entry.Severity, entry.Message);
            Console.ResetColor();
        }
    }
}
