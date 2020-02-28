/****************************************************************************

MIT License

Copyright (c) 2019 AndyDragon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

****************************************************************************/

using CoenM.ExifToolLib;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace ImageDetailsCataloger
{
    class Program
    {
        class ExifData
        {
            public string Formatted;
            public string Raw;
        }

        class Statistics
        {
            public int Added;
            public int Updated;
            public int ColumnsAdded;
        }

        static void Main(string[] args)
        {
            if (args.Count() < 1)
            {
                Console.WriteLine("Include a file name or multiple file names as parameters");
                return;
            }

            var homeFolder = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

            var exifToolPath = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"exiftool.exe" : @"exiftool";
            var exifToolResultEncoding = Encoding.UTF8;
            var exifToolResultNewLine = "\r\n";
            var config = new AsyncExifToolConfiguration(exifToolPath, exifToolResultEncoding, exifToolResultNewLine, null);
            var asyncExifTool = new AsyncExifTool(config);
            asyncExifTool.Initialize();

            if (!File.Exists(Path.Combine(homeFolder, "ImageDetails.sqlite")))
            {
                Console.WriteLine("Creating SQLite DB");

                SQLiteConnection.CreateFile(Path.Combine(homeFolder, "ImageDetails.sqlite"));
            }
            var sqlite = new SQLiteConnection("Data Source='" + Path.Combine(homeFolder, "ImageDetails.sqlite") + "'");
            sqlite.Open();

            var command1 = new SQLiteCommand("CREATE TABLE IF NOT EXISTS image_details (id integer PRIMARY KEY, file TEXT NOT NULL UNIQUE);", sqlite);
            command1.ExecuteNonQuery();

            var data = new Dictionary<string, Dictionary<string, ExifData>>();
            var statistics = new Statistics();

            foreach (var arg in args)
            {

                if (Directory.Exists(arg))
                {
                    var files = Directory.GetFiles(arg);
                    Array.Sort(files, StringComparer.OrdinalIgnoreCase);
                    foreach (var filePath in files)
                    {
                        if (string.Equals(Path.GetExtension(filePath), ".NEF", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(Path.GetExtension(filePath), ".ORF", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(Path.GetExtension(filePath), ".ARW", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(Path.GetExtension(filePath), ".CR2", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(Path.GetExtension(filePath), ".CR3", StringComparison.OrdinalIgnoreCase))
                        {
                            ParseImageFile(filePath, asyncExifTool, data);
                            if (data.Keys.Count >= 50)
                            {
                                asyncExifTool.DisposeAsync().AsTask().Wait();

                                // To avoid memory issues, write to the DB every so many files.
                                WriteDataToDB(sqlite, data, statistics);

                                asyncExifTool = new AsyncExifTool(config);
                                asyncExifTool.Initialize();
                            }
                        }
                    }
                }
                else if (File.Exists(arg))
                {
                    ParseImageFile(arg, asyncExifTool, data);
                }
                else
                {
                    Console.WriteLine("Could not load file {0}...", arg);
                }
            }


            WriteDataToDB(sqlite, data, statistics);

            sqlite.Close();

            Console.WriteLine("--------------------------------------------------------------");
            Console.WriteLine(" Added {0} files to the database", statistics.Added);
            Console.WriteLine(" Updated {0} files in the database", statistics.Updated);
            Console.WriteLine(" Added {0} columns to the database", statistics.ColumnsAdded);
            Console.WriteLine("--------------------------------------------------------------");
        }

        private static void WriteDataToDB(SQLiteConnection sqlite, Dictionary<string, Dictionary<string, ExifData>> data, Statistics statistics)
        {
            var command = new SQLiteCommand("PRAGMA table_info('image_details');", sqlite);
            var columnReader = command.ExecuteReader();

            var columns = new List<string>();
            while (columnReader.Read())
            {
                columns.Add(columnReader.GetString(1));
            }

            var newColumns = new List<string>();
            foreach (var fileKey in data.Keys)
            {
                foreach (var rowKey in data[fileKey].Keys)
                {
                    if (!columns.Contains(rowKey) && !newColumns.Contains(rowKey))
                    {
                        newColumns.Add(rowKey);
                    }
                    if (!columns.Contains(rowKey + ":raw") && !newColumns.Contains(rowKey + ":raw"))
                    {
                        newColumns.Add(rowKey + ":raw");
                    }
                }
            }

            if (newColumns.Count != 0)
            {
                Console.WriteLine("Adding {0} columns to DB", newColumns.Count);

                foreach (var newColumn in newColumns)
                {
                    command = new SQLiteCommand("ALTER TABLE image_details ADD COLUMN '" + newColumn + "' TEXT;", sqlite);
                    command.ExecuteNonQuery();
                    ++statistics.ColumnsAdded;
                }
            }

            command = new SQLiteCommand("PRAGMA table_info('image_details');", sqlite);
            columnReader = command.ExecuteReader();

            columns = new List<string>();
            while (columnReader.Read())
            {
                columns.Add(columnReader.GetString(1));
            }

            Console.WriteLine("Adding/Updating {0} files to DB", data.Keys.Count);

            foreach (var fileKey in data.Keys)
            {
                var fileData = data[fileKey];
                command = new SQLiteCommand("SELECT * FROM image_details WHERE file = '" + fileKey + "';", sqlite);
                var fileRowReader = command.ExecuteReader();
                if (fileRowReader.Read())
                {
                    var fileColumns = fileData.Keys;
                    var nullColumns = columns.Where(column => !fileColumns.Any(rawColumn => column == rawColumn));
                    var updateColumns = fileColumns.Select(fileColumn => "'" + fileColumn.Replace("'", "''") + "' = '" + fileData[fileColumn].Formatted.Replace("'", "''") + "'")
                                        .Concat(fileColumns.Select(fileColumn => "'" + fileColumn.Replace("'", "''") + ":raw' = '" + fileData[fileColumn].Raw.Replace("'", "''") + "'"));
                    command = new SQLiteCommand("UPDATE image_details SET " + string.Join(", ", updateColumns) + " WHERE file = '" + fileKey + "'", sqlite);
                    command.ExecuteNonQuery();
                    ++statistics.Updated;
                }
                else
                {
                    var fileColumns = fileData.Keys;
                    var insertColumns = fileColumns.Select(key => "'" + key.Replace("'", "''") + "'")
                                        .Concat(fileColumns.Select(key => "'" + key.Replace("'", "''") + ":raw'"));
                    var insertValues = fileData.Values.Select(value => "'" + value.Formatted.Replace("'", "''") + "'")
                                       .Concat(fileData.Values.Select(value => "'" + value.Raw.Replace("'", "''") + "'"));
                    command = new SQLiteCommand("INSERT INTO image_details (file, " + string.Join(", ", insertColumns) + ") VALUES ('" + fileKey + "', " + string.Join(", ", insertValues) + ");", sqlite);
                    command.ExecuteNonQuery();
                    ++statistics.Added;
                }
            }

            data.Clear();
        }

        private static void ParseImageFile(string path, AsyncExifTool asyncExifTool, Dictionary<string, Dictionary<string, ExifData>> data)
        {
            Console.WriteLine("Processing {0}...", path);

            var fileData = new Dictionary<string, ExifData>();
            data.Add(path, fileData);

            ParseFileData(asyncExifTool.ExecuteAsync(new[] { "-a", "-g", "-sort", path }).ConfigureAwait(false).GetAwaiter().GetResult(), fileData, true);
            ParseFileData(asyncExifTool.ExecuteAsync(new[] { "-a", "-g", "-n", "-sort", path }).ConfigureAwait(false).GetAwaiter().GetResult(), fileData, false);
        }

        private static void ParseFileData(string result, Dictionary<string, ExifData> fileData, bool formattedData)
        {
            var rows = result.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var section = "Root";
            foreach (var row in rows)
            {
                if (row.StartsWith("----"))
                {
                    section = row.Trim(new[] { '-', ' ' });
                    continue;
                }
                var tagSeparatorPosition = row.IndexOf(":");
                if (tagSeparatorPosition != -1)
                {
                    var tag = section + ":" + row.Substring(0, tagSeparatorPosition).Trim();
                    var value = row.Substring(tagSeparatorPosition + 1).Trim();
                    if (fileData.ContainsKey(tag))
                    {
                        if (formattedData)
                        {
                            fileData[tag].Formatted = value;
                        }
                        else
                        {
                            fileData[tag].Raw = value;
                        }
                    }
                    else
                    {
                        if (formattedData)
                        {
                            fileData.Add(tag, new ExifData { Formatted = value });
                        }
                        else
                        {
                            fileData.Add(tag, new ExifData { Raw = value });
                        }
                    }
                }
            }
        }
    }
}
