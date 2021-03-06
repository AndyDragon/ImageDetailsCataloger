﻿/****************************************************************************

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
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace ImageDetailsCataloger
{
    class Program
    {
        class ExifData
        {
            public string Formatted { get; set; }
            public string Raw { get; set; }
        }

        class Statistics
        {
            public int Added { get; set; }
            public int Updated { get; set; }
            public int ColumnsAdded { get; set; }
        }

        class Options
        {
            public bool RecursiveFolders { get; set; }
            public IList<string> FolderSearchExtensions { get; set; }
            public bool RecycleExifTool { get; set; }
            public int FilesInBatch { get; set; }
            public bool UseHomeFolder { get; set; }
        }

        static void Main(string[] args)
        {
            if (args.Count() < 1)
            {
                Console.WriteLine("Include a file name or multiple file names as parameters");
                return;
            }

            var location = Path.GetDirectoryName(Assembly.GetEntryAssembly().GetFiles()[0].Name);
            var homeFolder = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var options = ReadOptions(".imagedetailscataloger_options.json", homeFolder);
            if (options == null)
            {
                // Fall-back to the default options.
                Console.WriteLine("Using default options, user options not found");
                options = ReadOptions("options.json", location);
                if (options == null)
                {
                    Console.Error.WriteLine("Cannot find the Options JSON file in the application folder");
                }
            }

            var databaseLocation = Path.Combine(options.UseHomeFolder ? homeFolder : location, "ImageDetails.sqlite");

            var exifToolPath = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"exiftool.exe" : @"exiftool";
            var exifToolResultEncoding = Encoding.UTF8;
            var config = new AsyncExifToolConfiguration(exifToolPath, exifToolResultEncoding, null);
            var asyncExifTool = new AsyncExifTool(config);
            asyncExifTool.Initialize();

            if (!File.Exists(databaseLocation))
            {
                Console.WriteLine("Creating SQLite DB");

                SQLiteConnection.CreateFile(databaseLocation);
            }
            var sqlite = new SQLiteConnection("Data Source='" + databaseLocation + "'");
            sqlite.Open();

            var command1 = new SQLiteCommand("CREATE TABLE IF NOT EXISTS image_details (id integer PRIMARY KEY, file TEXT NOT NULL UNIQUE);", sqlite);
            command1.ExecuteNonQuery();

            var data = new Dictionary<string, Dictionary<string, ExifData>>();
            var statistics = new Statistics();

            foreach (var arg in args)
            {

                if (Directory.Exists(arg))
                {
                    var searchOptions = options.RecursiveFolders ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
                    var files = Directory.GetFiles(arg, "*.*", searchOptions);
                    Array.Sort(files, StringComparer.OrdinalIgnoreCase);
                    foreach (var filePath in files)
                    {
                        if (options.FolderSearchExtensions.Any(extension => string.Equals(Path.GetExtension(filePath), extension, StringComparison.OrdinalIgnoreCase)))
                        {
                            ParseImageFile(filePath, asyncExifTool, data);
                            if (data.Keys.Count >= options.FilesInBatch)
                            {
                                if (options.RecycleExifTool)
                                {
                                    asyncExifTool.DisposeAsync().AsTask().Wait();
                                }

                                // To avoid memory issues, write to the DB every so many files.
                                WriteDataToDB(sqlite, data, statistics);

                                if (options.RecycleExifTool)
                                {
                                    asyncExifTool = new AsyncExifTool(config);
                                    asyncExifTool.Initialize();
                                }
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


        private static Options ReadOptions(string optionsFileName, string location)
        {
            var optionsFile = Path.Combine(location, optionsFileName);
            if (!File.Exists(optionsFile))
            {
                return null;
            }

            var options = JsonConvert.DeserializeObject<Options>(File.ReadAllText(optionsFile));
            return options;
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
            var rows = result.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
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
                if (fileData.Keys.Count() == 0)
                {
                    Console.WriteLine("Skipping {0}, no EXIF data", fileKey);
                    continue;
                }
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
            GC.Collect();
        }
    }
}
