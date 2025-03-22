#pragma warning disable CS8600

using CsvHelper;
using CsvHelper.Configuration;
using ETL_CSV_DATALOADER.Model;
using ETL_CSV_DATALOADER.Services;
using System.Globalization;
using TimeZoneConverter;

namespace ETL_CSV_DATALOADER
{
    /// <summary>
    /// Entry point. Reads CSV line by line, applies cleaning and validation, detects duplicates, and inserts valid records in batches.
    /// </summary>
    public class Program
    {
        private const string DataFolder = "Data";
        private const int BatchSize = 5000;

        public static async Task Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.White;

            string connectionString = Environment.GetEnvironmentVariable("CONNECTION_STRING");
            if (string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("CONNECTION_STRING environment variable is not set.");
                return;
            }

            string csvFilePath = ResolveCsvFilePath(args);
            if (string.IsNullOrEmpty(csvFilePath))
            {
                return;
            }

            var invalidLines = new List<string>();
            var duplicateLines = new List<string>();
            var duplicatesHash = new HashSet<string>();
            var dataTable = BulkInsertService.CreateTableSchema();
            int totalInserted = 0;
            int totalDuplicates = 0;
            int totalInvalid = 0;

            var estZone = TZConvert.GetTimeZoneInfo("America/New_York");
            var utcZone = TimeZoneInfo.Utc;

            using var reader = new StreamReader(csvFilePath);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                BadDataFound = null
            };
            using var csv = new CsvReader(reader, config);

            csv.Read();
            csv.ReadHeader();
            var headerRow = csv.HeaderRecord;

            while (csv.Read())
            {
                try
                {
                    var record = csv.GetRecord<TaxiTripDto>();
                    DataCleaningService.CleanRecord(record);
                    var (isValid, parsed) = DataCleaningService.ValidateAndParse(record, estZone, utcZone);
                    if (!isValid)
                    {
                        totalInvalid++;
                        invalidLines.Add(
                            $"{record.TpepPickupDatetime},{record.TpepDropoffDatetime},{record.PassengerCount}," +
                            $"{record.TripDistance},{record.StoreAndFwdFlag},{record.PULocationID}," +
                            $"{record.DOLocationID},{record.FareAmount},{record.TipAmount}"
                        );
                        continue;
                    }

                    string key = $"{parsed.Pickup:O}|{parsed.Dropoff:O}|{parsed.PassengerCount}";
                    if (!duplicatesHash.Add(key))
                    {
                        totalDuplicates++;
                        duplicateLines.Add(
                            $"{record.TpepPickupDatetime},{record.TpepDropoffDatetime},{record.PassengerCount}," +
                            $"{record.TripDistance},{record.StoreAndFwdFlag},{record.PULocationID}," +
                            $"{record.DOLocationID},{record.FareAmount},{record.TipAmount}"
                        );
                        continue;
                    }

                    dataTable.Rows.Add(
                        parsed.Pickup,
                        parsed.Dropoff,
                        parsed.PassengerCount,
                        parsed.TripDistance,
                        parsed.StoreAndFwdFlag,
                        parsed.PULocationID,
                        parsed.DOLocationID,
                        parsed.FareAmount,
                        parsed.TipAmount
                    );

                    if (dataTable.Rows.Count >= BatchSize)
                    {
                        totalInserted += await BulkInsertService.InsertBatchAsync(connectionString, dataTable);
                        dataTable.Clear();
                    }
                }
                catch
                {
                    totalInvalid++;
                    var rawRecord = csv.Parser.RawRecord ?? "";
                    invalidLines.Add(rawRecord.TrimEnd('\r', '\n'));
                }
            }

            if (dataTable.Rows.Count > 0)
            {
                totalInserted += await BulkInsertService.InsertBatchAsync(connectionString, dataTable);
                dataTable.Clear();
            }

            if (invalidLines.Count > 0 && headerRow != null)
            {
                Directory.CreateDirectory(DataFolder);
                var filePath = Path.Combine(DataFolder, "invalid_rows.csv");
                var header = string.Join(",", headerRow);
                await File.WriteAllLinesAsync(filePath, new[] { header }.Concat(invalidLines));
            }

            if (duplicateLines.Count > 0 && headerRow != null)
            {
                Directory.CreateDirectory(DataFolder);
                var filePath = Path.Combine(DataFolder, "duplicates.csv");
                var header = string.Join(",", headerRow);
                await File.WriteAllLinesAsync(filePath, new[] { header }.Concat(duplicateLines));
            }

            Console.WriteLine("Done!");
            Console.WriteLine($"  Inserted   : {totalInserted}");
            Console.WriteLine($"  Duplicates : {totalDuplicates}");
            Console.WriteLine($"  Invalid    : {totalInvalid}");
        }

        private static string ResolveCsvFilePath(string[] args)
        {
            if (args.Length > 0)
            {
                return args[0];
            }

            if (!Directory.Exists(DataFolder))
            {
                Console.WriteLine("Data folder does not exist. Cannot find file.");
                return "";
            }

            var files = Directory.GetFiles(DataFolder, "*.csv", SearchOption.TopDirectoryOnly);
            if (files.Length == 0)
            {
                Console.WriteLine("No CSV files found in the Data folder.");
                return "";
            }

            return files[0];
        }
    }
}