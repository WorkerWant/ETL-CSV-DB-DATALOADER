using CsvHelper;
using CsvHelper.Configuration;
using ETL_CSV_DATALOADER.Model;
using System.Globalization;

namespace ETL_CSV_DATALOADER.Services
{
    /// <summary>
    /// Reads a CSV file and returns a list of TaxiTripDto.
    /// </summary>
    public class CsvProcessingService
    {
        public static List<TaxiTripDto> ReadAll(string filepath)
        {
            var records = new List<TaxiTripDto>();
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                BadDataFound = null
            };
            using var reader = new StreamReader(filepath);
            using var csv = new CsvReader(reader, config);
            records = new List<TaxiTripDto>(csv.GetRecords<TaxiTripDto>());
            return records;
        }
    }
}
