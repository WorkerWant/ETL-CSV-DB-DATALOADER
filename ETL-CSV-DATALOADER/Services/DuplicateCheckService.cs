using ETL_CSV_DATALOADER.Model;

namespace ETL_CSV_DATALOADER.Services
{
    /// <summary>
    /// Provides methods for detecting duplicate records.
    /// </summary>
    public class DuplicateCheckService
    {
        public static (List<TaxiTripDto> uniqueRecords, List<TaxiTripDto> duplicates) SeparateDuplicates(List<TaxiTripDto> records)
        {
            var seen = new HashSet<string>();
            var unique = new List<TaxiTripDto>();
            var dups = new List<TaxiTripDto>();

            foreach (var record in records)
            {
                string key = $"{record.TpepPickupDatetime}|{record.TpepDropoffDatetime}|{record.PassengerCount}";
                if (seen.Contains(key))
                {
                    dups.Add(record);
                }
                else
                {
                    seen.Add(key);
                    unique.Add(record);
                }
            }

            return (unique, dups);
        }
    }
}