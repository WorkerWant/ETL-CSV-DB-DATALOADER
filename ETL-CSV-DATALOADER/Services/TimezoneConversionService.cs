using ETL_CSV_DATALOADER.Model;
using System;
using TimeZoneConverter;

namespace ETL_CSV_DATALOADER.Services
{
    /// <summary>
    /// Converts times from EST to UTC.
    /// </summary>
    public class TimezoneConversionService
    {
        public static List<TaxiTripDto> ConvertEstToUtc(List<TaxiTripDto> records)
        {
            var estZone = TZConvert.GetTimeZoneInfo("America/New_York");
            foreach (var r in records)
            {
                if (DateTime.TryParse(r.TpepPickupDatetime, out var pickupLocal))
                {
                    var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, estZone);
                    r.TpepPickupDatetime = pickupUtc.ToString("yyyy-MM-dd HH:mm:ss");
                }
                if (DateTime.TryParse(r.TpepDropoffDatetime, out var dropoffLocal))
                {
                    var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, estZone);
                    r.TpepDropoffDatetime = dropoffUtc.ToString("yyyy-MM-dd HH:mm:ss");
                }
            }
            return records;
        }
    }
}