using ETL_CSV_DATALOADER.Model;
using System.Globalization;

namespace ETL_CSV_DATALOADER.Services
{
    /// <summary>
    /// Provides methods for trimming, converting flags, and validating/parsing CSV data into strongly typed objects.
    /// </summary>
    public class DataCleaningService
    {
        public static void CleanRecord(TaxiTripDto record)
        {
            record.TpepPickupDatetime = record.TpepPickupDatetime?.Trim();
            record.TpepDropoffDatetime = record.TpepDropoffDatetime?.Trim();
            record.StoreAndFwdFlag = record.StoreAndFwdFlag?.Trim()?.ToUpperInvariant();

            if (record.StoreAndFwdFlag == "N") record.StoreAndFwdFlag = "No";
            else if (record.StoreAndFwdFlag == "Y") record.StoreAndFwdFlag = "Yes";
        }

        public static (bool isValid, ParsedTaxiTrip? parsed) ValidateAndParse(
            TaxiTripDto raw,
            TimeZoneInfo estZone,
            TimeZoneInfo utcZone)
        {
            if (string.IsNullOrWhiteSpace(raw.TpepPickupDatetime) ||
                string.IsNullOrWhiteSpace(raw.TpepDropoffDatetime) ||
                string.IsNullOrWhiteSpace(raw.PassengerCount) ||
                string.IsNullOrWhiteSpace(raw.TripDistance) ||
                string.IsNullOrWhiteSpace(raw.StoreAndFwdFlag) ||
                string.IsNullOrWhiteSpace(raw.PULocationID) ||
                string.IsNullOrWhiteSpace(raw.DOLocationID) ||
                string.IsNullOrWhiteSpace(raw.FareAmount) ||
                string.IsNullOrWhiteSpace(raw.TipAmount))
            {
                return (false, null);
            }

            if (!DateTime.TryParse(raw.TpepPickupDatetime, out var pickupLocal)) return (false, null);
            if (!DateTime.TryParse(raw.TpepDropoffDatetime, out var dropoffLocal)) return (false, null);
            if (!int.TryParse(raw.PassengerCount, NumberStyles.Any, CultureInfo.InvariantCulture, out var passCount)) return (false, null);
            if (!decimal.TryParse(raw.TripDistance, NumberStyles.Any, CultureInfo.InvariantCulture, out var tripDist)) return (false, null);
            if (!int.TryParse(raw.PULocationID, NumberStyles.Any, CultureInfo.InvariantCulture, out var puLocId)) return (false, null);
            if (!int.TryParse(raw.DOLocationID, NumberStyles.Any, CultureInfo.InvariantCulture, out var doLocId)) return (false, null);
            if (!decimal.TryParse(raw.FareAmount, NumberStyles.Any, CultureInfo.InvariantCulture, out var fareAmount)) return (false, null);
            if (!decimal.TryParse(raw.TipAmount, NumberStyles.Any, CultureInfo.InvariantCulture, out var tipAmount)) return (false, null);

            var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, estZone);
            var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, estZone);

            if (dropoffUtc < pickupUtc)
            {
                Console.WriteLine($"Validation failed: Dropoff time is before pickup time: [{dropoffUtc} < {pickupUtc}");
                return (false, null);
            }

            if (tripDist <= 0)
            {
                Console.WriteLine($"Validation failed: Trip distance <= 0: {tripDist}");
                return (false, null);
            }

            if (passCount <= 0)
            {
                Console.WriteLine($"Validation failed: Passenger count <= 0: {passCount}");
                return (false, null);
            }

            if (tipAmount < 0)
            {
                Console.WriteLine($"Validation failed: Tip amount < 0: {tipAmount}");
                return (false, null);
            }

            if (fareAmount < 0)
            {
                Console.WriteLine($"Validation failed: Fare amount < 0: {fareAmount}");
                return (false, null);
            }

            var parsed = new ParsedTaxiTrip
            {
                Pickup = pickupUtc,
                Dropoff = dropoffUtc,
                PassengerCount = passCount,
                TripDistance = tripDist,
                StoreAndFwdFlag = raw.StoreAndFwdFlag,
                PULocationID = puLocId,
                DOLocationID = doLocId,
                FareAmount = fareAmount,
                TipAmount = tipAmount
            };

            return (true, parsed);
        }
    }
}
