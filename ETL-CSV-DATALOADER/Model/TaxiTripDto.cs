using CsvHelper.Configuration.Attributes;

namespace ETL_CSV_DATALOADER.Model
{
    /// <summary>
    /// DTO for raw CSV fields.
    /// </summary>
    public class TaxiTripDto
    {
        [Name("tpep_pickup_datetime")]
        public required string TpepPickupDatetime { get; set; }

        [Name("tpep_dropoff_datetime")]
        public required string TpepDropoffDatetime { get; set; }

        [Name("passenger_count")]
        public required string PassengerCount { get; set; }

        [Name("trip_distance")]
        public required string TripDistance { get; set; }

        [Name("store_and_fwd_flag")]
        public required string StoreAndFwdFlag { get; set; }

        [Name("PULocationID")]
        public required string PULocationID { get; set; }

        [Name("DOLocationID")]
        public required string DOLocationID { get; set; }

        [Name("fare_amount")]
        public required string FareAmount { get; set; }

        [Name("tip_amount")]
        public required string TipAmount { get; set; }
    }
}
