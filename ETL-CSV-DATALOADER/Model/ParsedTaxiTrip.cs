namespace ETL_CSV_DATALOADER.Model
{
    public class ParsedTaxiTrip
    {
        public DateTime Pickup { get; set; }
        public DateTime Dropoff { get; set; }
        public int PassengerCount { get; set; }
        public decimal TripDistance { get; set; }
        public required string StoreAndFwdFlag { get; set; }
        public int PULocationID { get; set; }
        public int DOLocationID { get; set; }
        public decimal FareAmount { get; set; }
        public decimal TipAmount { get; set; }
    }
}
