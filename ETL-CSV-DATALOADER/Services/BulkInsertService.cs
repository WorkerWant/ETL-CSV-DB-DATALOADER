using Microsoft.Data.SqlClient;
using System.Data;

namespace ETL_CSV_DATALOADER.Services
{
    /// <summary>
    /// Performs bulk insert operations to SQL Server.
    /// </summary>
    public class BulkInsertService
    {
        public static DataTable CreateTableSchema()
        {
            var dt = new DataTable();
            dt.Columns.Add("TpepPickupDatetime", typeof(DateTime));
            dt.Columns.Add("TpepDropoffDatetime", typeof(DateTime));
            dt.Columns.Add("PassengerCount", typeof(int));
            dt.Columns.Add("TripDistance", typeof(decimal));
            dt.Columns.Add("StoreAndFwdFlag", typeof(string));
            dt.Columns.Add("PULocationID", typeof(int));
            dt.Columns.Add("DOLocationID", typeof(int));
            dt.Columns.Add("FareAmount", typeof(decimal));
            dt.Columns.Add("TipAmount", typeof(decimal));
            return dt;
        }

        public static async Task<int> InsertBatchAsync(string connectionString, DataTable table)
        {
            if (table.Rows.Count == 0) return 0;
            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                using var bulkCopy = new SqlBulkCopy(connection)
                {
                    DestinationTableName = "dbo.TaxiTrips"
                };
                await bulkCopy.WriteToServerAsync(table);
                Console.WriteLine($"  Bulk insert completed. Inserted {table.Rows.Count} rows.");
                return table.Rows.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Bulk insert failed: {ex.Message}");
                return 0;
            }
        }
    }
}