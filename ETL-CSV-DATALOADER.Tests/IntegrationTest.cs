using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Configurations;

namespace ETL_CSV_DATALOADER.IntegrationTests
{
    [TestClass]
    public class IntegrationTests
    {
        private IContainer _container;
        private string _connectionString;

        [TestInitialize]
        public async Task SetupAsync()
        {
            _container = new ContainerBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2019-latest")
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithEnvironment("SA_PASSWORD", "Your_Str0ng_Password1234")
                .WithPortBinding(1433, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(1433))
                .Build();

            await _container.StartAsync();

            var mappedPort = _container.GetMappedPublicPort(1433);
            _connectionString = $"Server=127.0.0.1,{mappedPort};User=SA;Password=Your_Str0ng_Password1234;TrustServerCertificate=True;";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var createSql = @"
                IF OBJECT_ID('dbo.TaxiTrips', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.TaxiTrips;
                END;
                CREATE TABLE dbo.TaxiTrips
                (
                    TpepPickupDatetime DATETIME2 NOT NULL,
                    TpepDropoffDatetime DATETIME2 NOT NULL,
                    PassengerCount TINYINT NOT NULL,
                    TripDistance DECIMAL(5,2) NOT NULL,
                    StoreAndFwdFlag CHAR(3) NOT NULL,
                    PULocationID INT NOT NULL,
                    DOLocationID INT NOT NULL,
                    FareAmount DECIMAL(10,2) NOT NULL,
                    TipAmount DECIMAL(10,2) NOT NULL,
                    TripDurationMinutes AS DATEDIFF(MINUTE, TpepPickupDatetime, TpepDropoffDatetime) PERSISTED
                );
            ";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = createSql;
            await cmd.ExecuteNonQueryAsync();
        }

        [TestCleanup]
        public async Task TeardownAsync()
        {
            if (_container != null)
            {
                await _container.StopAsync();
                await _container.DisposeAsync();
            }
        }

        [TestMethod]
        public async Task IntegrationTest()
        {
            Environment.SetEnvironmentVariable("CONNECTION_STRING", _connectionString);

            await TestValidRows();

            await ClearTableAsync();
            await TestDuplicates();

            await ClearTableAsync();
            await TestInvalidRows();
        }

        private async Task TestValidRows()
        {
            var csvPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "sample.csv");
            var invalidCsv = Path.Combine(Directory.GetCurrentDirectory(), "Data", "invalid_rows.csv");
            var duplicatesCsv = Path.Combine(Directory.GetCurrentDirectory(), "Data", "duplicates.csv");

            if (File.Exists(invalidCsv)) File.Delete(invalidCsv);
            if (File.Exists(duplicatesCsv)) File.Delete(duplicatesCsv);

            await Program.Main(new[] { csvPath });

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM dbo.TaxiTrips";
            var count = (int)await cmd.ExecuteScalarAsync();

            Assert.AreEqual(2, count);

            Assert.IsFalse(File.Exists(invalidCsv), "Must be without invalid rows");
            Assert.IsFalse(File.Exists(duplicatesCsv), "Must be without duplicates");
        }

        private async Task TestDuplicates()
        {
            var csvPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "sample_duplicates.csv");
            var duplicatesCsv = Path.Combine(Directory.GetCurrentDirectory(), "Data", "duplicates.csv");

            if (File.Exists(duplicatesCsv)) File.Delete(duplicatesCsv);

            await Program.Main(new[] { csvPath });

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM dbo.TaxiTrips";
            var rowCount = (int)await cmd.ExecuteScalarAsync();

            Assert.AreEqual(2, rowCount);

            Assert.IsTrue(File.Exists(duplicatesCsv));
            var dupLines = File.ReadAllLines(duplicatesCsv);
            Assert.AreEqual(2, dupLines.Length, "1 duplicate + header");
        }

        private async Task TestInvalidRows()
        {
            var csvPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "sample_invalid.csv");
            var invalidCsv = Path.Combine(Directory.GetCurrentDirectory(), "Data", "invalid_rows.csv");

            if (File.Exists(invalidCsv)) File.Delete(invalidCsv);

            await Program.Main(new[] { csvPath });

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM dbo.TaxiTrips";
            var insertedCount = (int)await cmd.ExecuteScalarAsync();

            Assert.AreEqual(2, insertedCount);

            Assert.IsTrue(File.Exists(invalidCsv));
            var invalidLines = File.ReadAllLines(invalidCsv);
            Assert.AreEqual(2, invalidLines.Length, "Must be 1 incorrect line + header");
        }

        private async Task ClearTableAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "TRUNCATE TABLE dbo.TaxiTrips";
            await cmd.ExecuteNonQueryAsync();
        }
    }
}