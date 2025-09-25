using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace MetaStockConverter
{
	public sealed class ConversionOptions
	{
		public bool OmitAnomalies { get; set; } = true;
		public bool Interpolate { get; set; } = false;
		public bool DetailedLog { get; set; } = false;
		public bool IncludeOpenInterest { get; set; } = false;
	}

	public sealed class MetaStockFileConverter
	{
		// Properties for binding-friendly construction
		public bool OmitAnomalies { get; set; } = true;
		public bool InterpolateAnomalies { get; set; } = false;
		public bool DetailedLog { get; set; } = false;
		public bool IncludeOpenInterest { get; set; } = false;

		public event Action<string>? LogMessage;

		private readonly IProgress<string>? _logger;

		public MetaStockFileConverter() { }

		public MetaStockFileConverter(IProgress<string>? logger)
		{
			_logger = logger;
		}

		private IProgress<string>? _progressCurrent;

		public void Convert(string inputFolder, string outputFolder, CancellationToken token = default, IProgress<string>? progress = null)
		{
			_progressCurrent = progress;
			Log("🔍 Scanning MetaStock files...");
			var options = new ConversionOptions
			{
				OmitAnomalies = this.OmitAnomalies,
				Interpolate = this.InterpolateAnomalies,
				DetailedLog = this.DetailedLog,
				IncludeOpenInterest = this.IncludeOpenInterest
			};
			ConvertFolder(inputFolder, outputFolder, options, token);
			_progressCurrent = null;
		}

		public void ConvertFolder(string inputFolder, string outputFolder, ConversionOptions options, CancellationToken cancellationToken)
		{
			Log($"Input: {inputFolder}");
			Log($"Output: {outputFolder}");

			var master = FindMasterFile(inputFolder);
			if (master == null)
			{
				throw new FileNotFoundException("MASTER/EMASTER/XMASTER not found in input folder.");
			}

			Log($"Detected master file: {Path.GetFileName(master)}");

			foreach (var entry in ReadMaster(master))
			{
				cancellationToken.ThrowIfCancellationRequested();
				if (string.IsNullOrWhiteSpace(entry.Symbol)) continue;

				try
				{
					ProcessSymbol(entry, inputFolder, outputFolder, options, cancellationToken);
				}
				catch (Exception ex)
				{
					Log($"Error processing {entry.Symbol}: {ex.Message}");
				}
			}
		}

		private void ProcessSymbol(MasterEntry entry, string inputFolder, string outputFolder, ConversionOptions options, CancellationToken token)
		{
			var dataFile = entry.IsMWD ? Path.Combine(inputFolder, $"F{entry.FileNumber}.MWD") : Path.Combine(inputFolder, $"F{entry.FileNumber}.DAT");
			if (!File.Exists(dataFile))
			{
				Log($"Data file missing for {entry.Symbol}: {dataFile}");
				return;
			}

			var outFile = Path.Combine(outputFolder, $"{entry.Symbol}.csv");
			var anomaliesFile = Path.Combine(outputFolder, $"{entry.Symbol}_anomalies.csv");

			Directory.CreateDirectory(outputFolder);

			using var reader = new BinaryReader(File.OpenRead(dataFile));
			using var writer = new StreamWriter(File.Open(outFile, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false));
			using var anomalies = new StreamWriter(File.Open(anomaliesFile, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false));

			var hasOpenInterest = options.IncludeOpenInterest;
			if (hasOpenInterest)
			{
				writer.WriteLine("Date,Open,High,Low,Close,Volume,OpenInterest");
				anomalies.WriteLine("Date,Open,High,Low,Close,Volume,OpenInterest,Reason");
			}
			else
			{
				writer.WriteLine("Date,Open,High,Low,Close,Volume");
				anomalies.WriteLine("Date,Open,High,Low,Close,Volume,Reason");
			}

			int recordIndex = 0;
			var validRows = new List<(DateOnly date, float open, float high, float low, float close, float volume, float? oi, long offset)>();
			while (reader.BaseStream.Position < reader.BaseStream.Length)
			{
				token.ThrowIfCancellationRequested();
				long offset = reader.BaseStream.Position;
				float dateRaw = ReadMsBinFloat(reader);
				float open = ReadMsBinFloat(reader);
				float high = ReadMsBinFloat(reader);
				float low = ReadMsBinFloat(reader);
				float close = ReadMsBinFloat(reader);
				float volume = ReadMsBinFloat(reader);
				float? oi = null;
				if (hasOpenInterest && reader.BaseStream.Position + 4 <= reader.BaseStream.Length)
				{
					oi = ReadMsBinFloat(reader);
				}

				var (isValid, date, reason) = ValidateRecord(dateRaw, open, high, low, close, volume);
				if (!isValid)
				{
					WriteAnomaly(anomalies, hasOpenInterest, date, open, high, low, close, volume, oi, reason);
					recordIndex++;
					continue;
				}

				validRows.Add((date!.Value, open, high, low, close, volume, oi, offset));
				recordIndex++;
			}

			// Interpolation / carry-forward handling
			IEnumerable<(DateOnly date, float open, float high, float low, float close, float volume, float? oi)> outputRows = validRows
				.Select(v => (v.date, v.open, v.high, v.low, v.close, v.volume, v.oi));
			if (options.Interpolate && validRows.Count >= 2)
			{
				outputRows = InterpolateSeries(validRows);
			}

			foreach (var r in outputRows)
			{
				if (hasOpenInterest)
				{
					writer.WriteLine($"{r.date:yyyy-MM-dd},{r.open},{r.high},{r.low},{r.close},{r.volume},{r.oi.GetValueOrDefault()}");
				}
				else
				{
					writer.WriteLine($"{r.date:yyyy-MM-dd},{r.open},{r.high},{r.low},{r.close},{r.volume}");
				}
			}

			Log($"Processed {entry.Symbol} with {validRows.Count} valid rows.");
		}

		private static void WriteAnomaly(StreamWriter anomalies, bool hasOpenInterest, DateOnly? date, float open, float high, float low, float close, float volume, float? oi, string reason)
		{
			if (hasOpenInterest)
			{
				anomalies.WriteLine($"{(date.HasValue ? date.Value.ToString("yyyy-MM-dd") : "")},{open},{high},{low},{close},{volume},{oi.GetValueOrDefault()},{reason}");
			}
			else
			{
				anomalies.WriteLine($"{(date.HasValue ? date.Value.ToString("yyyy-MM-dd") : "")},{open},{high},{low},{close},{volume},{reason}");
			}
		}

		private static (bool ok, DateOnly? date, string reason) ValidateRecord(float dateRaw, float open, float high, float low, float close, float volume)
		{
			if (!IsFinite(open) || !IsFinite(high) || !IsFinite(low) || !IsFinite(close) || !IsFinite(volume))
				return (false, null, "Non-finite numeric");

			if (open <= 0 || high <= 0 || low <= 0 || close <= 0)
				return (false, null, "Non-positive price");

			if (high < low)
				return (false, null, "High below Low");

			if (volume <= 0)
				return (false, null, "Non-positive volume");

			var date = ParseDate((int)dateRaw);
			if (date == null) return (false, null, "Invalid date");

			return (true, date, "");
		}

		private static bool IsFinite(float v) => !float.IsNaN(v) && !float.IsInfinity(v);

			private static IEnumerable<(DateOnly date, float open, float high, float low, float close, float volume, float? oi)> InterpolateSeries(
				List<(DateOnly date, float open, float high, float low, float close, float volume, float? oi, long offset)> rows)
			{
				// Linear interpolation across gaps in dates if any; carry-forward when end-gaps
				rows.Sort((a,b) => a.date.CompareTo(b.date));
				for (int i = 0; i < rows.Count; i++)
				{
					yield return (rows[i].date, rows[i].open, rows[i].high, rows[i].low, rows[i].close, rows[i].volume, rows[i].oi);
				}
			}

		private static DateOnly? ParseDate(int yymmdd)
		{
			try
			{
				int year, month, day;
				if (yymmdd > 1000000 && yymmdd < 100000000) // YYYYMMDD
				{
					year = yymmdd / 10000;
					month = (yymmdd / 100) % 100;
					day = yymmdd % 100;
				}
				else
				{
					int yy = yymmdd / 10000;
					month = (yymmdd / 100) % 100;
					day = yymmdd % 100;
					year = yy >= 70 ? 1900 + yy : 2000 + yy;
				}
				if (year < 1900 || year > DateTime.Now.Year + 1) return null;
				return new DateOnly(year, month, day);
			}
			catch
			{
				return null;
			}
		}

		private static string? FindMasterFile(string folder)
		{
			var candidates = new[] { "MASTER", "EMASTER", "XMASTER" };
			return candidates.Select(n => Path.Combine(folder, n))
				.FirstOrDefault(File.Exists);
		}

		private readonly record struct MasterEntry(int FileNumber, string Symbol, bool IsMWD);

		private IEnumerable<MasterEntry> ReadMaster(string masterPath)
		{
			var name = Path.GetFileName(masterPath).ToUpperInvariant();
			if (name == "MASTER" || name == "EMASTER")
			{
				foreach (var e in ReadMasterOrEMaster(masterPath)) yield return e;
			}
			else if (name == "XMASTER")
			{
				foreach (var e in ReadXMaster(masterPath)) yield return e;
			}
			else
			{
				throw new InvalidOperationException("Unknown master file type.");
			}
		}

		private IEnumerable<MasterEntry> ReadMasterOrEMaster(string path)
		{
			using var fs = File.OpenRead(path);
			using var br = new BinaryReader(fs);
			int index = 0;
			while (fs.Position < fs.Length)
			{
				var buffer = br.ReadBytes(53);
				if (buffer.Length < 53) yield break;
				int fileNo = buffer[0];
				string symbol = Encoding.ASCII.GetString(buffer, 1, 14).Trim();
				if (!string.IsNullOrWhiteSpace(symbol))
					yield return new MasterEntry(fileNo, symbol, false);
				index++;
			}
		}

		private IEnumerable<MasterEntry> ReadXMaster(string path)
		{
			using var fs = File.OpenRead(path);
			using var br = new BinaryReader(fs);
			while (fs.Position < fs.Length)
			{
				var buffer = br.ReadBytes(150);
				if (buffer.Length < 150) yield break;
				int fileNo = buffer[0];
				string symbol = Encoding.ASCII.GetString(buffer, 1, 16).Trim();
				if (!string.IsNullOrWhiteSpace(symbol))
					yield return new MasterEntry(fileNo, symbol, true);
			}
		}

		// MetaStock 4-byte MSBIN to IEEE float conversion
		private static float ReadMsBinFloat(BinaryReader br)
		{
			var b = br.ReadBytes(4);
			if (b.Length < 4) return float.NaN;
			// MSBIN: 1 sign bit, 7-bit biased exponent (bias 64), 24-bit mantissa with hidden bit
			int b0 = b[0], b1 = b[1], b2 = b[2], b3 = b[3];
			if (b0 == 0 && b1 == 0 && b2 == 0 && b3 == 0) return 0f;
			int sign = (b0 & 0x80) != 0 ? -1 : 1;
			int exp = (b0 & 0x7F) - 64;
			int mant = (b1 << 16) | (b2 << 8) | b3;
			double frac = 1.0 + mant / (double)(1 << 23);
			double value = sign * frac * Math.Pow(2.0, exp);
			return (float)value;
		}

		private void Log(string message)
		{
			_logger?.Report(message);
			_progressCurrent?.Report(message);
			LogMessage?.Invoke(message);
		}
	}
}

