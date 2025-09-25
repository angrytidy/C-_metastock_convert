using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
		private StreamWriter? _logFileWriter;

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
			try
			{
				// Initialize log file
				InitializeLogFile(outputFolder);
				
				Log($"Input: {inputFolder}");
				Log($"Output: {outputFolder}");

				var master = FindMasterFile(inputFolder);
				if (master == null)
				{
					throw new FileNotFoundException("MASTER/EMASTER/XMASTER not found in input folder.");
				}

				Log($"Detected master file: {Path.GetFileName(master)}");

				Directory.CreateDirectory(outputFolder);

				foreach (var entry in ReadMaster(master))
				{
					cancellationToken.ThrowIfCancellationRequested();
					if (string.IsNullOrWhiteSpace(entry.Symbol)) continue;

					try
					{
						var dataFile = entry.IsMWD ? Path.Combine(inputFolder, $"F{entry.FileNumber}.MWD") : Path.Combine(inputFolder, $"F{entry.FileNumber}.DAT");
						if (!File.Exists(dataFile))
						{
							Log($"Data file missing for {entry.Symbol}: {dataFile}");
							continue;
						}

						ProcessDataFile(dataFile, entry.Symbol, outputFolder);
					}
					catch (Exception ex)
					{
						Log($"Error processing {entry.Symbol}: {ex.Message}");
					}
				}
			}
			finally
			{
				// Always close the log file
				CloseLogFile();
			}
		}

		private bool ProcessDataFile(string datFilename, string symbol, string outputFolder)
		{
			try
			{
				string safeSymbol = SanitizeFileName(symbol);
				string csvFilename = Path.Combine(outputFolder, $"{safeSymbol}.csv");

				using var fs = new FileStream(datFilename, FileMode.Open, FileAccess.Read);
				using var br = new BinaryReader(fs);
				using var writer = new StreamWriter(csvFilename, false, Encoding.UTF8);

				writer.WriteLine("Date,Open,High,Low,Close,Volume");

				int recordCount = 0;
				int rowIndex = 0;

				while (fs.Position <= fs.Length - 24)
				{
					byte[] buffer = br.ReadBytes(24);
					if (buffer.Length != 24) break;

					// Auto-detect for date; others can use MSBIN (will still be sane) or switch to IEEE if needed
					float dateF  = ReadMetaStockFloat(buffer,  0, isDateField: true);
					float open   = ReadMetaStockFloat(buffer,  4, isDateField: false);
					float high   = ReadMetaStockFloat(buffer,  8, isDateField: false);
					float low    = ReadMetaStockFloat(buffer, 12, isDateField: false);
					float close  = ReadMetaStockFloat(buffer, 16, isDateField: false);
					float volume = ReadMetaStockFloat(buffer, 20, isDateField: false);

					int dateInt = (int)dateF;
					if (!IsPlausibleDateInt(dateInt))
					{
						// anomaly – skip or collect based on your options
						if (DetailedLog) Log($"Data anomaly in {Path.GetFileNameWithoutExtension(datFilename)} row {rowIndex}: date={dateInt}");
						rowIndex++;
						continue;
					}

					string dateStr = FormatDate(dateInt);
					writer.WriteLine($"{dateStr},{open:F4},{high:F4},{low:F4},{close:F4},{volume:F2}");
					recordCount++;
					rowIndex++;
				}

				Log($"📈 {safeSymbol}: {recordCount} records → {safeSymbol}.csv");
				return recordCount > 0;
			}
			catch (Exception ex)
			{
				Log($"❌ Error processing {symbol}: {ex.Message}");
				return false;
			}
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
				int fileNumber = buffer[0];
				var (symbol, name) = ExtractMasterSymbolName(buffer);
				if (string.IsNullOrWhiteSpace(symbol)) continue;

				// optional: quick debug for first few items
				if (fileNumber <= 5)
					Log($"MASTER map: F{fileNumber} → Symbol='{symbol}', Name='{name}'");

				yield return new MasterEntry(fileNumber, symbol, false);
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

		// Accepts typical ticker chars
		private static readonly Regex SymbolRegex =
			new(@"^[A-Za-z0-9\.\-\$]{1,15}$", RegexOptions.Compiled);

		// file-name safe ticker
		private static string SanitizeFileName(string s)
		{
			if (string.IsNullOrWhiteSpace(s)) return "UNKNOWN";
			var invalid = Path.GetInvalidFileNameChars();
			var cleaned = new string(s.Select(ch => invalid.Contains(ch) ? '_' : ch).ToArray());
			cleaned = cleaned.Trim().TrimEnd('.');  // Windows cannot end with '.'
			return string.IsNullOrEmpty(cleaned) ? "UNKNOWN" : cleaned;
		}

		// MASTER symbol/name extractor with fallback for swapped fields
		private (string symbol, string name) ExtractMasterSymbolName(byte[] buffer)
		{
			const int MASTER_SYMBOL = 36; // 14 bytes
			const int MASTER_NAME   = 7;  // 16 bytes

			var s1 = ReadString(buffer, MASTER_SYMBOL, 14).Trim();
			var n1 = ReadString(buffer, MASTER_NAME,   16).Trim();

			if (SymbolRegex.IsMatch(s1))
				return (s1, n1);

			// try swapped interpretation (some datasets differ)
			var s2 = ReadString(buffer, MASTER_NAME,   14).Trim();
			var n2 = ReadString(buffer, MASTER_SYMBOL, 16).Trim();
			if (SymbolRegex.IsMatch(s2))
				return (s2, n2);

			// last resort: sanitize whatever is most likely a usable key
			var fallback = !string.IsNullOrEmpty(s1) ? s1 : n1;
			return (SanitizeFileName(fallback), n1.Length > 0 ? n1 : n2);
		}

		private static string ReadString(byte[] buffer, int offset, int length)
		{
			if (offset + length > buffer.Length) return "";
			return Encoding.ASCII.GetString(buffer, offset, length);
		}

		private static bool IsPlausibleDateInt(int v)
		{
			// YYYYMMDD
			if (v >= 19000101 && v <= 20991231) return true;
			// YYMMDD
			if (v >= 500101 && v <= 991231) return true;
			if (v >= 000101 && v <= 491231) return true; // assume 2000-2049 for 00-49
			return false;
		}

		private static float ReadMsbin(byte[] buf, int off)
		{
			if (off + 4 > buf.Length) return 0f;
			byte[] msbin = new byte[4];
			Array.Copy(buf, off, msbin, 0, 4);
			if (msbin[3] == 0) return 0f;

			byte[] ieee = new byte[4];
			byte sign = (byte)(msbin[2] & 0x80);
			byte ieeeExp = (byte)(msbin[3] - 2);
			ieee[3] = (byte)((sign) | (ieeeExp >> 1));
			ieee[2] = (byte)(((ieeeExp << 7) & 0x80) | (msbin[2] & 0x7F));
			ieee[1] = msbin[1];
			ieee[0] = msbin[0];
			return BitConverter.ToSingle(ieee, 0);
		}

		// Auto-detect: for the *date* field, try MSBIN first, then IEEE if implausible.
		private static float ReadMetaStockFloat(byte[] buf, int off, bool isDateField)
		{
			float vMsbin = ReadMsbin(buf, off);
			if (!isDateField)
			{
				if (float.IsFinite(vMsbin)) return vMsbin;
				return BitConverter.ToSingle(buf, off);
			}

			int d = (int)vMsbin;
			if (IsPlausibleDateInt(d)) return vMsbin;

			float vIeee = BitConverter.ToSingle(buf, off);
			int d2 = (int)vIeee;
			if (IsPlausibleDateInt(d2)) return vIeee;

			// Fallback to MSBIN if neither looks plausible — caller may skip
			return vMsbin;
		}

		private static string FormatDate(int dateInt)
		{
			try
			{
				int year, month, day;
				if (dateInt > 1000000 && dateInt < 100000000) // YYYYMMDD
				{
					year = dateInt / 10000;
					month = (dateInt / 100) % 100;
					day = dateInt % 100;
				}
				else
				{
					int yy = dateInt / 10000;
					month = (dateInt / 100) % 100;
					day = dateInt % 100;
					year = yy >= 70 ? 1900 + yy : 2000 + yy;
				}
				return $"{year:D4}-{month:D2}-{day:D2}";
			}
			catch
			{
				return "0000-00-00";
			}
		}

		private void InitializeLogFile(string outputFolder)
		{
			try
			{
				var logDirectory = Path.Combine(outputFolder, "log");
				Directory.CreateDirectory(logDirectory);
				
				var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
				var logFilePath = Path.Combine(logDirectory, $"MetaStockConverter_{timestamp}.txt");
				
				_logFileWriter = new StreamWriter(logFilePath, false, Encoding.UTF8);
				_logFileWriter.WriteLine($"MetaStock Converter Log - {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
				_logFileWriter.WriteLine(new string('=', 50));
				_logFileWriter.Flush();
			}
			catch (Exception ex)
			{
				// If we can't create log file, continue without file logging
				_logger?.Report($"Warning: Could not create log file: {ex.Message}");
			}
		}

		private void CloseLogFile()
		{
			try
			{
				if (_logFileWriter != null)
				{
					_logFileWriter.WriteLine(new string('=', 50));
					_logFileWriter.WriteLine($"Log completed at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
					_logFileWriter.Close();
					_logFileWriter.Dispose();
					_logFileWriter = null;
				}
			}
			catch
			{
				// Ignore errors when closing log file
			}
		}

		private void Log(string message)
		{
			var timestampedMessage = $"{DateTime.Now:HH:mm:ss}: {message}";
			
			_logger?.Report(timestampedMessage);
			_progressCurrent?.Report(timestampedMessage);
			LogMessage?.Invoke(timestampedMessage);
			
			// Write to log file
			try
			{
				_logFileWriter?.WriteLine(timestampedMessage);
				_logFileWriter?.Flush();
			}
			catch
			{
				// If file logging fails, continue without it
			}
		}
	}
}

