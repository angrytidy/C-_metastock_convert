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

				// Build quick index of present data files (case-insensitive, .DAT/.MWD)
				var presentData = Directory.EnumerateFiles(inputFolder, "F*.*", SearchOption.TopDirectoryOnly)
					.Where(p => {
						var name = Path.GetFileName(p);
						if (!name.StartsWith("F", StringComparison.OrdinalIgnoreCase)) return false;
						var ext = Path.GetExtension(name);
						return ext.Equals(".dat", StringComparison.OrdinalIgnoreCase) ||
							   ext.Equals(".mwd", StringComparison.OrdinalIgnoreCase);
					})
					.Select(p => (path: p,
							  num: int.TryParse(Path.GetFileNameWithoutExtension(p).Substring(1), out var n) ? n : -1,
							  isMWD: Path.GetExtension(p).Equals(".mwd", StringComparison.OrdinalIgnoreCase)))
					.Where(x => x.num >= 0)
					.ToDictionary(x => x.num, x => (x.path, x.isMWD), comparer: EqualityComparer<int>.Default);

				// Optional: show a completeness snapshot
				Log($"Pre-scan: MASTER listed symbols will be matched against {presentData.Count} F*.DAT/MWD files found in the folder.");

				// Add pre-run message when MASTER exists but only 1–2 data files are present
				if (presentData.Count <= 2)
					Log("ℹ Only a couple of F*.DAT/MWD files are present. That's why you'll see very few CSV outputs.");

				Directory.CreateDirectory(outputFolder);

				int totalSymbols = 0, filesFound = 0, rowsTotal = 0;
				var missing = new List<string>();

				foreach (var entry in ReadMaster(master))
				{
					cancellationToken.ThrowIfCancellationRequested();
					if (string.IsNullOrWhiteSpace(entry.Symbol)) continue;
					totalSymbols++;

					// Prefer what's truly present in the folder (handles .DAT vs .MWD and casing)
					if (presentData.TryGetValue(entry.FileNumber, out var present))
					{
						filesFound++;
						int rc = ProcessDataFile(present.path, entry.Symbol, outputFolder);
						if (rc > 0) rowsTotal += rc;
					}
					else
					{
						missing.Add($"{entry.Symbol} (F{entry.FileNumber})");
					}
				}

				// After loop:
				if (missing.Count > 0)
				{
					Log($"⚠ Missing data files for {missing.Count} symbols (showing up to 20):");
					foreach (var m in missing.Take(20)) Log($"   - {m}");
					if (missing.Count > 20) Log($"   ... {missing.Count - 20} more");
				}
				Log($"Summary: symbols={totalSymbols}, filesFound={filesFound}, totalRows={rowsTotal}");

				// Process orphan files not in MASTER
				bool processOrphans = true; // TODO: wire to an Options checkbox in the UI
				if (processOrphans)
				{
					// Build set of file numbers referenced by MASTER to avoid duplicates
					var referenced = new HashSet<int>(ReadMaster(master).Select(e => e.FileNumber));
					var orphans = presentData.Where(kv => !referenced.Contains(kv.Key)).ToList();

					if (orphans.Count > 0)
					{
						Log($"ℹ Processing {orphans.Count} orphan data files not listed in {Path.GetFileName(master)}...");
						foreach (var kv in orphans)
						{
							cancellationToken.ThrowIfCancellationRequested();
							var fileNum = kv.Key;
							var (path, isMWD) = kv.Value;
							var symbol = $"F{fileNum}"; // Or derive symbol via a custom mapping if you have one
							int rc = ProcessDataFile(path, symbol, outputFolder);
							if (rc <= 0) Log($"ℹ {symbol}: no valid rows; CSV not created.");
						}
					}
				}
			}
			finally
			{
				// Always close the log file
				CloseLogFile();
			}
		}

		private int ProcessDataFile(string datFilename, string symbol, string outputFolder)
		{
			try
			{
				string safeSymbol = SanitizeFileName(symbol);
				using var fs = new FileStream(datFilename, FileMode.Open, FileAccess.Read);
				using var br = new BinaryReader(fs);

				var sb = new StringBuilder();
				sb.AppendLine("Date,Open,High,Low,Close,Volume");

				int recordCount = 0, rowIndex = 0;

				while (fs.Position <= fs.Length - 24)
				{
					byte[] buffer = br.ReadBytes(24);
					if (buffer.Length != 24) break;

					float dateF  = ReadMetaStockFloat(buffer,  0, true);
					float open   = ReadMetaStockFloat(buffer,  4, false);
					float high   = ReadMetaStockFloat(buffer,  8, false);
					float low    = ReadMetaStockFloat(buffer, 12, false);
					float close  = ReadMetaStockFloat(buffer, 16, false);
					float volume = ReadMetaStockFloat(buffer, 20, false);

					int dateInt = (int)dateF;
					if (!IsPlausibleDateInt(dateInt))
					{
						if (DetailedLog) Log($"Anomaly {safeSymbol} row {rowIndex}: invalid date={dateInt}");
						rowIndex++;
						continue;
					}

					string dateStr = FormatDate(dateInt);
					sb.AppendLine($"{dateStr},{open:F4},{high:F4},{low:F4},{close:F4},{volume:F2}");
					if (recordCount == 0 && DetailedLog)
						Log($"Preview {safeSymbol} first row: {dateStr},{open:F4},{high:F4},{low:F4},{close:F4},{volume:F2}");

					recordCount++;
					rowIndex++;
				}

				if (recordCount > 0)
				{
					string csvFilename = Path.Combine(outputFolder, $"{safeSymbol}.csv");
					using var writer = new StreamWriter(csvFilename, false, Encoding.UTF8);
					writer.Write(sb.ToString());
					Log($"📈 {safeSymbol}: {recordCount} records → {safeSymbol}.csv");
				}
				else
				{
					Log($"ℹ {safeSymbol}: no valid rows; CSV not created.");
				}

				return recordCount;
			}
			catch (Exception ex)
			{
				Log($"❌ Error processing {symbol}: {ex.Message}");
				return -1;
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

		// Extension-insensitive auto-detect when not using presentData dictionary (fallback branch)
		static string? ProbeDataFile(string folder, int n)
		{
			var cands = new[]
			{
				Path.Combine(folder, $"F{n}.DAT"),
				Path.Combine(folder, $"F{n}.dat"),
				Path.Combine(folder, $"F{n}.MWD"),
				Path.Combine(folder, $"F{n}.mwd")
			};
			return cands.FirstOrDefault(File.Exists);
		}

		private readonly record struct MasterEntry(int FileNumber, string Symbol, bool IsMWD);

		private IEnumerable<MasterEntry> ReadMaster(string masterPath)
		{
			var name = Path.GetFileName(masterPath).ToUpperInvariant();
			return name switch
			{
				"MASTER" => ReadMasterFile_Master(masterPath),
				"EMASTER" => ReadEMaster(masterPath),
				"XMASTER" => ReadXMaster(masterPath),
				_ => throw new InvalidOperationException("Unknown master file type.")
			};
		}

		// === MASTER / EMASTER / XMASTER readers ===
		// MASTER: 53-byte header, then 53-byte records.
		// EMASTER: 192-byte header, then 192-byte records (version 0x36 0x36 at record[0..1]).
		// XMASTER: 150-byte header, then 150-byte records; file number is UInt16 at offset 65.

		private IEnumerable<MasterEntry> ReadMasterFile_Master(string path)
		{
			using var fs = File.OpenRead(path);
			using var br = new BinaryReader(fs);

			// Skip MASTER header (53 bytes)
			if (fs.Length < 53) yield break;
			fs.Seek(53, SeekOrigin.Begin);

			while (fs.Position <= fs.Length - 53)
			{
				var buffer = br.ReadBytes(53);
				if (buffer.Length != 53) yield break;

				int fileNumber = buffer[0]; // MASTER_FX = 0
				var (symbol, name) = ExtractMasterSymbolName_Master(buffer); // MASTER offsets
				if (string.IsNullOrWhiteSpace(symbol)) continue;

				// Debug first few
				if (fileNumber <= 5)
					Log($"MASTER map: F{fileNumber} → Symbol='{symbol}', Name='{name}'");

				yield return new MasterEntry(fileNumber, symbol, false);
			}
		}

		private IEnumerable<MasterEntry> ReadEMaster(string path)
		{
			using var fs = File.OpenRead(path);
			using var br = new BinaryReader(fs);

			// Skip EMASTER header (192 bytes)
			if (fs.Length < 192) yield break;
			fs.Seek(192, SeekOrigin.Begin);

			while (fs.Position <= fs.Length - 192)
			{
				var buffer = br.ReadBytes(192);
				if (buffer.Length != 192) yield break;

				// Version bytes
				if (!(buffer[0] == 0x36 && buffer[1] == 0x36)) continue;

				int fileNumber = buffer[2]; // EMASTER_FX = 2
				var (symbol, name) = ExtractMasterSymbolName_EMaster(buffer); // EMASTER offsets
				if (string.IsNullOrWhiteSpace(symbol)) continue;

				if (fileNumber <= 5)
					Log($"EMASTER map: F{fileNumber} → Symbol='{symbol}', Name='{name}'");

				yield return new MasterEntry(fileNumber, symbol, false);
			}
		}

		private IEnumerable<MasterEntry> ReadXMaster(string path)
		{
			using var fs = File.OpenRead(path);
			using var br = new BinaryReader(fs);

			// Skip XMASTER header (150 bytes)
			if (fs.Length < 150) yield break;
			fs.Seek(150, SeekOrigin.Begin);

			while (fs.Position <= fs.Length - 150)
			{
				var buffer = br.ReadBytes(150);
				if (buffer.Length != 150) yield break;

				// Record type check: first byte 0x01 (commonly)
				if (buffer[0] != 0x01) continue;

				// Symbol at offset 1 (len 14), name at 16 (len 45), file number UInt16 at 65
				string symbol = ReadString(buffer, 1, 14).Trim();
				int fileNumber = BitConverter.ToUInt16(buffer, 65); // XMASTER_FN = 65

				if (string.IsNullOrWhiteSpace(symbol)) continue;

				if (fileNumber <= 5)
					Log($"XMASTER map: F{fileNumber} → Symbol='{symbol}'");

				yield return new MasterEntry(fileNumber, symbol, true);
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

		// MASTER field offsets
		private (string symbol, string name) ExtractMasterSymbolName_Master(byte[] buffer)
		{
			// MASTER_SYMBOL = 36 (14 bytes), MASTER_NAME = 7 (16 bytes)
			string sym = ReadString(buffer, 36, 14).Trim();
			string name = ReadString(buffer, 7, 16).Trim();

			// Heuristic: if sym isn't ticker-ish, try swap
			if (!SymbolRegex.IsMatch(sym))
			{
				string s2 = ReadString(buffer, 7, 14).Trim();
				string n2 = ReadString(buffer, 36, 16).Trim();
				if (SymbolRegex.IsMatch(s2)) return (s2, n2);
			}
			return (sym, name);
		}

		// EMASTER field offsets
		private (string symbol, string name) ExtractMasterSymbolName_EMaster(byte[] buffer)
		{
			// EMASTER_SYMBOL = 11 (14 bytes), EMASTER_NAME = 32 (16 bytes)
			string sym = ReadString(buffer, 11, 14).Trim();
			string name = ReadString(buffer, 32, 16).Trim();

			if (!SymbolRegex.IsMatch(sym))
			{
				string s2 = ReadString(buffer, 32, 14).Trim();
				string n2 = ReadString(buffer, 11, 16).Trim();
				if (SymbolRegex.IsMatch(s2)) return (s2, n2);
			}
			return (sym, name);
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

