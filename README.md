## MetaStock to CSV Converter

Windows WPF tool that converts MetaStock data (MASTER/EMASTER/XMASTER + F*.DAT/MWD) to CSV.

### Build & Publish

```bash
dotnet restore
dotnet build -c Release
dotnet publish -c Release -r win-x64
```

Or run PowerShell script:

```bash
pwsh ./publish.ps1
```

Output: `./bin/Release/net8.0-windows/win-x64/publish/MetaStockConverter.exe`

### Usage

1. Select input folder containing master file and `F*.DAT`/`F*.MWD`.
2. Select output folder (defaults to `CSV` inside input).
3. Choose options and click `Convert to CSV`.

Anomalies are written to per-symbol `*_anomalies.csv`.

### Notes

- Default behavior omits anomalies. Interpolation placeholder exists; full interpolation can be extended.
- App is self-contained single-file .NET 8, win-x64.

