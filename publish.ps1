$ErrorActionPreference = "Stop"
dotnet publish -c Release -r win-x64 `
  -p:SelfContained=true `
  -p:PublishSingleFile=true `
  -p:IncludeNativeLibrariesForSelfExtract=true `
  -p:EnableCompressionInSingleFile=true `
  -p:PublishReadyToRun=true
Write-Host "Published to ./bin/Release/net8.0-windows/win-x64/publish/MetaStockConverter.exe"

