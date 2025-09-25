using System;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;

namespace MetaStockConverter
{
	public partial class MainWindow : Window, INotifyPropertyChanged
	{
		public event PropertyChangedEventHandler? PropertyChanged;

		private string _logText = "Ready to convert MetaStock files...\n";
		public string LogText
		{
			get => _logText;
			set { _logText = value; PropertyChanged?.Invoke(this, new(nameof(LogText))); }
		}

		private bool _isConverting;
		public bool IsConverting
		{
			get => _isConverting;
			set { _isConverting = value; PropertyChanged?.Invoke(this, new(nameof(IsConverting))); }
		}

		private CancellationTokenSource? _cts;

		public MainWindow()
		{
			InitializeComponent();
			DataContext = this;
		}

		private void AppendLog(string msg)
		{
			Dispatcher.Invoke(() => { LogText += $"{DateTime.Now:HH:mm:ss}: {msg}\n"; });
		}

		private void BrowseInput_Click(object sender, RoutedEventArgs e)
		{
			var dlg = new System.Windows.Forms.FolderBrowserDialog
			{ Description = "Select MetaStock Data Folder", ShowNewFolderButton = false };
			if (dlg.ShowDialog() == System.Windows.Forms.DialogResult.OK)
			{
				InputFolderText.Text = dlg.SelectedPath;
				AppendLog($"Selected input folder: {dlg.SelectedPath}");
			}
		}

		private void BrowseOutput_Click(object sender, RoutedEventArgs e)
		{
			var dlg = new System.Windows.Forms.FolderBrowserDialog
			{ Description = "Select Output Folder for CSV files", ShowNewFolderButton = true };
			if (dlg.ShowDialog() == System.Windows.Forms.DialogResult.OK)
			{
				OutputFolderText.Text = dlg.SelectedPath;
				AppendLog($"Selected output folder: {dlg.SelectedPath}");
			}
		}

		private async void Convert_Click(object sender, RoutedEventArgs e)
		{
			var input = InputFolderText.Text?.Trim();
			var output = OutputFolderText.Text?.Trim();

			if (string.IsNullOrWhiteSpace(input) || string.IsNullOrWhiteSpace(output))
			{
				MessageBox.Show("Please select both input and output folders.", "Missing Folders",
					MessageBoxButton.OK, MessageBoxImage.Warning);
				return;
			}
			if (!Directory.Exists(input))
			{
				MessageBox.Show("Input folder does not exist.", "Invalid Input Folder",
					MessageBoxButton.OK, MessageBoxImage.Error);
				return;
			}
			if (!Directory.Exists(output))
			{
				try { Directory.CreateDirectory(output); }
				catch (Exception ex)
				{
					MessageBox.Show($"Could not create output folder: {ex.Message}", "Folder Creation Error",
						MessageBoxButton.OK, MessageBoxImage.Error);
					return;
				}
			}

			bool omitAnomalies = OmitAnomaliesCheck.IsChecked == true;
			bool interpolate = InterpolateCheck.IsChecked == true;
			bool detailedLog = DetailedLogCheck.IsChecked == true;
			bool includeOI = IncludeOpenInterestCheck.IsChecked == true;

			_cts = new CancellationTokenSource();
			IsConverting = true;
			Progress.Visibility = Visibility.Visible;
			LogText = "Starting conversion...\n";

			try
			{
				var conv = new MetaStockFileConverter
				{
					OmitAnomalies = omitAnomalies,
					InterpolateAnomalies = interpolate,
					DetailedLog = detailedLog,
					IncludeOpenInterest = includeOI
				};
				conv.LogMessage += AppendLog;

				await Task.Run(() => conv.Convert(input!, output!, _cts.Token));
				MessageBox.Show("Conversion completed successfully!", "Success",
					MessageBoxButton.OK, MessageBoxImage.Information);
			}
			catch (OperationCanceledException)
			{
				AppendLog("Conversion canceled.");
			}
			catch (Exception ex)
			{
				AppendLog($"Error during conversion: {ex.Message}");
				MessageBox.Show($"Error during conversion: {ex.Message}", "Conversion Error",
					MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				IsConverting = false;
				Progress.Visibility = Visibility.Collapsed;
				_cts?.Dispose();
				_cts = null;
			}
		}

		private void Cancel_Click(object sender, RoutedEventArgs e) => _cts?.Cancel();

		private void Exit_Click(object sender, RoutedEventArgs e) => Close();
	}
}

