package de.soderer.dbimport.dataprovider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.soderer.dbimport.DbImportException;
import de.soderer.dbimport.utilities.DateUtilities;
import de.soderer.dbimport.utilities.Tuple;
import de.soderer.dbimport.utilities.Utilities;
import de.soderer.dbimport.utilities.csv.CsvDataException;
import de.soderer.dbimport.utilities.csv.CsvFormat;
import de.soderer.dbimport.utilities.csv.CsvReader;
import de.soderer.dbimport.utilities.csv.CsvWriter;
import de.soderer.dbimport.utilities.db.DbColumnType;
import de.soderer.dbimport.utilities.zip.Zip4jUtilities;
import de.soderer.dbimport.utilities.zip.ZipUtilities;

public class CsvDataProvider extends DataProvider {
	// Default optional parameters
	private char separator = ';';
	private Character stringQuote = '"';
	private char escapeStringQuote = '"';
	private String nullValueText = null;
	private boolean allowUnderfilledLines = false;
	private boolean removeSurplusEmptyTrailingColumns = false;
	private boolean noHeaders = false;
	private boolean trimData = false;

	private CsvReader csvReader = null;
	private List<String> columnNames = null;
	private Map<String, DbColumnType> dataTypes = null;
	private Long itemsAmount = null;
	private String itemsUnitSign = null;

	private final boolean isInlineData;
	private final String importFilePathOrData;
	private final char[] zipPassword;

	private final Charset encoding = StandardCharsets.UTF_8;

	public CsvDataProvider(final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword, final char separator, final Character stringQuote, final char escapeStringQuote, final boolean allowUnderfilledLines, final boolean removeSurplusEmptyTrailingColumns, final boolean noHeaders, final String nullValueText, final boolean trimData) {
		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
		this.separator = separator;
		this.stringQuote = stringQuote;
		this.escapeStringQuote = escapeStringQuote;
		this.allowUnderfilledLines = allowUnderfilledLines;
		this.removeSurplusEmptyTrailingColumns = removeSurplusEmptyTrailingColumns;
		this.noHeaders = noHeaders;
		this.nullValueText = nullValueText;
		this.trimData = trimData;
	}

	@Override
	public String getConfigurationLogString() {
		String dataPart;
		if (isInlineData) {
			dataPart = "Data: " + importFilePathOrData + "\n";
		} else {
			dataPart = "File: " + importFilePathOrData + "\n"
					+ "Zip: " + Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") + "\n";
		}
		return
				dataPart
				+ "Format: CSV" + "\n"
				+ "Encoding: " + encoding + "\n"
				+ "Separator: " + separator + "\n"
				+ "StringQuote: " + stringQuote + "\n"
				+ "EscapeStringQuote: " + escapeStringQuote + "\n"
				+ "AllowUnderfilledLines: " + allowUnderfilledLines + "\n"
				+ "RemoveSurplusEmptyTrailingColumns: " + removeSurplusEmptyTrailingColumns + "\n"
				+ "TrimData: " + trimData + "\n"
				+ "Null value text: " + (nullValueText == null ? "none" : "\"" + nullValueText + "\"") + "\n";
	}

	@Override
	public Map<String, DbColumnType> scanDataPropertyTypes(final Map<String, Tuple<String, String>> mapping) throws Exception {
		if (dataTypes == null) {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(trimData);

			try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
				if (!noHeaders) {
					// Read headers from file
					columnNames = scanCsvReader.readNextCsvLine();
				}

				dataTypes = new HashMap<>();

				// Scan all data for maximum
				List<String> values;
				while ((values = scanCsvReader.readNextCsvLine()) != null) {
					for (int i = 0; i < values.size(); i++) {
						final String columnName = (columnNames == null || columnNames.size() <= i ? "column_" + Integer.toString(i + 1) : columnNames.get(i));
						final String currentValue = values.get(i);
						detectNextDataType(mapping, dataTypes, columnName, currentValue);
					}
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return dataTypes;
	}

	@Override
	public List<String> getAvailableDataPropertyNames() throws Exception {
		if (columnNames == null) {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(true);

			try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
				if (noHeaders) {
					final List<String> returnList = new ArrayList<>();
					if (allowUnderfilledLines) {
						// Scan all data for maximum
						List<String> values;
						int maxColumns = 0;
						while ((values = scanCsvReader.readNextCsvLine()) != null) {
							maxColumns = Math.max(maxColumns, values.size());
						}
						for (int i = 0; i < maxColumns; i++) {
							returnList.add(Integer.toString(i + 1));
						}
						columnNames = returnList;
					} else {
						// Only take first data as example for all other data
						final List<String> values = scanCsvReader.readNextCsvLine();
						for (int i = 0; i < values.size(); i++) {
							returnList.add("column_" + Integer.toString(i + 1));
						}
						columnNames = returnList;
					}
				} else {
					// Read headers from file
					columnNames = scanCsvReader.readNextCsvLine();
				}
			} catch (final Exception e) {
				throw e;
			}
		}

		return columnNames;
	}

	@Override
	public long getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			final long dataSize;
			if (isInlineData) {
				dataSize = importFilePathOrData.length();
			} else if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || ZipUtilities.isZipArchiveFile(new File(importFilePathOrData))) {
				dataSize = ZipUtilities.getDataSizeUncompressed(new File(importFilePathOrData));
			} else {
				dataSize = new File(importFilePathOrData).length();
			}

			if (dataSize < 1024 * 1024 * 1024) {
				final CsvFormat csvFormat = new CsvFormat()
						.setSeparator(separator)
						.setStringQuote(stringQuote)
						.setStringQuoteEscapeCharacter(escapeStringQuote)
						.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
						.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns);

				try (CsvReader scanCsvReader = new CsvReader(getInputStream(), encoding, csvFormat)) {
					if (noHeaders) {
						itemsAmount = Long.valueOf(scanCsvReader.getCsvLineCount());
					} else {
						itemsAmount = Long.valueOf(scanCsvReader.getCsvLineCount() - 1);
					}
				} catch (final CsvDataException e) {
					throw new DbImportException(e.getMessage(), e);
				} catch (final Exception e) {
					throw e;
				}
			} else {
				itemsAmount = dataSize;
				itemsUnitSign = "B";
			}
		}

		return itemsAmount;
	}

	@Override
	public String getItemsUnitSign() throws Exception {
		return itemsUnitSign;
	}

	@Override
	public Map<String, Object> getNextItemData() throws Exception {
		if (csvReader == null) {
			openReader();
		}

		final List<String> values = csvReader.readNextCsvLine();
		if (values != null) {
			final Map<String, Object> returnMap = new HashMap<>();
			for (int i = 0; i < getAvailableDataPropertyNames().size(); i++) {
				final String columnName = getAvailableDataPropertyNames().get(i);
				if (values.size() > i) {
					if (nullValueText != null && nullValueText.equals(values.get(i))) {
						returnMap.put(columnName, null);
					} else {
						returnMap.put(columnName, values.get(i));
					}
				} else {
					returnMap.put(columnName, null);
				}
			}
			return returnMap;
		} else {
			return null;
		}
	}

	@Override
	public void close() {
		Utilities.closeQuietly(csvReader);
		csvReader = null;
	}

	@Override
	public File filterDataItems(final List<Integer> indexList, final String fileSuffix) throws Exception {
		OutputStream outputStream = null;
		try {
			openReader();

			File filteredDataFile;
			if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip")) {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".csv.zip");
				outputStream = ZipUtilities.openNewZipOutputStream(filteredDataFile, null);
				((ZipOutputStream) outputStream).putNextEntry(new ZipEntry(new File(importFilePathOrData + "." + fileSuffix + ".csv").getName()));
			} else {
				filteredDataFile = new File(importFilePathOrData + "." + fileSuffix + ".csv");
				outputStream = new FileOutputStream(filteredDataFile);
			}

			try (CsvWriter csvWriter = new CsvWriter(outputStream, encoding, new CsvFormat().setSeparator(separator).setStringQuote(stringQuote).setStringQuoteEscapeCharacter(escapeStringQuote))) {

				csvWriter.writeValues(columnNames);

				Map<String, Object> item;
				int itemIndex = 0;
				while ((item = getNextItemData()) != null) {
					itemIndex++;
					if (indexList.contains(itemIndex)) {
						final List<String> values = new ArrayList<>();
						for (final String columnName : columnNames) {
							if (item.get(columnName) == null) {
								values.add(nullValueText);
							} else if (item.get(columnName) instanceof String) {
								values.add((String) item.get(columnName));
							} else if (item.get(columnName) instanceof Date) {
								values.add(DateUtilities.formatDate(DateUtilities.YYYY_MM_DD_HHMMSS, (Date) item.get(columnName)));
							} else if (item.get(columnName) instanceof Number) {
								values.add(item.get(columnName).toString());
							} else {
								values.add(item.get(columnName).toString());
							}
						}
						csvWriter.writeValues(values);
					}
				}

				return filteredDataFile;
			}
		} finally {
			close();
			Utilities.closeQuietly(outputStream);
		}
	}

	@Override
	public long getImportDataAmount() {
		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				return 0;
			} else if (new File(importFilePathOrData).isDirectory()) {
				return 0;
			} else if (new File(importFilePathOrData).length() == 0) {
				return 0;
			}

			try {
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || ZipUtilities.isZipArchiveFile(new File(importFilePathOrData))) {
					if (zipPassword != null)  {
						return Zip4jUtilities.getUncompressedSize(new File(importFilePathOrData), zipPassword);
					} else {
						return ZipUtilities.getDataSizeUncompressed(new File(importFilePathOrData));
					}
				} else {
					return new File(importFilePathOrData).length();
				}
			} catch (@SuppressWarnings("unused") final IOException e) {
				return 0;
			}
		} else {
			return importFilePathOrData.length();
		}
	}

	private InputStream getInputStream() throws Exception {
		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).length() == 0) {
				throw new DbImportException("Import file is empty: " + importFilePathOrData);
			}

			InputStream inputStream = null;
			try {
				if (Utilities.endsWithIgnoreCase(importFilePathOrData, ".zip") || ZipUtilities.isZipArchiveFile(new File(importFilePathOrData))) {
					if (zipPassword != null)  {
						inputStream = Zip4jUtilities.openPasswordSecuredZipFile(importFilePathOrData, zipPassword);
					} else {
						final List<String> filepathsFromZipArchiveFile = ZipUtilities.getZipFileEntries(new File(importFilePathOrData));
						if (filepathsFromZipArchiveFile.size() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (filepathsFromZipArchiveFile.size() > 1) {
							throw new DbImportException("Zipped import file contains more than one file: " + importFilePathOrData);
						}

						inputStream = new ZipInputStream(new FileInputStream(new File(importFilePathOrData)));
						final ZipEntry zipEntry = ((ZipInputStream) inputStream).getNextEntry();
						if (zipEntry == null) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData);
						} else if (zipEntry.getSize() == 0) {
							throw new DbImportException("Zipped import file is empty: " + importFilePathOrData + ": " + zipEntry.getName());
						}
					}
				} else {
					inputStream = new FileInputStream(new File(importFilePathOrData));
				}
				return inputStream;
			} catch (final Exception e) {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (@SuppressWarnings("unused") final IOException e1) {
						// do nothing
					}
				}
				throw e;
			}
		} else {
			return new ByteArrayInputStream(importFilePathOrData.getBytes(StandardCharsets.UTF_8));
		}
	}

	private void openReader() throws Exception {
		if (csvReader != null) {
			throw new Exception("Reader was already opened before");
		}

		try {
			final CsvFormat csvFormat = new CsvFormat()
					.setSeparator(separator)
					.setStringQuote(stringQuote)
					.setStringQuoteEscapeCharacter(escapeStringQuote)
					.setFillMissingTrailingColumnsWithNull(allowUnderfilledLines)
					.setRemoveSurplusEmptyTrailingColumns(removeSurplusEmptyTrailingColumns)
					.setAlwaysTrim(trimData);

			csvReader = new CsvReader(getInputStream(), encoding, csvFormat);

			if (!noHeaders) {
				// Skip headers
				csvReader.readNextCsvLine();
			}
		} catch (final Exception e) {
			Utilities.closeQuietly(csvReader);
			throw e;
		}
	}

	@Override
	public long getReadDataSize() {
		if (csvReader != null) {
			return csvReader.getReadDataSize();
		} else {
			return 0;
		}
	}
}
