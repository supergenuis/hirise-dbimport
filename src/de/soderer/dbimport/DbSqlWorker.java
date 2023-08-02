package de.soderer.dbimport;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.soderer.dbimport.utilities.*;
import de.soderer.dbimport.utilities.csv.CsvDataException;
import de.soderer.dbimport.utilities.db.DbUtilities;
import de.soderer.dbimport.utilities.worker.WorkerParentSimple;
import de.soderer.dbimport.utilities.zip.Zip4jUtilities;
import de.soderer.dbimport.utilities.zip.ZipUtilities;

public class DbSqlWorker extends DbImportWorker {
	private SqlScriptReader sqlScriptReader = null;
	private Integer itemsAmount = null;

	private final boolean isInlineData;
	private final String importFilePathOrData;
	private final char[] zipPassword;

	private final Charset encoding = StandardCharsets.UTF_8;

	/**
	 * modify by huangzh@20230707
	 * Param: final DbImportDefinition dbDefinition -> final DbDefinition dbDefinition
	 */

	public DbSqlWorker(final WorkerParentSimple parent, final DbImportDefinition dbDefinition, final String tableName, final boolean isInlineData, final String importFilePathOrData, final char[] zipPassword) throws Exception {
		super(parent, dbDefinition, tableName, null, null);

		this.isInlineData = isInlineData;
		this.importFilePathOrData = importFilePathOrData;
		this.zipPassword = zipPassword;
		//add by huangzh@20230707
//		System.out.println("isLog:"+ dbDefinition.isLog());
//		System.out.println("isInlineData:"+ isInlineData);
		if (dbDefinition.isLog() && !isInlineData) {
			final File logDir = new File(new File(importFilePathOrData).getParentFile(), "importlogs");
			if (!logDir.exists()) {
				logDir.mkdir();
			}
			String fileName = new File(importFilePathOrData).getName() + "." + DateUtilities.formatDate(DateUtilities.YYYYMMDDHHMMSS, LocalDateTime.now()) + ".import.log";
			File logFile = new File(logDir, fileName);
			this.setLogFile(logFile);
			System.out.println("logDir path:"+ logFile.getAbsolutePath()+"");
		}
		this.setAnalyseDataOnly(analyseDataOnly);
		this.setTextFileEncoding(dbDefinition.getEncoding());
		this.setMapping(dbDefinition.getMapping());
		this.setImportMode(dbDefinition.getImportMode());
		this.setDuplicateMode(dbDefinition.getDuplicateMode());
		this.setKeycolumns(dbDefinition.getKeycolumns());
		this.setCompleteCommit(dbDefinition.isCompleteCommit());
		this.setCreateNewIndexIfNeeded(dbDefinition.isCreateNewIndexIfNeeded());
		this.setDeactivateForeignKeyConstraints(dbDefinition.isDeactivateForeignKeyConstraints());
		this.setDeactivateTriggers(dbDefinition.isDeactivateTriggers());
		this.setAdditionalInsertValues(dbDefinition.getAdditionalInsertValues());
		this.setAdditionalUpdateValues(dbDefinition.getAdditionalUpdateValues());
		this.setUpdateNullData(dbDefinition.isUpdateNullData());
		this.setCreateTableIfNotExists(dbDefinition.isCreateTable());
		this.setStructureFilePath(dbDefinition.getStructureFilePath());
		this.setLogErroneousData(dbDefinition.isLogErroneousData());
		this.setDatabaseTimeZone(dbDefinition.getDatabaseTimeZone());
		this.setImportDataTimeZone(dbDefinition.getImportDataTimeZone());

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
				+ "Format: SQL" + "\n"
				+ "Encoding: " + encoding + "\n";
	}

	public int getItemsAmountToImport() throws Exception {
		if (itemsAmount == null) {
			try (SqlScriptReader scanSqlScriptReader = new SqlScriptReader(getInputStream(), encoding)) {
				int statementsCount = 0;
				while (scanSqlScriptReader.readNextStatement() != null) {
					statementsCount++;
				}
				itemsAmount = statementsCount;
			} catch (final CsvDataException e) {
				throw new DbImportException(e.getMessage(), e);
			} catch (final Exception e) {
				throw e;
			}
		}
		return itemsAmount;
	}

	public void close() {
		Utilities.closeQuietly(sqlScriptReader);
		sqlScriptReader = null;
	}

	//modify by huangzh@20230707
	@SuppressWarnings("resource")
	@Override
	public Boolean work() throws Exception {
		OutputStream logOutputStream = null;

		if (!isInlineData) {
			if (!new File(importFilePathOrData).exists()) {
				throw new DbImportException("Import file does not exist: " + importFilePathOrData);
			} else if (new File(importFilePathOrData).isDirectory()) {
				throw new DbImportException("Import path is a directory: " + importFilePathOrData);
			}
		}

		Connection connection = null;
		boolean previousAutoCommit = false;
		try {
			connection = DbUtilities.createConnection(dbDefinition, true);
			previousAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);

			validItems = 0;
			invalidItems = new ArrayList<>();

			try {
				if (logFile != null) {
					logOutputStream = new FileOutputStream(logFile);
					logToFile(logOutputStream, getConfigurationLogString());
				}

				logToFile(logOutputStream, "Start: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getStartTime()));

				signalUnlimitedProgress();

				itemsToDo = getItemsAmountToImport();
				logToFile(logOutputStream, "Statements to execute: " + itemsToDo);
				signalProgress(true);


				try (Statement statement = connection.createStatement()) {
					//add by huangzh@20230707
					//检查是否为表（排除视图、函数等对象）
					if (dbDefinition.getDbVendor() == DbUtilities.DbVendor.MsSQL) {
						String tableCheckSql = "SELECT OBJECT_ID('" + tableName + "', 'u')";
						try (ResultSet resultSet = statement.executeQuery(tableCheckSql)) {
							resultSet.next();
							if (resultSet.getObject(1) == null) {
								System.out.println(tableName + " is not a table! will not import!");
								return !cancel;
							}
						}
					}

					//先清空，再插入数据
					System.out.println("importMode:" + importMode+"\r\ntableName:"+tableName);
					if (importMode == DbImportDefinition.ImportMode.CLEARINSERT) {
						parent.changeTitle(LangResources.get("clearTable"));
						deletedItems = DbUtilities.clearTable(connection, tableName);
						connection.commit();
					}

					/**
					 * microst sqlserver允许插入自增序列显性字段值
					 * 触发器先禁用
					 */
					boolean isInsertOn = false;
					boolean hasTrigger = false;
					if (dbDefinition.getDbVendor() == DbUtilities.DbVendor.MsSQL) {
						//校验microst sqlserver是否开启自增序列
						String checkInsertOnSql = "SELECT OBJECTPROPERTY(OBJECT_ID('"+tableName+"'),'TableHasIdentity')";
						//System.out.println("checkInsertOnSql-->"+checkInsertOnSql);
						try (ResultSet resultSet = statement.executeQuery(checkInsertOnSql)) {
							resultSet.next();
							if(resultSet.getInt(1)==1){
								isInsertOn = true;
							}
						}
						if(isInsertOn) {
							//允许插入自增序列显性字段值
							String identiyInsertOn = "set identity_insert " + tableName + " on";
							//System.out.println("identiyInsertOn-->" + identiyInsertOn);
							statement.execute(identiyInsertOn);
							connection.commit();
						}
						//触发器禁用
						String checkTriggerSql = "SELECT a.name AS table_name, " +
								"sysobjects.name AS trigger_name, " +
								"sysobjects.crdate, " +
								"sysobjects.info, " +
								"sysobjects.status " +
								"FROM sysobjects " +
								"LEFT JOIN ( SELECT * " +
								"FROM sysobjects " +
								"WHERE xtype = 'U' " +
								") AS a ON sysobjects.parent_obj = a.id " +
								"WHERE sysobjects.xtype = 'TR' and a.name='"+tableName+"'";
						try (ResultSet resultSet = statement.executeQuery(checkTriggerSql)) {
							if(resultSet.next()){
								hasTrigger = true;
							}
						}
						if(hasTrigger) {
							//禁用触发器
							String disabledTrigger = "disable trigger ALL on  " + tableName;
							System.out.println("disable trigger ALL on " + tableName);
							statement.execute(disabledTrigger);
							connection.commit();
						}
					}

					// Execute statements
					openReader();
					String nextStatement;
					while ((nextStatement = sqlScriptReader.readNextStatement()) != null) {
						try {
							dataItemsDone++;
							statement.execute(nextStatement);
							validItems++;
						} catch (final Exception e) {
							if (commitOnFullSuccessOnly) {
								connection.rollback();
								throw new Exception("Erroneous statement number " + (itemsDone + 1) + " at character index " + sqlScriptReader.getReadCharacters() + ": " + e.getMessage());
							}
							invalidItems.add((int) itemsDone);
							if (logErroneousData) {
								if (erroneousDataFile == null) {
									erroneousDataFile = new File(DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
								} else {
									FileUtilities.append(erroneousDataFile, "\n", StandardCharsets.UTF_8);
								}
								FileUtilities.append(erroneousDataFile, nextStatement, StandardCharsets.UTF_8);
							}
						}
						itemsDone++;
					}
					connection.commit();
					//add by huangzh@20230707
					if (dbDefinition.getDbVendor() == DbUtilities.DbVendor.MsSQL) {
						/**
						 * microst sqlserver禁止插入自增序列显性字段值
						 * 触发器先启用
						 */
						if(isInsertOn) {
							String identiyInsertOff = "set identity_insert " + tableName + " off";
							//System.out.println("identiyInsertOff-->"+identiyInsertOff);
							statement.execute(identiyInsertOff);
							connection.commit();
						}

						if(hasTrigger) {
							//启用触发器
							String enabledTrigger = "enable trigger ALL on  " + tableName;
							System.out.println("enable trigger ALL on " + tableName);
							statement.execute(enabledTrigger);
							connection.commit();
						}
					}
				}

				setEndTime(LocalDateTime.now());

				importedDataAmount += isInlineData ? importFilePathOrData.length() : new File(importFilePathOrData).length();

				if (importMode == DbImportDefinition.ImportMode.CLEARINSERT || importMode == DbImportDefinition.ImportMode.INSERT || importMode == DbImportDefinition.ImportMode.UPSERT) {
					insertedItems = validItems;
				}
				countItems = DbUtilities.getTableEntriesCount(connection, tableName);
				if (logErroneousData & invalidItems.size() > 0) {
					erroneousDataFile = dataProvider.filterDataItems(invalidItems, DateUtilities.formatDate(DateUtilities.DD_MM_YYYY_HH_MM_SS_ForFileName, getStartTime()) + ".errors");
				}
				logToFile(logOutputStream, getResultStatistics());

				final long elapsedTimeInSeconds = Duration.between(getStartTime(), getEndTime()).getSeconds();
				if (elapsedTimeInSeconds > 0) {
					final long itemsPerSecond = validItems / elapsedTimeInSeconds;
					logToFile(logOutputStream, "Import speed: " + itemsPerSecond + " items/second");
				} else {
					logToFile(logOutputStream, "Import speed: immediately");
				}
				logToFile(logOutputStream, "End: " + DateUtilities.formatDate(DateUtilities.getDateTimeFormatWithSecondsPattern(Locale.getDefault()), getEndTime()));
				logToFile(logOutputStream, "Time elapsed: " + DateUtilities.getHumanReadableTimespanEnglish(Duration.between(getStartTime(), getEndTime()), true));
			} catch (final SQLException sqle) {
				throw new DbImportException("SQL error: " + sqle.getMessage());
			} catch (final Exception e) {
				try {
					logToFile(logOutputStream, "Error: " + e.getMessage());
				} catch (final Exception e1) {
					e1.printStackTrace();
				}
				throw e;
			} finally {
				close();
				Utilities.closeQuietly(logOutputStream);
			}

			return !cancel;
		} catch (final Exception e) {
			throw e;
		} finally {
			if (connection != null) {
				connection.rollback();
				connection.setAutoCommit(previousAutoCommit);
				connection.close();
			}
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
		final InputStream inputStream = null;
		try {
			sqlScriptReader = new SqlScriptReader(getInputStream(), encoding);
		} catch (final Exception e) {
			Utilities.closeQuietly(sqlScriptReader);
			Utilities.closeQuietly(inputStream);
			throw e;
		}
	}

	@Override
	public Map<String, Tuple<String, String>> getMapping() throws Exception {
		return mapping;
	}
}
