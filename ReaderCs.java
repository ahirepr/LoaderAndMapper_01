package com.logilite.training.csvtodatabase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ReaderCs {
	static Logger log = Logger.getLogger(ReaderCs.class);
	private static String url = "";
	private static String user = "";
	private static String password = "";
	static Properties props = null;
	static ReaderCs readerCsv;
	static Scanner sc = new Scanner(System.in);
	static Statement st = null;
	static Connection con = null;
	ResultSet rs = null;
	static DatabaseMetaData dbm = null;
	String headers;
	String line;
	static int count = 0;
	static String file = null;
	static String fileName = null;
	static int noOfCsvRecord = 0;
	static String[] head = null;
	static String[] tempHead = null;
	BufferedReader br;
	static String columnName = "";
	static String tableName = "";

	public static void main(String[] args) throws IOException {
		System.out.println("hi");
		 PropertyConfigurator.configure("src/resource/log4j.properties");
		readerCsv = new ReaderCs();
		readerCsv.setDatabaseProperties();
		try {
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
			dbm = con.getMetaData();
			File[] files = readerCsv.readCsvDirectory();
			String propertyFileName = "";
			for (int i = 0; i < files.length; i++) {
				log.info("Application start");
				propertyFileName = files[i].getAbsoluteFile().getName();
				log.info(propertyFileName + "..............");
				propertyFileName = propertyFileName.substring(0, propertyFileName.length() - 4);
				log.info(propertyFileName + ":..............");
				tableName = propertyFileName;
				log.info(tableName + "  <--tableName");
				readerCsv.readPropertiesFile(propertyFileName);
				readerCsv.readcsvfile(files[i]);
			}
			log.info("Hi.......database operation complete..");
			log.info("Total records in csv file are:" + noOfCsvRecord);
			log.info("No of fails record:" + count);
			log.info("No of pass record from csv11:" + (noOfCsvRecord - count));

		} catch (ClassNotFoundException | SQLException e1) {
			e1.printStackTrace();
		}

	}

	private File[] readCsvDirectory() {
		File[] files = new File("src/resource/").listFiles(obj -> obj.isFile() && obj.getName().endsWith(".csv"));

		return files;

	}

	private void readcsvfile(File file) {

		try {

			BufferedReader br = new BufferedReader(new FileReader(file));
			try {
				headers = br.readLine();
				head = headers.split(",");
				tempHead = head;
				columnName = "";
				for (int i = 0; i < head.length; i++) {
					head[i] = props.getProperty(head[i]) + ",";
					System.out.println(" " + head[i]);
					columnName = columnName + head[i];
				}
				columnName = columnName.substring(0, columnName.length() - 1);
				log.info(columnName + " ---");

			} catch (IOException e) {
				e.printStackTrace();
			}

			dataInsertion(br);
		}

		catch (FileNotFoundException e) {
			log.debug("File is not available in current directory");
		}
	}

	private void dataInsertion(BufferedReader br) {
		try {
			while ((line = br.readLine()) != null) {
				if (line.trim().length() != 0) {
					noOfCsvRecord++;
					String[] record = line.split(",");
					String recordvalues = "";
					String suffixPrefix = "'";

					for (int j = 0; j < record.length; j++) {
						record[j] = suffixPrefix + record[j] + suffixPrefix;
						recordvalues = recordvalues + record[j];
						recordvalues = recordvalues.concat(",");

					}
					recordvalues = recordvalues.substring(0, recordvalues.length() - 1);

					String query = "insert into " + tableName + "(" + columnName + ") " + "values(" + recordvalues
							+ ");";

					log.info(query);
					ResultSet tables = dbm.getTables(null, null, tableName, null);
					log.info("____________________________________");
					if (!tables.next()) {
						System.out.println("go to create table  " + Arrays.toString(head));

						String createTableQuery = createTable(fileName);
						st.execute(createTableQuery);
						log.info("Table is created");
					} else {
						log.info("table is already present");
					}
					st.execute(query);
					log.info("Records store");

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			log.debug(e + "  " + "sqlexception------------");
			count++;
			try {
				dataInsertion(br);
				br.readLine();

			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (NullPointerException e) {
			try {

				br.readLine();
			} catch (IOException e1) {
				e1.printStackTrace();
				log.debug("IOException----------------");
			}
		}
	}

	private static String createTable(String fileName) {
		log.info(columnName + ":");
		String[] createcolumnName = columnName.split(",");
		String primar = tableName + "_id serial primary key,";
		String columns = primar;

		for (int i = 0; i < createcolumnName.length; i++) {
			createcolumnName[i] = createcolumnName[i] + " char(30),";
			columns = columns + createcolumnName[i];
		}
		columns = columns.substring(0, columns.length() - 1);
		log.info(columns + "  :columnnam");

		String query = "create table " + tableName + "(" + columns + ");";
		log.info(query + "-------------------------");
		return query;

	}

	public void setDatabaseProperties() {
		url = "jdbc:postgresql://localhost/FreshUp";
		user = "postgres";
		password = "postgres";
	}

	public Properties readPropertiesFile(String file) throws IOException {
		props = readPropertiesFile1(new File("src/resource/" + file + ".properties"));
		log.info("property files are readed");

		return props;
	}

	public static Properties readPropertiesFile1(File fileName) {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(fileName);
			props = new Properties();
			props.load(fis);
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			log.error(ioe);
		}
		return props;
	}
}
/*
 * 1) pk should be like tablename_ID and not come from data 2) dont ask file
 * name instead read it form resource folder and process all available .csv
 * files 3) assume all data is string
 */
