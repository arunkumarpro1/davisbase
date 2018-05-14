package edu.arun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

/**
 * @author Chris Irwin Davis
 * @version 1.0 <b>
 *          <p>
 *          This is an example of how to create an interactive prompt
 *          </p>
 *          <p>
 *          There is also some guidance to get started wiht read/write of binary
 *          data files using RandomAccessFile class
 *          </p>
 *          </b>
 *
 */
public class DavisBasePrompt {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2016 Chris Irwin Davis";
	static boolean isExit = false;
	static Map<String, SerialTypeCode> serialTypeCodes;
	static Column[] davisTablesColumns = new Column[4];
	static Column[] davisColumnsColumns = new Column[8];
	static CCJSqlParserManager parser = new CCJSqlParserManager();
	static String database = null;
	static String genericError = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax.";

	static {
		Map<String, SerialTypeCode> aMap = new HashMap<>();
		aMap.put("NULL TINYINT", new SerialTypeCode(0, 1));
		aMap.put("NULL SMALLINT", new SerialTypeCode(1, 2));
		aMap.put("NULL INT", new SerialTypeCode(2, 4));
		aMap.put("NULL DOUBLE", new SerialTypeCode(3, 8));
		aMap.put("TINYINT", new SerialTypeCode(4, 1));
		aMap.put("SMALLINT", new SerialTypeCode(5, 2));
		aMap.put("INT", new SerialTypeCode(6, 4));
		aMap.put("BIGINT", new SerialTypeCode(7, 8));
		aMap.put("REAL", new SerialTypeCode(8, 4));
		aMap.put("DOUBLE", new SerialTypeCode(9, 8));
		aMap.put("DATETIME", new SerialTypeCode(10, 8));
		aMap.put("DATE", new SerialTypeCode(11, 8));
		aMap.put("TEXT", new SerialTypeCode(12, 0));
		serialTypeCodes = Collections.unmodifiableMap(aMap);

		davisTablesColumns[0] = new Column("rowid", "INT", "N", "PRI");
		davisTablesColumns[1] = new Column("table_name", "TEXT", "N", "NULL");
		davisTablesColumns[2] = new Column("record_count", "INT", "N", "NULL");
		davisTablesColumns[3] = new Column("database_name", "TEXT", "N", "NULL");

		davisColumnsColumns[0] = new Column("rowid", "INT", "N", "PRI");
		davisColumnsColumns[1] = new Column("table_name", "TEXT", "N", "NULL");
		davisColumnsColumns[2] = new Column("column_name", "TEXT", "N", "NULL");
		davisColumnsColumns[3] = new Column("data_type", "TEXT", "N", "NULL");
		davisColumnsColumns[4] = new Column("ordinal_postion", "TINYINT", "N", "NULL");
		davisColumnsColumns[5] = new Column("is_nullable", "TEXT", "N", "NULL");
		davisColumnsColumns[6] = new Column("column_key", "TEXT", "N", "NULL");
		davisColumnsColumns[7] = new Column("database_name", "TEXT", "N", "NULL");

	}

	static BTree davisTablesTree;
	static BTree davisColumnsTree;

	/*
	 * Page size for alll files is 512 bytes by default. You may choose to make
	 * it user modifiable
	 */
	static long pageSize = 512;

	/*
	 * The Scanner class is used to collect user commands from the prompt There
	 * are many ways to do this. This is just one.
	 *
	 * Each time the semicolon (;) delimiter is entered, the userCommand String
	 * is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	/**
	 * ***********************************************************************
	 * Main method
	 *
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		boolean firstTime = false;

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		// create the directories
		Path path = Paths.get("data\\catalog");
		if (!Files.exists(path)) {
			try {
				Files.createDirectories(path);
			} catch (IOException e) {
				// fail to create directory
				e.printStackTrace();
			}
		}

		// create metadata tables
		try {
			if (!Files.exists(Paths.get("data\\catalog\\davisbase_tables.tbl"), LinkOption.NOFOLLOW_LINKS)) {
				firstTime = true;
			}
			davisTablesTree = new BTree("catalog\\davisbase_tables", pageSize);
			davisColumnsTree = new BTree("catalog\\davisbase_columns", pageSize);

			if (firstTime) {
				davisTablesTree.filePointer.setLength(0);
				davisColumnsTree.filePointer.setLength(0);

				insertIntoDavisTableMetaData("davisbase_tables", "catalog");
				insertIntoDavisTableMetaData("davisbase_columns", "catalog");

				insertIntoDavisColumnsMetaData("davisbase_tables", davisTablesColumns, "catalog");
				insertIntoDavisColumnsMetaData("davisbase_columns", davisColumnsColumns, "catalog");
			}
		} catch (FileNotFoundException e) {
			System.out.println("Unable to open Metadata tables..!");
			return;
		}

		/* Display the welcome screen */
		splashScreen();

		while (!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}

	private static void insertIntoDavisTableMetaData(String tableName, String database) throws IOException {
		Payload payload = new Payload();
		short payloadLength;
		int rowId = davisTablesTree.getLastRowId();
		payload.noOfColumns = 3;
		// Maximum length of a table name can be 40.
		payload.serialTypeCodes = new byte[payload.noOfColumns];
		payload.serialTypeCodes[0] = getSerialTypeCode("TEXT", tableName.length());
		payload.serialTypeCodes[1] = getSerialTypeCode("INT", 0);
		payload.serialTypeCodes[2] = getSerialTypeCode("TEXT", database.length());
		payload.columnData = new LinkedList<>();
		payload.columnData.add(new Column(tableName, "TEXT", "N", "NULL"));
		payload.columnData.add(new Column("0", "INT", "N", "NULL"));
		payload.columnData.add(new Column(database, "TEXT", "N", "NULL"));
		payloadLength = (short) (1 + payload.noOfColumns + tableName.length() + 4 + database.length());

		davisTablesTree.insert(++rowId, payload, payloadLength);
	}

	private static void insertIntoDavisColumnsMetaData(String tableName, Column[] tableColumns, String database)
			throws IOException {
		Payload payload = new Payload();
		short payloadLength;
		int rowId = davisColumnsTree.getLastRowId();
		for (int i = 0; i < tableColumns.length; i++) {
			payload.noOfColumns = (byte) (davisColumnsColumns.length - 1);
			payload.serialTypeCodes = new byte[payload.noOfColumns];
			payload.serialTypeCodes[0] = getSerialTypeCode("TEXT", tableName.length());
			payload.serialTypeCodes[1] = getSerialTypeCode("TEXT", tableColumns[i].column.length());
			payload.serialTypeCodes[2] = getSerialTypeCode("TEXT", tableColumns[i].type.length());
			payload.serialTypeCodes[3] = getSerialTypeCode("TINYINT", i + 1);
			payload.serialTypeCodes[4] = getSerialTypeCode("TEXT", 1);
			payload.serialTypeCodes[5] = getSerialTypeCode("TEXT", tableColumns[i].isPrimary.length());
			payload.serialTypeCodes[6] = getSerialTypeCode("TEXT", database.length());
			payload.columnData = new LinkedList<>();
			payload.columnData.add(new Column(tableName, "TEXT", "N", "NULL"));
			payload.columnData.add(new Column(tableColumns[i].column, "TEXT", "N", "NULL"));
			payload.columnData.add(new Column(tableColumns[i].type, "TEXT", "N", "NULL"));
			payload.columnData.add(new Column(String.valueOf(i + 1), "TINYINT", "N", "NULL"));
			payload.columnData.add(new Column(tableColumns[i].isNullable, "TEXT", "N", "NULL"));
			payload.columnData.add(new Column(tableColumns[i].isPrimary, "TEXT", "N", "NULL"));
			payload.columnData.add(new Column(database, "TEXT", "N", "NULL"));
			payloadLength = (short) (1 + payload.noOfColumns + tableName.length() + tableColumns[i].column.length()
					+ tableColumns[i].type.length() + 1 + 1 + tableColumns[i].isPrimary.length() + database.length());
			davisColumnsTree.insert(++rowId, payload, payloadLength);
		}
	}

	private static byte getSerialTypeCode(String typeName, int length) {
		if (typeName.equals("TEXT")) {
			return (byte) (serialTypeCodes.get(typeName).typeCode + length);
		} else {
			return serialTypeCodes.get(typeName).typeCode;
		}
	}

	private static short getLengthFromType(String typeName, String value) {
		if (typeName.equals("TEXT")) {
			return (short) value.length();
		} else {
			return serialTypeCodes.get(typeName).length;
		}
	}

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself
	 *         num times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*", 80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.printf("\t%-65s %s\n", "USE <database_name>;",
				"Uses the database for subsequent queries. You have to select the database before querying any table.");
		System.out.printf("\t%-65s %s\n", "SHOW DATABASES;", "Displays a list of all databases in DavisBase.");
		System.out.printf("\t%-65s %s\n", "SHOW TABLES", "Displays a list of all tables in the current database.");
		System.out.printf("\t%-65s %s\n", "CREATE DATABASE <database_name>",
				"Creates a new database schema, i.e. a new empty database.");
		System.out.printf("\t%-65s %s\n", "CREATE TABLE table_name[columname type constraint]",
				"Creates a new table schema, i.e. a new empty table.");
		System.out.printf("\t%-65s %s\n", "INSERT INTO table_name VALUES value_list",
				"Inserts a single record into a table (value_list size must be n).");
		System.out.printf("\t%-65s %s\n", "INSERT INTO table_name [column_list] VALUES value_list",
				"Inserts a single record into a table.");
		System.out.printf("\t%-65s %s\n", "DELETE FROM table_name [WHERE condition]",
				"Deletes one or more records from the table.");
		System.out.printf("\t%-65s %s\n", "UPDATE table_name SET column_name = value,...  [WHERE condition]",
				"Modifies one or more records in a table.");
		System.out.printf("\t%-65s %s\n", "SELECT * FROM table_name;", "Display all records in the table.");
		System.out.printf("\t%-65s %s\n", "SELECT * FROM table_name WHERE rowid = <value>;",
				"Display records whose rowid is <value>.");
		System.out.printf("\t%-65s %s\n", "SELECT [column_list] FROM table_name WHERE rowid = <value>;",
				"Display the specified column values for records whose rowid is <value>.");
		System.out.printf("\t%-65s %s\n", "SELECT [column_list] FROM table_name WHERE columnname = <value>;",
				"Display the specified column values for records whose attribute/field named columnname has the value <value>.");
		System.out.printf("\t%-65s %s\n", "DROP TABLE table_name;", "Remove table data, its schema.and its metdadata");
		System.out.printf("\t%-65s %s\n", "DROP DATABASE database_name;",
				"Remove a database schema, and all of its contained tables.");
		System.out.printf("\t%-65s %s\n", "VERSION;", "Show the program version.");
		System.out.printf("\t%-65s %s\n", "HELP;", "Show this help information");
		System.out.printf("\t%-65s %s\n", "EXIT;", "Exit the program");
		System.out.printf("\t%-65s %s\n", "QUIT;", "Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*", 80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) throws IOException {

		/*
		 * commandTokens is an array of Strings that contains one token per
		 * array element The first token can be used to determine the type of
		 * command The other tokens can be used to pass relevant parameters to
		 * each command-specific method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		/*
		 * This switch handles a very small list of hardcoded commands of known
		 * syntax. You will want to rewrite this method to interpret more
		 * complex commands.
		 */

		if (database == null && (!userCommand.toLowerCase().startsWith("use")
				&& !userCommand.toLowerCase().startsWith("create database")
				&& !userCommand.toLowerCase().startsWith("show database")
				&& !userCommand.toLowerCase().startsWith("help") && !userCommand.toLowerCase().startsWith("exit")
				&& !userCommand.toLowerCase().startsWith("quit"))) {
			createError("ERROR 1046 (3D000): No database selected");
			return;
		}

		switch (commandTokens.get(0).toLowerCase()) {
		case "select":
			parseQueryString(userCommand);
			break;
		case "drop":
			parseDropQueryString(userCommand);
			break;
		case "create":
			parseCreateQuery(userCommand);
			break;
		case "insert":
			parseInsertString(userCommand);
			break;
		case "update":
			parseUpdateString(userCommand);
			break;
		case "delete":
			parseDeleteString(userCommand);
			break;
		case "show":
			parseShowString(userCommand);
			break;
		case "use":
			parseUseQuery(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	private static void parseDropQueryString(String userCommand) throws IOException {
		if (userCommand.toLowerCase().startsWith("drop table"))
			dropTableQuery(userCommand);
		else if (userCommand.toLowerCase().startsWith("drop database"))
			dropDatabaseQuery(userCommand);
		else
			createError("Error in SQL statement. Valid query can start with 'DROP TABLE' or 'DROP DATABASE'");
	}

	private static void dropDatabaseQuery(String userCommand) throws IOException {
		String db = Arrays.asList(userCommand.split(" ")).get(2);
		if (!Files.exists(Paths.get("data\\" + db), LinkOption.NOFOLLOW_LINKS)) {
			createError("ERROR 1008 (HY000): Can't drop database '" + db + "'; database doesn't exist");
		} else {
			File f = new File("data\\" + db);
			for (String name : f.list()) {
				davisColumnsTree.delete(2, name.replaceAll(".tbl", ""), 8, "=", "TEXT");
				davisTablesTree.delete(2, name.replaceAll(".tbl", ""), 4, "=", "TEXT");
			}
			System.out.println("Query OK, " + f.list().length + " rows affected");
			Path directory = Paths.get("data\\" + db);
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
			});
			if (database.equals(db))
				database = null;
		}
	}

	private static void parseUseQuery(String userCommand) {
		String db = Arrays.asList(userCommand.split(" ")).get(1);
		if (Files.exists(Paths.get("data\\" + db), LinkOption.NOFOLLOW_LINKS)) {
			database = db;
			System.out.println("Database changed");
		} else
			createError("ERROR 1049 (42000): Unknown database '" + db + "'");
	}

	private static void parseCreateQuery(String userCommand) throws IOException {
		if (userCommand.toLowerCase().startsWith("create database"))
			createDatabaseQuery(userCommand);
		else if (userCommand.toLowerCase().startsWith("create table"))
			createTableQuery(userCommand);
		else
			createError("Error in SQL statement. Valid query can start with 'CREATE TABLE' or 'CREATE DATABASE'");
	}

	private static void createDatabaseQuery(String userCommand) throws IOException {
		String db = Arrays.asList(userCommand.split(" ")).get(2);
		if (Files.exists(Paths.get("data\\" + db), LinkOption.NOFOLLOW_LINKS)) {
			createError("ERROR 1007 (HY000): Can't create database '" + db + "'; database exists");
		} else {
			Files.createDirectories(Paths.get("data\\" + db));
			System.out.println("Query OK, 1 row affected");
		}
	}

	private static void parseShowString(String userCommand) throws IOException {
		if (userCommand.equalsIgnoreCase("show tables"))
			showTables();
		else if (userCommand.equalsIgnoreCase("show databases"))
			showDatabases();
		else
			createError("Error in SQL statement. Valid query can either be 'SHOW TABLES' or 'SHOW DATABASES'");
	}

	private static void showDatabases() {
		File f = new File("data");
		List<List<Column>> names = new LinkedList<>();
		for (File dir : f.listFiles()) {
			if (dir.isDirectory()) {
				List<Column> nameList = new LinkedList<>();
				nameList.add(new Column(dir.getName(), "", "", ""));
				names.add(nameList);
			}
		}

		List<List<Column>> column = new LinkedList<>();
		List<Column> columnName = new LinkedList<>();
		columnName.add(new Column("Database", "", "", ""));
		column.add(columnName);

		QueryResultPrinter.printQueryResult(names, column, "DATABASES", null);
	}

	private static void showTables() throws IOException {
		File f = new File("data\\" + database);
		List<List<Column>> names = new LinkedList<>();
		for (String name : f.list()) {
			List<Column> nameList = new LinkedList<>();
			nameList.add(new Column(name.replaceAll(".tbl", ""), "", "", ""));
			names.add(nameList);
		}

		List<List<Column>> column = new LinkedList<>();
		List<Column> columnName = new LinkedList<>();
		columnName.add(new Column("Tables_in_" + database, "", "", ""));
		column.add(columnName);

		QueryResultPrinter.printQueryResult(names, column, "TABLES", null);
	}

	private static void parseDeleteString(String userCommand) throws IOException {
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(userCommand));
		} catch (JSQLParserException e) {
		}
		String tabName = null;
		if (statement instanceof Delete) {
			Delete deleteStat = (Delete) statement;
			tabName = deleteStat.getTable().getName().toLowerCase();
			if (Files.notExists(Paths.get("data\\" + database + "\\" + tabName + ".tbl"), LinkOption.NOFOLLOW_LINKS)) {
				createError("ERROR 1146 (42S02): Table '" + database + "." + deleteStat.getTable().getName()
						+ "' doesn't exist");
				return;
			}
			ComparisonOperator e = (ComparisonOperator) deleteStat.getWhere();
			String whereColumn = e.getLeftExpression().toString().toLowerCase();
			String whereValue = e.getRightExpression().toString().toLowerCase().replaceAll("^('|\")|('|\")$", "");
			String operator = e.getStringExpression();
			List<List<Column>> columns = davisColumnsTree.select(2, tabName, 5, "=", "TEXT");
			int wherePosition = 0;
			String whereType = null;
			for (List<Column> colTuple : columns) {
				if (colTuple.get(2).column.equals(whereColumn)) {
					wherePosition = Integer.parseInt(colTuple.get(4).column);
					whereType = colTuple.get(3).type;
					break;
				}
			}

			BTree tableTree = new BTree(database + "\\" + tabName, pageSize);
			System.out.println(
					"Query OK, " + tableTree.delete(wherePosition, whereValue, columns.size(), operator, whereType)
							+ " rows affected");
		} else {
			createError(genericError);
		}
	}

	// Parses the Update query and updates the corresponding record using update
	private static void parseUpdateString(String userCommand) throws IOException {
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(userCommand));
		} catch (JSQLParserException e) {
		}
		List<net.sf.jsqlparser.schema.Column> colList = null;
		String tabName = null;
		if (statement instanceof Update) {
			Update updateStat = (Update) statement;
			tabName = updateStat.getTables().get(0).getName().toLowerCase();
			if (Files.notExists(Paths.get("data\\" + database + "\\" + tabName + ".tbl"), LinkOption.NOFOLLOW_LINKS)) {
				createError("ERROR 1146 (42S02): Table '" + database + "." + tabName + "' doesn't exist");
				return;
			}
			colList = updateStat.getColumns();
			List<String> colNames = new LinkedList<>();
			for (net.sf.jsqlparser.schema.Column s : colList)
				colNames.add(s.getColumnName().toLowerCase());
			List<String> updateValues = new LinkedList<>();
			List<Expression> expressions = updateStat.getExpressions();
			for (Expression exp : expressions)
				updateValues.add(exp.toString().replaceAll("^('|\")|('|\")$", ""));
			String whereColumn = "", whereValue = "", operator = "";
			ComparisonOperator e = (ComparisonOperator) updateStat.getWhere();
			if (e != null) {
				whereColumn = e.getLeftExpression().toString();
				whereValue = e.getRightExpression().toString();
				operator = e.getStringExpression();
			}
			List<List<Column>> columns = davisColumnsTree.select(2, tabName, 5, "=", "TEXT");
			int wherePosition = 0;
			String whereType = null, updateType = null;
			List<Integer> updatePositions = new LinkedList<>();
			for (List<Column> colTuple : columns) {
				if (colTuple.get(2).column.equals(whereColumn.toLowerCase())) {
					wherePosition = Integer.parseInt(colTuple.get(4).column);
					whereType = colTuple.get(3).type;
				}
				if (colNames.contains(colTuple.get(2).column)) {
					updatePositions.add(Integer.parseInt(colTuple.get(4).column));
				}
			}

			if (updatePositions.contains(1)) {
				createError("Error: Primary key cannot be updated");
				return;
			} else if (updatePositions.isEmpty()) {
				createError("Error: The given column(s) are not present in the table '" + tabName + "'.");
				return;
			}
			BTree tableTree = new BTree(database + "\\" + tabName, pageSize);
			System.out.println("Query OK, "
					+ tableTree.update(wherePosition, updatePositions, whereValue.replaceAll("^('|\")|('|\")$", ""),
							updateValues, columns.size(), operator, whereType, updateType)
					+ " rows affected");
		} else {
			createError(genericError);
		}
	}

	private static void parseInsertString(String userCommand) throws IOException {
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(userCommand));
		} catch (JSQLParserException e) {
		}
		List<net.sf.jsqlparser.schema.Column> colList = null;
		List<String> colNames = new LinkedList<>();
		String tabName = null;
		Integer key;
		if (statement instanceof Insert) {
			Insert insertStat = (Insert) statement;
			tabName = insertStat.getTable().getName().toLowerCase();
			if (Files.notExists(Paths.get("data\\" + database + "\\" + tabName + ".tbl"), LinkOption.NOFOLLOW_LINKS)) {
				createError("ERROR 1146 (42S02): Table '" + database + "." + insertStat.getTable().getName()
						+ "' doesn't exist");
				return;
			}
			colList = insertStat.getColumns();
			if (colList != null)
				for (net.sf.jsqlparser.schema.Column c : colList)
					colNames.add(c.getColumnName().toLowerCase());
			List<Expression> expressionList = ((ExpressionList) insertStat.getItemsList()).getExpressions();
			List<String> values = new LinkedList<>();

			for (Expression exp : expressionList)
				values.add(exp.toString().replaceAll("^'|'$", ""));
			BTree tableTree = new BTree(database + "\\" + tabName, pageSize);
			List<List<Column>> columns = davisColumnsTree.select(2, tabName, 7, "=", "TEXT");
			Payload payload = new Payload();
			payload.noOfColumns = (byte) (columns.size() - 1);
			payload.columnData = new LinkedList<>();
			payload.serialTypeCodes = new byte[payload.noOfColumns];
			short payloadLength = (short) (1 + payload.noOfColumns);
			if (values.size() == columns.size()) {
				for (int i = 1; i <= payload.noOfColumns; i++) {
					payload.serialTypeCodes[i - 1] = getSerialTypeCode(columns.get(i).get(3).column,
							values.get(i).length());
				}
				for (int i = 1; i < columns.size(); i++) {
					payload.columnData.add(new Column(values.get(i), columns.get(i).get(3).column,
							columns.get(i).get(5).column, "NULL"));
					payloadLength += getLengthFromType(columns.get(i).get(3).column, values.get(i));
				}
			} else {
				int i = -1;
				for (List<Column> colAttributes : columns) {
					if (!colNames.contains(colAttributes.get(2).column)) {
						if (colAttributes.get(6).column.equals("PRI")) {
							createError("ERROR 1364 (HY000): Primary Key not provided in the statement: Field '"
									+ colAttributes.get(2).column + "' doesn't have a default value");
							return;
						} else if (colAttributes.get(5).column.equals("N")) {
							createError("ERROR 1364 (HY000): Field '" + colAttributes.get(2).column
									+ "' doesn't have a default value. It has a NOT NULL constraint.");
							return;
						}
						if (colAttributes.get(3).column.equals("TEXT")) {
							payload.serialTypeCodes[i] = getSerialTypeCode(colAttributes.get(3).column, 4);
							payload.columnData.add(new Column("NULL", "TEXT", "Y", "NULL"));
							payloadLength += 4;
						} else
							payloadLength += getNULLSerialTypeCode(payload,
									serialTypeCodes.get(colAttributes.get(3).column), i);
					} else if (i != -1) {
						int j = colNames.indexOf(colAttributes.get(2).column);
						payload.serialTypeCodes[i] = getSerialTypeCode(colAttributes.get(3).column,
								values.get(j).length());
						payload.columnData.add(new Column(values.get(j), columns.get(i).get(3).column,
								columns.get(i).get(5).column, "NULL"));
						payloadLength += getLengthFromType(columns.get(i).get(3).column, values.get(j));
					}
					i++;
				}
			}

			if ((key = (Integer.valueOf(values.get(0)))) != null) {
				if (tableTree.insert(key, payload, payloadLength) != 0)
					System.out.println("Query OK, 1 row affected");
			} else
				createError("ERROR: Invalid Primary Key");
		} else {
			createError(genericError);
		}
	}

	private static byte getNULLSerialTypeCode(Payload payload, SerialTypeCode serialTypeCode, int i) {
		switch (serialTypeCode.length) {
		case 1:
			payload.serialTypeCodes[i - 1] = 0;
			payload.columnData.add(new Column("", "NULL TINYINT", "Y", "NULL"));
			return 1;
		case 2:
			payload.serialTypeCodes[i - 1] = 1;
			payload.columnData.add(new Column("", "NULL SMALLINT", "Y", "NULL"));
			return 2;
		case 4:
			payload.serialTypeCodes[i - 1] = 2;
			payload.columnData.add(new Column("", "NULL INT", "Y", "NULL"));
			return 4;
		default:
			payload.serialTypeCodes[i - 1] = 3;
			payload.columnData.add(new Column("", "NULL DOUBLE", "Y", "NULL"));
			return 8;
		}
	}

	/*
	 *
	 * }
	 *
	 * /** Stub method for dropping tables
	 *
	 * @param dropTableString is a String of the user input
	 */
	public static void dropTableQuery(String dropTableString) throws IOException {
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(dropTableString));
		} catch (JSQLParserException e) {
		}
		Table tabName = null;
		if (statement instanceof Drop) {
			Drop dropStat = (Drop) statement;
			tabName = dropStat.getName();
			File tableFile = new File("data\\" + database + "\\" + tabName.getName().toLowerCase() + ".tbl");
			tableFile.delete();
			davisColumnsTree.delete(2, tabName.getName().toLowerCase(), 8, "=", "TEXT");
			davisTablesTree.delete(2, tabName.getName().toLowerCase(), 4, "=", "TEXT");

		} else {
			createError(genericError);
		}
	}

	/**
	 * Stub method for executing queries
	 *
	 * @param queryString
	 *            is a String of the user input
	 * @throws IOException
	 */
	public static void parseQueryString(String queryString) throws IOException {
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(queryString));
		} catch (JSQLParserException e) {
		}
		String tableName = null;
		PlainSelect selectBody = null;
		if (statement instanceof Select) {
			Select selectStat = (Select) statement;
			selectBody = (PlainSelect) selectStat.getSelectBody();
			tableName = selectBody.getFromItem().toString().toLowerCase();
			if (Files.notExists(Paths.get("data\\" + database + "\\" + tableName + ".tbl"),
					LinkOption.NOFOLLOW_LINKS)) {
				createError("ERROR 1146 (42S02): Table '" + database + "." + selectBody.getFromItem().toString()
						+ "' doesn't exist");
				return;
			}
			List<SelectItem> se = selectBody.getSelectItems();
			List<Integer> ordinalPostions = null;
			ComparisonOperator e = (ComparisonOperator) selectBody.getWhere();
			List<List<Column>> columns = davisColumnsTree.select(2, tableName, 5, "=", "TEXT");
			List<String> columnNames;
			if (!se.isEmpty() && !se.get(0).toString().equals("*")) {
				columnNames = new LinkedList<>();
				ordinalPostions = new LinkedList<>();
				for (SelectItem s : se)
					columnNames.add(s.toString().toLowerCase());
				for (List<Column> colTuple : columns) {
					if (columnNames.contains(colTuple.get(2).column))
						ordinalPostions.add(Integer.valueOf(colTuple.get(4).column));
				}
			}
			List<List<Column>> queryData;
			BTree tableTree = new BTree(database + "\\" + tableName, pageSize);
			if (e != null) {
				String whereColumn = e.getLeftExpression().toString();
				String whereValue = e.getRightExpression().toString().replaceAll("^('|\")|('|\")$", "");
				String operator = e.getStringExpression();
				int wherePosition = 0;
				String whereType = null;
				for (List<Column> colTuple : columns)
					if (colTuple.get(2).column.equals(whereColumn.toLowerCase())) {
						wherePosition = Integer.parseInt(colTuple.get(4).column);
						whereType = colTuple.get(3).type;
						break;
					}
				queryData = tableTree.select(wherePosition, whereValue, columns.size(), operator, whereType);
			} else
				queryData = tableTree.selectTable(columns.size());
			QueryResultPrinter.printQueryResult(queryData, columns, tableName, ordinalPostions);
		} else {
			createError(genericError);
		}
	}

	/**
	 * Stub method for creating new tables
	 *
	 * @param queryString
	 *            is a String of the user input
	 * @throws IOException
	 */
	public static void createTableQuery(String createTableString) throws IOException {

		createTableString = createTableString.toLowerCase();
		Statement statement = null;
		try {
			statement = parser.parse(new StringReader(createTableString));
		} catch (JSQLParserException e) {
		}
		List<ColumnDefinition> colList = null;
		Table tabName = null;
		if (statement instanceof CreateTable) {
			CreateTable createTabSt = (CreateTable) statement;
			tabName = createTabSt.getTable();
			colList = createTabSt.getColumnDefinitions();
		} else {
			createError(genericError);
			return;
		}
		/*
		 * if table name and column details are present
		 */
		if (tabName == null && colList == null) {
			createError(genericError);
			return;
		} else if (colList.get(0).getColumnSpecStrings() == null
				|| !colList.get(0).getColumnSpecStrings().contains("primary")) {
			createError("Primary key not provided");
			return;
		} else {
			String tableName = tabName.getFullyQualifiedName().toLowerCase();
			Column[] tableColumns = new Column[colList.size()];

			if (!davisTablesTree.select(2, tableName, 4, "=", "TEXT").isEmpty()) {
				createError("ERROR 1146 (42S02): Table '" + tableName + "' already exist");
				return;
			}

			int i = 0;
			for (ColumnDefinition c : colList) {
				tableColumns[i++] = new Column(c.getColumnName(), c.getColDataType().toString().toUpperCase(),
						c.getColumnSpecStrings() != null && ((c.getColumnSpecStrings().contains("not")
								&& c.getColumnSpecStrings().contains("null"))
								|| c.getColumnSpecStrings().contains("primary")) ? "N" : "Y",
						c.getColumnSpecStrings() != null && c.getColumnSpecStrings().contains("primary") ? "PRI"
								: "NULL");
			}

			RandomAccessFile tableFile = null;

			/* YOUR CODE GOES HERE */

			/* Code to create a .tbl file to contain table data */
			try {
				/*
				 * Create RandomAccessFile tableFile in read-write mode. Note
				 * that this doesn't create the table file in the correct
				 * directory structure
				 */
				tableFile = new RandomAccessFile("data\\" + database + "\\" + tableName + ".tbl", "rw");
				tableFile.setLength(0);
				// Code to insert a row in the davisbase_tables table
				insertIntoDavisTableMetaData(tableName, database);

				// Code to insert rows in the davisbase_columns table
				insertIntoDavisColumnsMetaData(tableName, tableColumns, database);
				System.out.println("Query OK, 0 rows affected");
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				try {
					tableFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void createError(String err) {
		System.out.println("*** " + err + "***\n");
	}
}

class Column {
	String column;
	String type;
	String isNullable;
	String isPrimary;

	public Column(String column, String type, String isNullable, String isPrimary) {
		super();
		this.column = column;
		this.type = type;
		this.isNullable = isNullable;
		this.isPrimary = isPrimary;
	}

	@Override
	public String toString() {
		return column;
	}

}