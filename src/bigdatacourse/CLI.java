package bigdatacourse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;

import bigdatacourse.code.cpkg;



public class CLI {

	// consts
	private static final String				KEYSPACE			=	"bigdatacourse";

	private static final String				FILE_ASTRA_DB			=	"astradb.zip";
	private static final String				FILE_GENERATED_TOKEN	=	"GeneratedToken.csv";

	private static final String				FILE_DATASET_ITEMS		=	"meta_Office_Products.json";
	private static final String				FILE_DATASET_REVIEWS	=	"reviews_Office_Products.json";

	private static final String				FILE_SEPARATOR			=	System.getProperty("file.separator");


	public static void main(String[] args) throws Exception {
		CLI CLI = new CLI();		// creating the object
		CLI.parsePassedFolders(args);	// saving
		CLI.runCLI();					// running the CLI

		System.out.println("CLI terminated, bye bye...");
	}


	private String 		pathAstraDBFolder;		// with the "Secure Connect Bundle" and GeneratedToken.csv
	private String 		pathDatasetFolder;		// the dataset folder
	private API			API;					// will contain student answers


	public CLI() {
		this.API				=	new cpkg();
	}


	public void runCLI() {
		// initializing the input
		Scanner scanner = new Scanner(System.in);

		// print the menu
		printHelp();

		// looping
		boolean isRunning = true;
		while (isRunning) {
			// getting the input
			System.out.print("> ");
			String line		=	scanner.nextLine();
			String[] tokens	=	line.split(" ");
			String input	=	tokens[0];

			try {
				switch (input) {
					case "connect":				API.connect(pathAstraDBFolder + FILE_ASTRA_DB,
																getUsername(pathAstraDBFolder + FILE_GENERATED_TOKEN),
																getPasswrod(pathAstraDBFolder + FILE_GENERATED_TOKEN),
																KEYSPACE);
												break;

					case "createTables":		API.createTables();			break;
					case "initialize":			API.initialize();			break;
					case "loadItems":			API.loadItems(pathDatasetFolder + FILE_DATASET_ITEMS);		break;
					case "loadReviews":			API.loadReviews(pathDatasetFolder + FILE_DATASET_REVIEWS);	break;
					case "item":				API.item(tokens[1]);											break;
					case "userReviews":			API.userReviews(tokens[1]);	break;
					case "itemReviews":			API.itemReviews(tokens[1]);	break;

					case "help":				printHelp();					break;
					case "exit":				isRunning = false;
												API.close();
												break;

					case "":					// do nothing
												break;

					default:					System.out.println("Unknonw input. type 'help'");
												break;
				}
			}
			catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		// closing the scanner
		scanner.close();
	}



	private void printHelp() {
		System.out.println("-------------------- options ----------------------------");
		System.out.println("connect \t\t connect to the DB");
		System.out.println("createTables\t\t creates the tables");
		System.out.println("initialize\t\t initialize the logic (prepared statements)");
		System.out.println("loadItems\t\t prase and lode the items");
		System.out.println("loadReviews\t\t prase and lode the reviews");
		System.out.println("item * \t\t \t print the info for item *");
		System.out.println("userReviews *\t\t print the reviews for user *");
		System.out.println("itemReviews *\t\t print the reviews for item *");
		System.out.println("help    \t\t print available commands");
		System.out.println("exit    \t\t exit the CLI");
		System.out.println("-------------------------------------------------------------");
	}




	private void parsePassedFolders(String[] args) throws Exception {
		if (args.length != 2)
			throw new Exception("ERROR - 2 folder paths are required to be passed: first astradb and then dataset folders");

		String pathAstraDBFolder 	= 	args[0] + FILE_SEPARATOR;
		String pathDatasetFolder	=	args[1] + FILE_SEPARATOR;


		// validating files for astradb
		validateFileExists(pathAstraDBFolder, FILE_ASTRA_DB);
		validateFileExists(pathAstraDBFolder, FILE_GENERATED_TOKEN);

		// validating files for dataset
		validateFileExists(pathDatasetFolder, FILE_DATASET_ITEMS);
		validateFileExists(pathDatasetFolder, FILE_DATASET_REVIEWS);

		// saving
		this.pathAstraDBFolder	=	pathAstraDBFolder;
		this.pathDatasetFolder	=	pathDatasetFolder;
	}


	private static void validateFileExists(String path, String filename) throws Exception {
		if (new File(path + filename).exists() == false)
			throw new Exception("ERROR - can not find file " + path + filename);
	}

	private static String getUsername(String pathGeneratedTokenFile) throws Exception {
		return parseTokenFile(pathGeneratedTokenFile, 0);
	}

	private static String getPasswrod(String pathGeneratedTokenFile) throws Exception {
		return parseTokenFile(pathGeneratedTokenFile, 1);
	}

	private static String parseTokenFile(String pathGeneratedTokenFile, int columnIndex) throws Exception {
		String answer;
		try {
			// opening the file
			BufferedReader reader = new BufferedReader(new FileReader(pathGeneratedTokenFile));

			// getting the second lind;
			String line =	reader.readLine();
			line		=	reader.readLine();

			// splitting the line by ","
			String[] parsedLine = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

			// getting the column and removing the "
			answer = parsedLine[columnIndex].replaceAll("\"", "");

			// closing
			reader.close();
		}
		catch (Exception e) {
			System.out.println("ERROR parsing generated token file");
			throw e;
		}

		return answer;
	}

}
