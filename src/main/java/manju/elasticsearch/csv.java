package manju.elasticsearch;

import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import com.opencsv.CSVWriter;

public class csv {
	public static void main(String[] args) throws Exception {

		        CSVWriter csvWriter = new CSVWriter(new FileWriter("//home//e1066//Documents//JS//JS task - aug22//example.csv"));
		        csvWriter.writeNext(new String[]{"1", "jan", "Male", "20"});
		        csvWriter.writeNext(new String[]{"2", "con", "Male", "24"});
		        csvWriter.writeNext(new String[]{"3", "jane", "Female", "18"});
		        csvWriter.writeNext(new String[]{"4", "ryo", "Male", "28"});
		 
		        csvWriter.close();
		    
		 }
}
