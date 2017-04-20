import java.io.File;
import java.io.FileNotFoundException;
import java.io.*;


public class CSV {
    public static void main(String[]args) throws FileNotFoundException{
        PrintWriter pw = new PrintWriter(new File("c2.csv"));
        FileReader fr = new FileReader("c2.txt");
        BufferedReader br = new BufferedReader(fr);
        StringBuilder sb = new StringBuilder();
        String line;
        try{
            while((line=br.readLine()) != null){
                line = line.replace(" ",",");
                sb.append(line);
                sb.append("\n");

            }
            pw.write(sb.toString());
            pw.close();
            System.out.println("done!");

        }
        catch(Exception e){

        }
        
    }
}