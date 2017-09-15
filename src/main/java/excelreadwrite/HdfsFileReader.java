package excelreadwrite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * Created by vaijnathp on 9/15/2017.
 */
public class HdfsFileReader {

        public static void main (String [] args) throws Exception{
            try{
                Path pt=new Path(args[0]);
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    line=br.readLine();
                }
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }
    }

