package samples.join;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.FileWriter;

/**
 * This simple class uses javacvs library to read a multiline CSV file
 * and to transofrm it into a single line CSV
 * User: andrea
 * Date: 19/04/14
 * Time: 22.18
 */
public class CsvTransformer {

    public static void main(String[] args) throws Exception {

        CsvReader csvReader = new CsvReader("/home/andrea/forum_node.tsv");
        csvReader.setDelimiter('\t');
        csvReader.setTextQualifier('"');

        CsvWriter csvWriter = new CsvWriter(new FileWriter("forum_nodes_no_lf.tsv", false), '\t');
        csvWriter.setForceQualifier(true);
        csvWriter.setTextQualifier('"');
        csvWriter.setUseTextQualifier(true);

        csvWriter.write("id");
        csvWriter.write("title");
        csvWriter.write("tagnames");
        csvWriter.write("author_id");
        csvWriter.write("body");
        csvWriter.write("node_type");
        csvWriter.write("parent_id");
        csvWriter.write("added_at");
        csvWriter.write("score");
        csvWriter.endRecord();
        csvReader.readHeaders();

        int counter = 0;
        while (csvReader.readRecord()) {
            String id = csvReader.get("id");
            String title = csvReader.get("title");
            String tags = csvReader.get("tagnames");
            String authorId = csvReader.get("author_id");
            String body = csvReader.get("body");
            String nodeType = csvReader.get("node_type");
            String parentId = csvReader.get("parent_id");
            String addedAt = csvReader.get("added_at");
            String score = csvReader.get("score");

            csvWriter.write(id);
            csvWriter.write(title);
            csvWriter.write(tags);
            csvWriter.write(authorId);
            csvWriter.write(body.replaceAll("\n", " "));
            csvWriter.write(nodeType);
            csvWriter.write(parentId);
            csvWriter.write(addedAt);
            csvWriter.write(score);
            csvWriter.endRecord();
        }

        csvReader.close();
        csvWriter.close();
    }

}
