/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loadclients;

import jargs.gnu.CmdLineParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import oracle.xml.parser.v2.DOMParser;
import oracle.xml.parser.v2.SAXParser;
import oracle.xml.parser.v2.XMLDocument;
import oracle.xml.parser.v2.XMLElement;
import oracle.xml.parser.v2.XMLParser;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author gavrilov_a
 */
public class LoadClients {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException, FileNotFoundException {
        // TODO code application logic here
        Connection connection = null;
        ResultSet resultSet = null;
        Statement statement = null;
        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option xmlscheme;
        xmlscheme = parser.addStringOption('x', "xmlfile");
        CmdLineParser.Option fileIn = parser.addStringOption('i', "input");
        CmdLineParser.Option dataBase = parser.addStringOption('d', "database");
        CmdLineParser.Option table = parser.addStringOption('t', "table");
        CmdLineParser.Option bCountMax = parser.addIntegerOption('c', "batchcount");
        DOMParser domp = new DOMParser();
        SAXParser saxp = new SAXParser();
        HashMap rowsCol = new HashMap();
        XMLDocument xmld = null;
        //String[] strArr=line.split(",");


        try {
            /**
             * разбираем аргументы коммандной строки
             */
            parser.parse(args);

            /**
             * читаем xml-файл описания документа
             */
            InputStream input = new FileInputStream(
                    new File((String) parser.getOptionValue(xmlscheme)));
            domp.parse(input);
            xmld = domp.getDocument();
            NodeList nodeList = xmld.selectNodes("//ELEMENT");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                String rowkey = node.getAttributes().getNamedItem("ID").getNodeValue();
                String poskey = node.getAttributes().getNamedItem("POS").getNodeValue();
                if (!rowsCol.containsKey(rowkey)) {
                    rowsCol.put(rowkey, new HashMap());
                }
                ((HashMap) rowsCol.get(rowkey)).put(poskey, node.getAttributes().getNamedItem("NAME").getNodeValue());
            }

            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite://"+(String) parser.getOptionValue(dataBase));
            statement = connection.createStatement();
            //statement.execute("DROP TABLE " + (String) parser.getOptionValue(table));
            statement.execute("CREATE TABLE " + (String) parser.getOptionValue(table) + " (doc_serial VARCHAR(2),doc_num VARCHAR(7),row_type_id VARCHAR(5),position DECIMAL,NAME VARCHAR(255),VALUE VARCHAR(1000))");


            /**
             * читаем csv-файл
             */
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream((String) parser.getOptionValue(fileIn)), "cp866"));

            String strLine = "";





            //read comma separated file line by line
            int strNum = 1;
            int batchCount = 0;

            while ((strLine = br.readLine()) != null) {

                String[] strArr = strLine.split("\\|");
                //System.out.println("Строка: "+strNum+" размер: "+strArr.length);

                if (strArr.length > 2 && rowsCol.containsKey(strArr[3].trim())) {
                    HashMap hashMap = (HashMap) rowsCol.get(strArr[3].trim());
                    for (Iterator i = hashMap.keySet().iterator(); i.hasNext();) {
                        String key = (String) i.next();
                        int pos = Integer.parseInt(key);
                        //System.out.println("Строка: "+strArr[3].trim()+" Позиция: "+pos);
                        String val = (String) hashMap.get(key);
                        if (pos + 3 < strArr.length) {
                            //System.out.println(" Результат: " + strArr[1].trim() + strArr[2].trim() + " " + val + " : " + strArr[pos + 3].trim());
                            String strSql="INSERT INTO " + (String) parser.getOptionValue(table)
                                    + " (doc_serial ,doc_num ,row_type_id ,position ,NAME ,VALUE) VALUES ('"
                                    + strArr[1].trim() + "','" + strArr[2].trim() + "','" + strArr[3].trim() + "','" + val + "','" + strArr[pos + 3].trim() + "')";
                            System.out.println(strSql);
                            statement.addBatch(strSql);
                            batchCount++;
                        }
                    }
                }
                if (batchCount >= ((Integer) parser.getOptionValue(bCountMax))) {
                    batchCount = 0;
                    statement.executeBatch();
                    //statement=connection.createStatement();
                }
                strNum++;
            }








//            while (resultSet.next()) {
//                System.out.println("DOC:"
//                        + resultSet.getString("SDOC") + resultSet.getString("NDOC"));
//            }
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());

            System.exit(2);
        } catch (Exception e) {
            System.out.println(e);
        }
        connection.close();
    }
}
