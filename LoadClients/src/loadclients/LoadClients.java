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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.Date;
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
        String xmlfilestr = "";
        CmdLineParser.Option fileIn = parser.addStringOption('i', "input");
        String fileInStr = "";
        CmdLineParser.Option dataBase = parser.addStringOption('d', "database");
        String dbStr = "";
        CmdLineParser.Option table = parser.addStringOption('t', "table");
        String tableStr = "";
        CmdLineParser.Option bCountMax = parser.addIntegerOption('c', "batchcount");
        int bCMAxInt = 0;
        long currentdt;
        DOMParser domp = new DOMParser();
        SAXParser saxp = new SAXParser();
        HashMap rowsCol = new HashMap();
        XMLDocument xmld = null;
        try {
            /**
             * разбираем аргументы коммандной строки
             */
            parser.parse(args);
            xmlfilestr = (String) parser.getOptionValue(xmlscheme);
            fileInStr = (String) parser.getOptionValue(fileIn);
            dbStr = (String) parser.getOptionValue(dataBase);
            tableStr = (String) parser.getOptionValue(table);
            bCMAxInt = (Integer) parser.getOptionValue(bCountMax);
            /**
             * читаем xml-файл описания документа
             */
            InputStream input = new FileInputStream(
                    new File(xmlfilestr));
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
            connection = DriverManager.getConnection("jdbc:sqlite://" + dbStr);
            statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE if exists " + tableStr + ";");
            statement.execute("CREATE TABLE " + tableStr + " (doc_serial VARCHAR(2),doc_num VARCHAR(7),row_type_id VARCHAR(5),position DECIMAL,NAME VARCHAR(255),VALUE VARCHAR(1000))");
            PreparedStatement pStat = connection.prepareStatement("INSERT INTO " + tableStr
                    + " (doc_serial ,doc_num ,row_type_id ,position ,NAME ,VALUE) VALUES (?,?,?,?,?,?)");
            /**
             * читаем csv-файл
             */
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileInStr), "cp866"));
            String strLine = "";
            int strNum = 1;
            int batchCount = 0;
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            currentdt = System.currentTimeMillis();
            try {
                while ((strLine = br.readLine()) != null) {
                    String[] strArr = strLine.split("\\|");
                    if (strArr.length > 2 && rowsCol.containsKey(strArr[3].trim())) {
                        HashMap hashMap = (HashMap) rowsCol.get(strArr[3].trim());
                        for (Iterator i = hashMap.keySet().iterator(); i.hasNext();) {
                            String key = (String) i.next();
                            int pos = Integer.parseInt(key);
                            String val = (String) hashMap.get(key);
                            if (pos + 3 < strArr.length && strArr[pos + 3].trim() != "") {
                                pStat.setString(1, strArr[1].trim());
                                pStat.setString(2, strArr[2].trim());
                                pStat.setString(3, strArr[3].trim());
                                pStat.setInt(4, pos);
                                pStat.setString(5, val);
                                pStat.setString(6, strArr[pos + 3].trim());
                                pStat.addBatch();
                                batchCount++;
                            }
                            if (batchCount >= (bCMAxInt)) {
                                System.out.println("Стоимость подготовки: " + (System.currentTimeMillis() - currentdt));
                                batchCount = 0;
                                currentdt = System.currentTimeMillis();
                                pStat.executeBatch();
                                System.out.println("Стоимость запроса: " + (System.currentTimeMillis() - currentdt));
                                connection.commit();
                                pStat.clearBatch();
                                currentdt = System.currentTimeMillis();
                            }

                        }
                    }
                    strNum++;

                }
                if (batchCount != 0) {
                    System.out.println("Стоимость подготовки: " + (System.currentTimeMillis() - currentdt));
                    currentdt = System.currentTimeMillis();
                    pStat.executeBatch();
                    System.out.println("Стоимость запроса: " + (System.currentTimeMillis() - currentdt));
                    connection.commit();
                    pStat.clearBatch();
                    currentdt = System.currentTimeMillis();
                }

            } catch (SQLException se) {
                connection.rollback();
            }
            connection.setAutoCommit(true);
        } catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());

            System.exit(2);
        } catch (SQLException e) {
            System.out.println(e);
        } catch (Exception e) {
            System.out.println(e);
        }
        connection.close();
    }
}
