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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
        SimpleDateFormat dateFormat=new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
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
            connection = DriverManager.getConnection("jdbc:oracle:thin:cds/cds_pws@192.168.51.126:1521:lrise1");
            statement = connection.createStatement();
            //statement.executeUpdate("CREATE OR REPLACE TABLE if exists " + tableStr + ";");
            String crStat="CREATE TABLE " + tableStr + " (id NUMBER,doc_type VARCHAR(10),doc_serial VARCHAR(20),doc_num VARCHAR(20),row_type_id VARCHAR(5),position NUMBER,NAME VARCHAR(255),VALUE VARCHAR(1000),DTCHANGE DATE)";
            statement.execute(crStat);
            ArrayList<String[]> strArArray = new ArrayList<String[]>();
            PreparedStatement pStat = connection.prepareStatement("INSERT INTO " + tableStr
                    + " (id,doc_type,doc_serial ,doc_num ,row_type_id ,position ,NAME ,VALUE,DTCHANGE) VALUES (?,?,?,?,?,?,?,?,TO_DATE(?,'DD/MM/YYYY HH24:MI:SS'))");
            /**
             * читаем csv-файл
             */
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileInStr), "cp866"));
            String strLine = "";
            int strNum = 1;
            int batchCount = 0;
            int elemCount = 0;
            int elemEscCount = 0;
            int insertElemCount = 0;
            int escapeStr = 0;
            int[] rowsInserted;
            String userName="";
            int userId;
            int clientID=0;
            Date changeClientDT = null;
            java.sql.Time upClDT=null;
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            currentdt = System.currentTimeMillis();
            int prevRow = 0;
            try {
                while ((strLine = br.readLine()) != null) {
                    String[] strA = strLine.split("\\|");


                    elemCount += strA.length;
                    if (strA.length > 2 && rowsCol.containsKey(strA[3].trim())) {
                        strArArray.add(strA);
                        if (strA[3].trim().equals("30000") && strArArray.size() > 0) {
                            userId=Integer.parseInt(strA[6].trim());
                            userName=strA[5].trim();
                            changeClientDT=dateFormat.parse(strA[4].trim()+" "+strA[7].trim());
                            clientID++;
                            for (Iterator<String[]> it = strArArray.iterator(); it.hasNext();) {
                                String[] strArr = it.next();
                                HashMap hashMap = (HashMap) rowsCol.get(strArr[3].trim());


                                for (Iterator i = hashMap.keySet().iterator(); i.hasNext();) {
                                    String key = (String) i.next();
                                    int pos = Integer.parseInt(key);
                                    String val = (String) hashMap.get(key);
                                    if (pos + 3 < strArr.length && strArr[pos + 3].trim().length() > 0) {
                                        pStat.setInt(1, clientID);
                                        pStat.setString(2, strArr[0].trim());
                                        pStat.setString(3, strArr[1].trim());
                                        pStat.setString(4, strArr[2].trim());
                                        pStat.setString(5, strArr[3].trim());
                                        pStat.setInt(6, pos);
                                        pStat.setString(7, val);
                                        pStat.setString(8, strArr[pos + 3].trim());
                                        upClDT=new java.sql.Time(changeClientDT.getTime());
                                        
                                        pStat.setString(9, dateFormat.format(changeClientDT));


                                        pStat.addBatch();
                                        batchCount++;
                                    }
                                    
                                }
                                if (batchCount >= (bCMAxInt)) {
                                        //System.out.println("Обработано строк: " + (strNum - prevRow) + " Последняя строка: " + strNum);
                                        prevRow = strNum;
                                        //System.out.println("Время подготовки: " + (System.currentTimeMillis() - currentdt));
                                        batchCount = 0;
                                        currentdt = System.currentTimeMillis();
                                        rowsInserted = pStat.executeBatch();
                                        insertElemCount += rowsInserted.length;
                                        //System.out.println("Время запроса: " + (System.currentTimeMillis() - currentdt));
                                        connection.commit();
                                        pStat.clearBatch();
                                        currentdt = System.currentTimeMillis();
                                    }
                            }
                            strArArray.clear();
                            userId=99999;
                            userName="(LWO) Гаврилов А.В.";
                            changeClientDT=new Date(System.currentTimeMillis());
                                    
                        }
                        
//                        if(strA[3].trim().equals("30000"))
//                        {
//                            userId=Integer.parseInt(strA[6].trim());
//                            userName=strA[5].trim();
//                            changeClientDT=dateFormat.parse(strA[4].trim()+" "+strA[7].trim());
//                            
//                        }
                    } else {
                        escapeStr++;
                        elemEscCount += strA.length;
                    }
                    strNum++;

                }
                if (batchCount != 0) {
                    //System.out.println("Обработано строк: " + (strNum - prevRow) + " Последняя строка: " + strNum);
                    prevRow = strNum;
                    //System.out.println("Стоимость подготовки: " + (System.currentTimeMillis() - currentdt));
                    currentdt = System.currentTimeMillis();
                    rowsInserted = pStat.executeBatch();
                    insertElemCount += rowsInserted.length;
                    //System.out.println("Стоимость запроса: " + (System.currentTimeMillis() - currentdt));
                    connection.commit();
                    pStat.clearBatch();
                    currentdt = System.currentTimeMillis();
                }
                System.out.println("Всего обработано строк: " + strNum + ", содержащих " + elemCount + " элементов");
                System.out.println("Из них:");
                System.out.println("        пропущено: " + escapeStr + " строк, содержащих " + elemEscCount + " элементов");
                System.out.println("        вставлено в БД: " + insertElemCount + " элементов");


            } catch (SQLException se) {
                connection.rollback();
                System.out.println(se.getMessage());
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
