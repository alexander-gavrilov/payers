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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import oracle.xml.parser.v2.DOMParser;
import oracle.xml.parser.v2.SAXParser;
import oracle.xml.parser.v2.XMLDocument;
import oracle.xml.parser.v2.XMLElement;
import oracle.xml.parser.v2.XSLException;
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
    static PreparedStatement pStat;
    static String[] insertAttrs;
    static int ind = 0;
    static SimpleDateFormat outDateFormat = new SimpleDateFormat("dd/MM/yyyy");
    static String insStr = "";

    public static void main(String[] args) throws SQLException, FileNotFoundException {
        // TODO code application logic here
        Connection connection = null;
        ResultSet resultSet = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

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
        Boolean rbt = false;
        CmdLineParser.Option pRebuild = parser.addBooleanOption('r', "rebuild");
        int bCMAxInt = 0;
        long currentdt;
        DOMParser domp = new DOMParser();
        SAXParser saxp = new SAXParser();
        HashMap rows;

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
            rbt = (Boolean) parser.getOptionValue(pRebuild);

            /**
             * читаем xml-файл описания документа
             */
            InputStream input = new FileInputStream(
                    new File(xmlfilestr));
            domp.parse(input);
            xmld = domp.getDocument();
            String delimiter = xmld.getDocumentElement().getAttribute("DELIMITER");
            NodeList nodeList = xmld.selectNodes("DOC/ELEMENT");
            rows = getElementHashMap(nodeList);


            //Class.forName("org.sqlite.JDBC"); "jdbc:oracle:thin:cds/cds_pws@192.168.51.126:1521:lrise1"
            connection = DriverManager.getConnection(dbStr);
            statement = connection.createStatement();
            //statement.executeUpdate("CREATE OR REPLACE TABLE if exists " + tableStr + ";");
            if (rbt) {
                statement.executeUpdate("DROP TABLE " + tableStr /*+ ";"*/);
            }
            String crStat = "CREATE TABLE " + tableStr + " ( " + " RN NUMBER," + getCreateStat(rows) + " )";
            //statement.execute("DROP TABLE " + tableStr);
            statement.execute(crStat);
            ArrayList<String[]> strArArray = new ArrayList<String[]>();
            crStat = "INSERT INTO " + tableStr
                    + " VALUES (" + getInsertStat(rows) + ");";

            pStat = connection.prepareStatement(crStat);
            /**
             * читаем csv-файл
             */
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileInStr), "cp1251"));
            String strLine = "";
            int strNum = 1;
            int batchCount = 0;
            int elemCount = 0;
            int elemEscCount = 0;
            int insertElemCount = 0;
            int escapeStr = 0;
            int[] rowsInserted;
            String userName = "";
            int userId;
            int clientID = 0;
            Date changeClientDT = null;
            java.sql.Time upClDT = null;
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            currentdt = System.currentTimeMillis();
            int prevRow = 0;
            //System.out.println("Test\ttab");
            try {
                while ((strLine = br.readLine()) != null) {
                    //String[] strA = strLine.split("\\|");
                    ind = 1;
                    pStat.clearParameters();
                    insStr = "";
                    /*if (strLine.length() > rows.size()) {*/
                    try {
                        //System.out.println("Row "+strNum);
                        parseString(rows, strLine, "", "", delimiter);
                    } catch (Exception e) {
                        ind = 0;
                        System.out.println("Row " + strNum);
                    }


                    //}
                    elemCount += ind;
                    if (ind >= rows.size()) {
//                        pStat.addBatch();
                        String strStat = "INSERT INTO " + tableStr
                                + " VALUES (" + strNum + "," + insStr.substring(0, insStr.length() - 1) + ")";
                        pStat.clearParameters();
                        //System.out.println(strStat);

                        statement.execute(strStat);
                        batchCount++;
                        if (batchCount >= (bCMAxInt)) {
                            prevRow = strNum;
                            batchCount = 0;
                            currentdt = System.currentTimeMillis();
                            //rowsInserted = pStat.executeBatch();
                            //insertElemCount += rowsInserted.length;
                            connection.commit();
                            pStat.clearBatch();
                            currentdt = System.currentTimeMillis();
                        }


//                        if (strA[3].trim().equals("30000") && strArArray.size() > 0) {
//                            userId = Integer.parseInt(strA[6].trim());
//                            userName = strA[5].trim();
//                            changeClientDT = dateFormat.parse(strA[4].trim() + " " + strA[7].trim());
//                            clientID++;
//                            for (Iterator<String[]> it = strArArray.iterator(); it.hasNext();) {
//                                String[] strArr = it.next();
//                                HashMap hashMap = (HashMap) rowsCol.get(strArr[3].trim());
//
//
//                                for (Iterator i = hashMap.keySet().iterator(); i.hasNext();) {
//                                    String key = (String) i.next();
//                                    int pos = Integer.parseInt(key);
//                                    String val = (String) hashMap.get(key);
//                                    if (pos + 3 < strArr.length && strArr[pos + 3].trim().length() > 0) {
//                                        pStat.setInt(1, clientID);
//                                        pStat.setString(2, strArr[0].trim());
//                                        pStat.setString(3, strArr[1].trim());
//                                        pStat.setString(4, strArr[2].trim());
//                                        pStat.setString(5, strArr[3].trim());
//                                        pStat.setInt(6, pos);
//                                        pStat.setString(7, val);
//                                        pStat.setString(8, strArr[pos + 3].trim());
//                                        upClDT = new java.sql.Time(changeClientDT.getTime());
//
//                                        pStat.setString(9, dateFormat.format(changeClientDT));
//
//
//                                        pStat.addBatch();
//                                        batchCount++;
//                                    }
//
//                                }
//                                if (batchCount >= (bCMAxInt)) {
//                                    //System.out.println("Обработано строк: " + (strNum - prevRow) + " Последняя строка: " + strNum);
//                                    prevRow = strNum;
//                                    //System.out.println("Время подготовки: " + (System.currentTimeMillis() - currentdt));
//                                    batchCount = 0;
//                                    currentdt = System.currentTimeMillis();
//                                    rowsInserted = pStat.executeBatch();
//                                    insertElemCount += rowsInserted.length;
//                                    //System.out.println("Время запроса: " + (System.currentTimeMillis() - currentdt));
//                                    connection.commit();
//                                    pStat.clearBatch();
//                                    currentdt = System.currentTimeMillis();
//                                }
//                            }
//                            strArArray.clear();
//                            userId = 99999;
//                            userName = "(LWO) Гаврилов А.В.";
//                            changeClientDT = new Date(System.currentTimeMillis());
//
//                        }
//
////                        if(strA[3].trim().equals("30000"))
////                        {
////                            userId=Integer.parseInt(strA[6].trim());
////                            userName=strA[5].trim();
////                            changeClientDT=dateFormat.parse(strA[4].trim()+" "+strA[7].trim());
////                            
////                        }
                    } else {
                        escapeStr++;
                        elemEscCount += ind;
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

    static HashMap getElementHashMap(NodeList nodeList) throws XSLException {
        HashMap rowsCol = new HashMap();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            //XMLElement xmlEl = (XMLElement) nodeList.item(i);
            //xmlEl.selectNodes("ATTR")
            //node.getTextContent()
            String poskey = node.getAttributes().getNamedItem("POS").getNodeValue();
            String type = node.getAttributes().getNamedItem("TYPE").getNodeValue();
            int pos = Integer.parseInt(poskey);
            //node.normalize();
            //node=nodeList.item(i).cloneNode(true);
            //org.w3c.dom.Element xmlEl= (org.w3c.dom.Element) node; 
            //node.getNodeType();
            System.out.println(nodeToString(node));
            if (type.equals("STRUCT")) {
                //String str = "DOC/ELEMENT[@NAME='" + node.getAttributes().getNamedItem("NAME").getNodeValue() + "']/SUBELEM";
                NodeList nLst = ((XMLElement) node).selectNodes("SUBELEM");
                rowsCol.put(pos, new Element(pos,
                        node.getAttributes().getNamedItem("NAME").getNodeValue(),
                        type,
                        getElementHashMap(nLst),
                        node.getAttributes().getNamedItem("DELIMITER").getNodeValue(),
                        node.getAttributes().getNamedItem("STARTSYM").getNodeValue(),
                        node.getAttributes().getNamedItem("ENDSYM").getNodeValue()));
            } else if (type.equals("DATE")) {
                rowsCol.put(pos, new Element(pos,
                        node.getAttributes().getNamedItem("NAME").getNodeValue(),
                        type,
                        node.getAttributes().getNamedItem("FORMAT").getNodeValue()));
            } else {
                rowsCol.put(pos, new Element(pos,
                        node.getAttributes().getNamedItem("NAME").getNodeValue(),
                        type));
            }
            //((HashMap) rowsCol.get(rowkey)).put(poskey, node.getAttributes().getNamedItem("NAME").getNodeValue());
        }
        return rowsCol;

    }

    static void parseString(HashMap schema, String inStr, String startsym, String endsym, String delsym) throws ParseException, SQLException {
        String cuStr = inStr.substring(startsym.length(), inStr.length() - endsym.length());
        String[] strA;
        if (delsym.equals("\\t")) {
            String st003=cuStr.replace((char) 28, (char) 124)+" ";
            strA = st003.split("\\|");
        } else {
            strA = cuStr.split("\\|");
        }
        if (strA.length < schema.size()) {
            System.out.println(cuStr);
            throw new java.lang.ArrayIndexOutOfBoundsException();
        }
        SimpleDateFormat inputDateFormat = new SimpleDateFormat();
        for (int i = 0; i < schema.size(); i++) {
            Element el = (Element) schema.get(i);
            //System.out.println(i+"\t\t"+strA[i]);
            if (el.getType().equals("STRUCT")) {
                parseString(el.getStruct(), strA[i], el.getStructStart(), el.getStructEnd(), el.getStructDel());
            } else if (el.getType().equals("DATE")
                    && i < strA.length) {
                if (strA[i].trim().length() == el.getFormat().length()) {
                    insStr += "TO_DATE('" + strA[i].trim() + "','" + el.getFormat() + "'),";
                    pStat.setString(ind, strA[i].trim());
                    //System.out.println(el.getName() + "\t\t\t\t" + ind + "\t\t\t\t" + strA[i].trim());
                    ind++;
                } else {
                    pStat.setString(ind, "");
                    insStr += "'',";
                    //System.out.println(el.getName() + "\t\t\t\t" + ind + "\t\t\t\tNULL" );
                    ind++;
                }
            } else if (el.getType().equals("NUMBER") && i < strA.length) {
                insStr += "'" + strA[i].trim().replaceAll(",", "").replace('.', ',') + "',";
                pStat.setString(ind, strA[i].trim().replaceAll(",", "").replace('.', ','));
                //System.out.println(el.getName() + "\t\t\t\t" + ind + "\t\t\t\t" + strA[i].trim().replace('.', ','));
                ind++;
            } else if (i < strA.length) {
                insStr += "'" + strA[i].trim() + "',";
                if (!strA[i].trim().equals("")) {
                    pStat.setString(ind, strA[i].trim());

                } else {
                    pStat.setString(ind, "");
                }
                //System.out.println(el.getName() + "\t\t\t\t" + ind + "\t\t\t\t" + strA[i].trim());
                ind++;
            } else {
                pStat.setString(ind, "");
                insStr += "'',";
                //System.out.println(el.getName() + "\t\t\t\t" + ind + "\t\t\t\tNULL" );
                ind++;
            }



        }
    }

    static String getCreateStat(HashMap schema) {
        String resStr = "";
        for (int i = 0; i < schema.size(); i++) {
            Element el = (Element) schema.get(i);
            //System.out.println(el.getName());
            if (el.getType().equals("STRUCT")) {
                resStr += getCreateStat(el.getStruct()) + ",";
            } else {
                resStr += el.getName() + " " + el.getType() + ",";
            }
        }
        return resStr.substring(0, resStr.length() - 1);
    }

    static String getInsertStat(HashMap schema) {
        String resStr = "";
        for (int i = 0; i < schema.size(); i++) {
            Element el = (Element) schema.get(i);
            if (el.getType().equals("STRUCT")) {
                resStr += getInsertStat(el.getStruct()) + ",";
            } else if (el.getType().equals("DATE")) {
                resStr += "TO_DATE(?,'" + el.getFormat() + "')),";
                ind++;
            } else {
                resStr += "?,";
                ind++;
            }
        }
        return resStr.substring(0, resStr.length() - 1);
    }

    static private String nodeToString(Node node) {
        StringWriter sw = new StringWriter();
        try {
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.transform(new DOMSource(node), new StreamResult(sw));
        } catch (TransformerException te) {
            System.out.println("nodeToString Transformer Exception");
        }
        return sw.toString();
    }
}
