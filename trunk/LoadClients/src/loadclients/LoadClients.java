/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loadclients;

import jargs.gnu.CmdLineParser;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
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
        DOMParser domp = new DOMParser();
        SAXParser saxp = new SAXParser();
        Dictionary dictionary = new Hashtable();
        XMLDocument xmld = null;



        try {
            parser.parse(args);
            InputStream input = new FileInputStream(
                    new File((String) parser.getOptionValue(xmlscheme)));
            domp.parse(input);
            xmld = domp.getDocument();
            NodeList nodeList = xmld.selectNodes("//ELEMENT");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node=nodeList.item(i);
                String key=node.getAttributes().getNamedItem("ID").getNodeValue()+node.getAttributes().getNamedItem("POS").getNodeValue();
                dictionary.put(key, node.getAttributes().getNamedItem("NAME").getNodeValue());
                System.out.println(dictionary.get(key));           
            }
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite://W:/kassa30.db");
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT SDOC,NDOC FROM ACCOUNT");
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
