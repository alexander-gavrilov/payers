/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package deposxml;

/**
 *
 * @author gavrilov_a
 */
import jargs.gnu.CmdLineParser;
import jargs.gnu.CmdLineParser.IllegalOptionValueException;
import jargs.gnu.CmdLineParser.UnknownOptionException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.xml.parser.v2.DOMParser;
import oracle.xml.parser.v2.XMLDocument;
import oracle.xml.parser.v2.XMLParseException;
import oracle.xml.parser.v2.XSLException;
import org.xml.sax.SAXException;

public class DeposXML {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        InputStream input = null;
        try {
            // TODO code application logic here
            Connection connection = null;
            ResultSet resultSet = null;
            Statement statement = null;
            DOMParser domp = new DOMParser();
            XMLDocument xmld;
            CmdLineParser parser = new CmdLineParser();
            CmdLineParser.Option outpath;
            outpath = parser.addStringOption('o', "outpath");
            String outpathStr;
            CmdLineParser.Option dataBase = parser.addStringOption('d', "database");
            String dbStr;
            CmdLineParser.Option bCountMax = parser.addIntegerOption('c', "batchcount");
            int bCMAxInt;
            CmdLineParser.Option xmlscheme;
            xmlscheme = parser.addStringOption('x', "xmlfile");
            String xmlfilestr;
            parser.parse(args);
            xmlfilestr = (String) parser.getOptionValue(xmlscheme);
            outpathStr = (String) parser.getOptionValue(outpath);
            dbStr = (String) parser.getOptionValue(dataBase);
            bCMAxInt = (Integer) parser.getOptionValue(bCountMax);
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite://" + dbStr);
            statement = connection.createStatement();

            input = new FileInputStream(new File(xmlfilestr));
            domp.parse(input);
            xmld = domp.getDocument();
            
            String clientsSQL = xmld.valueOf("//SELECT[@id='clients']/text()");
            
            
            resultSet=statement.executeQuery(clientsSQL);
            String a="a";

        } catch (XSLException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        }  catch (XMLParseException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalOptionValueException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnknownOptionException ex) {
            Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                input.close();
            } catch (IOException ex) {
                Logger.getLogger(DeposXML.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
