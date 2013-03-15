/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loadclients;

import java.util.HashMap;

/**
 *
 * @author gavrilov_a
 */
public class Element {

    private int position;
    private String name;
    private String type;
    private String format;
    private HashMap struct;
    private String structDel;
    private String structStart;
    private String structEnd;

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public HashMap getStruct() {
        return struct;
    }

    public void setStruct(HashMap struct) {
        this.struct = struct;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStructDel() {
        return structDel;
    }

    public void setStructDel(String structDel) {
        this.structDel = structDel;
    }

    public String getStructStart() {
        return structStart;
    }

    public void setStructStart(String structStart) {
        this.structStart = structStart;
    }

    public String getStructEnd() {
        return structEnd;
    }

    public void setStructEnd(String structEnd) {
        this.structEnd = structEnd;
    }

    public Element(int position, String name, String type, HashMap struct, String structDel, String structStart, String structEnd) {
        this.position = position;
        this.name = name;
        this.type = type;
        this.struct = struct;
        this.structDel = structDel;
        this.structStart = structStart;
        this.structEnd = structEnd;
    }

    public Element(int position, String name, String type) {
        this.position = position;
        this.name = name;
        this.type = type;
    }

    public Element() {
    }

    public Element(int position, String name, String type, String format) {
        this.position = position;
        this.name = name;
        this.type = type;
        this.format = format;
    }

    public Element(int position, String name, String type, HashMap struct) {
        this.position = position;
        this.name = name;
        this.type = type;
        this.struct = struct;
    }
}
