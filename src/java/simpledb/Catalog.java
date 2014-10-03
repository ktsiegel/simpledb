package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
	/** 
	 * Holds the files in the catalog; maps file IDs to a data structure
	 * that holds more information about that particular file.
	 */
	ConcurrentHashMap<Integer,FileItem> tables;
	
	/**
     * A helper class to facilitate organizing the information of each table
     * */
    public static class FileItem {

    	/** The name of the file */
        public final String name;
        
        /** The file object */
        public final DbFile file;
        
        /** The primary key field of the file */
        public final String pKey;

        public FileItem(String n, DbFile f, String p) {
            this.name = n;
            this.file = f;
            this.pKey = p;
        }
    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	tables = new ConcurrentHashMap<Integer, FileItem>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identifier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * 	  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     * 
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	tables.put(file.getId(), new FileItem(name, file, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
    	Object[] ids = tables.keySet().toArray();
    	// iterate through all of the items in the tables hash map
    	// to find the entry with the correct name.
    	for (Object id: ids) {
    		if ( ((FileItem)tables.get(id)).name.equals(name) ) return ((Integer)id).intValue();
    	}
    	// If no element with the given name exists, then throw an exception.
    	throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	DbFile file;
        try {
        	// look up the database file and throw an exception if it does not exist.
        	file = this.getDatabaseFile(tableid);
        } catch (NoSuchElementException e) {
        	throw new NoSuchElementException();
        }
        return file.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        FileItem fileItem = tables.get(tableid);
        if (fileItem == null) throw new NoSuchElementException();
        return fileItem.file;
    }

    public String getPrimaryKey(int tableid) {
        return tables.get(tableid).pKey;
    }

    public Iterator<Integer> tableIdIterator() {
        return tables.keySet().iterator();
    }

    public String getTableName(int id) {
        return tables.get(id).name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
    	tables = new ConcurrentHashMap<Integer, FileItem>();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

