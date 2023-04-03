package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * @Threadsafe
 */

/**
 * 存储所有表记录
 */
public class Catalog {

    /**
     * 然后我们再来说说为什么又将内部类设计为静态内部类与内部类：首先来看一下静态内部类的特点：如 昭言 用户所述那样，我是静态内部类，只不过是想借你的外壳用一下。
     * 本身来说，我和你没有什么“强依赖”上的关系。没有你，我也可以创建实例。
     * 那么，在设计内部类的时候我们就可以做出权衡：如果我内部类与你外部类关系不紧密，耦合程度不高，不需要访问外部类的所有属性或方法，那么我就设计成静态内部类。
     * 而且，由于静态内部类与外部类并不会保存相互之间的引用，因此在一定程度上，还会节省那么一点内存资源，何乐而不为呢~~
     *
     * 既然上面已经说了什么时候应该用静态内部类，那么如果你的需求不符合静态内部类所提供的一切好处，你就应该考虑使用内部类了。
     * 最大的特点就是：你在内部类中需要访问有关外部类的所有属性及方法，我知晓你的一切... ...
     *
     */
    /**
     * 表类
     */
    private static class Table{
        /**
         * 对应文件
         */
        DbFile dbFile;
        /**
         * 表名称
         */
        String name;
        /**
         * 表主键的名称
         */
        String pkeyField;

        public Table(DbFile dbFile,String name,String pkeyField){
            this.dbFile=dbFile;
            this.name=name;
            this.pkeyField=pkeyField;
        }

        @Override
        public String toString(){
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("DbFile: ").append(dbFile)
                    .append("Name: ").append(name).
                    append("PkeyField: ").append(pkeyField);
            return stringBuilder.toString();
        }
    }

    /**
     * 管理所有的表文件的哈希表
     * tableId -->对应的table
     * 用于存储 tableId 和 表记录的映射
     *
     * 注意表id 等价于 文件id
     * 由文件绝对路径生成  一张表对应一个文件
     */
    ConcurrentHashMap<Integer,Table> hashTable;
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        hashTable=new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        hashTable.put(file.getId(),new Table(file,name,pkeyField));
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
        // some code goes here
        // 遍历
        Integer res=null;
        for(Integer key:hashTable.keySet()){
            if(hashTable.get(key).name.equals(name)){
                res=key;
                break;
            }
        }
        if(res != null){
            return res;
        }
        throw new NoSuchElementException("not found id for table " + name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if(table!=null){
            return table.dbFile.getTupleDesc();
        }
        throw new NoSuchElementException("not found TupleDesc for table:"+tableid);
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if(table!=null){
            return table.dbFile;
        }
        throw new NoSuchElementException("not found DatabaseFile for table:"+tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if(table!=null){
            return table.pkeyField;
        }
        throw new NoSuchElementException("not found PrimaryKey for table:"+tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table table = hashTable.getOrDefault(id, null);
        if(table!=null){
            return table.name;
        }
        throw new NoSuchElementException("not found name for table:"+id);
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
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

