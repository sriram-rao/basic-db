#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"
#include "ix.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator
#define TABLE_FILE_NAME "Tables"
#define COLUMN_FILE_NAME "Columns"
#define INDEX_FILE_NAME "Indexes"

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();

        RBFM_ScanIterator rbfmScanner;

        std::string tableName;
        std::string conditionAttribute;
        CompOp compOp;
        void *value;
        std::vector<std::string> attributeNames;
        vector<Attribute> descriptor;
        unsigned pageNum;
        short slotNum;
    };

    typedef int (RecordBasedFileManager::*operateRecord)(FileHandle &handle, const vector<Attribute> &recordDescriptor,
            void *data, RID &rid);

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan

        IX_ScanIterator ixScanner;
        IXFileHandle ixHandle;
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, bool allowSystemTables);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);


        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    private:
        static std::vector<Attribute> getTablesDescriptor();
        static std::vector<Attribute> getColumnsDescriptor();
        static std::vector<Attribute> getIndexesDescriptor();
        static void getStaticTableRecord(int id, const string& name, const string& fileName, char* data);
        static void getTableRecord(int id, const string& name, const string& fileName, int tableType, char* data);
        static void getStaticColumnRecord(int id, const Attribute &attribute, int position, char* data);
        static void getColumnRecord(int id, const Attribute &attribute, int position, int columnFlag, char* data);
        static void getIndexRecord(int tableId, const string &columnName, const string &filename, char *data);
        static vector<string> getAttributeSchema();
        static Attribute parseColumnAttribute(char* data);
        static void copyData(void* data, void* newData, int& copiedLength, int newLength);
        int getTableId(const string &tableName, RID &tableRid);
        void makeTableIdFilter(int tableId, char* tableFilter);
        int getMaxTableId();
        int operateTuple(const string &tableName, void *data, RID &rid, operateRecord);

        static const int TABLE_RECORD_MAX_SIZE = 208;
        static const int SYSTEM_TABLE_TYPE = 1;
        static const int COLUMN_RECORD_MAX_SIZE = 70;
        static const int SYSTEM_COLUMN_TYPE = 1;
        static const int INDEX_RECORD_MAX_SIZE = 112;

        vector<string> getIndexFiles(const string &tableName, vector<RID> &indexRids, int tableId = -1);
        string getIndexFileName(const string &tableName, const string &columnName);

        void removeFromIndex(const string &tableName, const RID &rid);

        void addToIndex(const string &tableName, const RID &rid, const void *data);

        Attribute getAttribute(const string &tableName, const string &columnName);
    };

} // namespace PeterDB

#endif // _rm_h_