#include "src/include/rm.h"
#include "src/utils/copy_utils.h"
#include <vector>
#include <string>
#include <src/include/ix.h>

using namespace std;

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.createFile(TABLE_FILE_NAME);
        recordManager.createFile(COLUMN_FILE_NAME);
        recordManager.createFile(INDEX_FILE_NAME);

        // Add tables and columns records into tables
        vector<Attribute> tableDescriptor = getTablesDescriptor();
        FileHandle handle;
        recordManager.openFile(TABLE_FILE_NAME, handle);
        char* data = (char*) malloc(TABLE_RECORD_MAX_SIZE);
        getStaticTableRecord(1, "Tables", TABLE_FILE_NAME, data);
        RID rid;
        recordManager.insertRecord(handle, tableDescriptor, data, rid);
        getStaticTableRecord(2, "Columns", COLUMN_FILE_NAME, data);
        recordManager.insertRecord(handle, tableDescriptor, data, rid);
        getStaticTableRecord(3, "Indexes", INDEX_FILE_NAME, data);
        recordManager.insertRecord(handle, tableDescriptor, data, rid);
        recordManager.closeFile(handle);
        handle = FileHandle();

        // Add columns of "tables" and "columns" into columns
        vector<Attribute> columnsDescriptor = getColumnsDescriptor();
        recordManager.openFile(COLUMN_FILE_NAME, handle);
        int position = 1;
        for (auto & attribute : tableDescriptor) {
            getStaticColumnRecord(1, attribute, position, data);
            recordManager.insertRecord(handle, columnsDescriptor, data, rid);
            position++;
        }

        position = 1;
        for (auto & attribute : columnsDescriptor) {
            getStaticColumnRecord(2, attribute, position, data);
            recordManager.insertRecord(handle, columnsDescriptor, data, rid);
            position++;
        }

        position = 1;
        for (auto & attribute : getIndexesDescriptor()) {
            getStaticColumnRecord(3, attribute, position, data);
            recordManager.insertRecord(handle, columnsDescriptor, data, rid);
        }
        free(data);
        return recordManager.closeFile(handle);
    }

    RC RelationManager::deleteCatalog() {
        // Delete tables and columns files
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.destroyFile(COLUMN_FILE_NAME);
        recordManager.destroyFile(TABLE_FILE_NAME);
        recordManager.destroyFile(INDEX_FILE_NAME);
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        int maxId = getMaxTableId();
        if (maxId == -1)
            return -1; // No catalog, return error

        // Insert record into "tables"
        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(TABLE_FILE_NAME, handle);
        char* data = (char *) malloc(TABLE_RECORD_MAX_SIZE);
        getTableRecord(maxId + 1, tableName, tableName, 0, data);
        RID rid;
        recordManager.insertRecord(handle, getTablesDescriptor(), data, rid);
        recordManager.closeFile(handle);
        handle = FileHandle();

        // Insert columns into "columns"
        recordManager.openFile(COLUMN_FILE_NAME, handle);
        int position = 0;
        for (auto & attribute : attrs) {
            getColumnRecord(maxId + 1, attribute, position, 0, data);
            recordManager.insertRecord(handle, getColumnsDescriptor(), data, rid);
            position++;
        }
        free(data);
        recordManager.closeFile(handle);

        return recordManager.createFile(tableName);
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        // Get table ID
        FileHandle handle; RID rid;
        int tableId = getTableId(tableName, rid);

        if (tableId <= 2)
            return -1;

        // Remove table
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(TABLE_FILE_NAME, handle);
        recordManager.deleteRecord(handle, getTablesDescriptor(), rid);
        recordManager.closeFile(handle);
        handle = FileHandle();

        // Get the columns RIDs for this table
        char *columnTableId = (char *) malloc(sizeof(tableId) + 1);
        vector<Attribute> columnsDescriptor = getColumnsDescriptor();

        char *tableFilter = (char *)malloc(sizeof(tableId));
        makeTableIdFilter(tableId, tableFilter);

        recordManager.openFile(COLUMN_FILE_NAME, handle);
        vector<RID> columnsToDelete;
        RBFM_ScanIterator rbfmScanner;
        recordManager.scan(handle, columnsDescriptor, "table-id", EQ_OP, tableFilter, vector<string>(1, "table-id"), rbfmScanner);
        while (rbfmScanner.getNextRecord(rid, columnTableId) != RBFM_EOF)
            columnsToDelete.push_back(rid);
        rbfmScanner.close();
        recordManager.closeFile(handle);

        handle = FileHandle();
        recordManager.openFile(COLUMN_FILE_NAME, handle);
        // Delete columns
        for (RID colRid : columnsToDelete)
            recordManager.deleteRecord(handle, columnsDescriptor, colRid);
        recordManager.closeFile(handle);

        // Delete indexes
        std::vector<Attribute> indexesDescriptor = getIndexesDescriptor();
        handle = FileHandle();
        std::vector<RID> indexRids;
        std::vector<std::string> indexFiles = getIndexFiles(tableName, indexRids, tableId);
        recordManager.openFile(INDEX_FILE_NAME, handle);
        for (int i = 0; i < indexRids.size(); ++i) {
            IndexManager::instance().destroyFile(indexFiles.at(i));
            recordManager.deleteRecord(handle, indexesDescriptor, indexRids.at(i));
        }
        recordManager.closeFile(handle);

        free(columnTableId);
        free(tableFilter);

        // Delete file
        return recordManager.destroyFile(tableName);
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return getAttributes(tableName, attrs, true);
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, bool allowSystemTables) {
        RID tableRid;
        int tableId = getTableId(tableName, tableRid);
        if (tableId == -1 || (tableId <= 2 && !allowSystemTables))
            return -1;

        // Fetch attributes from columns
        FileHandle handle;
        char *tableFilter = (char *)malloc(sizeof(tableId));
        makeTableIdFilter(tableId, tableFilter);
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(COLUMN_FILE_NAME, handle);
        RBFM_ScanIterator scanner;
        recordManager.scan(handle, getColumnsDescriptor(), "table-id", EQ_OP, tableFilter, getAttributeSchema(), scanner);

        RID rid;
        char* data = (char*) malloc(COLUMN_RECORD_MAX_SIZE);
        while (scanner.getNextRecord(rid, data) != RBFM_EOF) {
            Attribute column = parseColumnAttribute(data);
            attrs.push_back(column);
        }
        scanner.close();
        recordManager.closeFile(handle);
        free(tableFilter);
        free(data);

        return attrs.empty() ? -1 : 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        vector<Attribute> tupleDescriptor;
        if (getAttributes(tableName, tupleDescriptor, false) == -1)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
        int insertSuccess = recordManager.insertRecord(handle, tupleDescriptor, data, rid);
        recordManager.closeFile(handle);
        addToIndex(tableName, rid, data);

        return insertSuccess;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RID tableRid;
        if (getTableId(tableName, tableRid) <= 2)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);

        removeFromIndex(tableName, rid);
        int deleteSuccess = recordManager.deleteRecord(handle, vector<Attribute>(0), rid);
        recordManager.closeFile(handle);

        return deleteSuccess;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        vector<Attribute> tupleDescriptor;
        if (getAttributes(tableName, tupleDescriptor, false) == -1)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
        removeFromIndex(tableName, rid);

        int updateSuccess = recordManager.updateRecord(handle, tupleDescriptor, data, rid);
        recordManager.closeFile(handle);

        addToIndex(tableName, rid, data);

        return updateSuccess;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        vector<Attribute> tupleDescriptor;
        if (getAttributes(tableName, tupleDescriptor, true) == -1)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
        RC readSuccess = recordManager.readRecord(handle, tupleDescriptor, rid, data);
        recordManager.closeFile(handle);
        return readSuccess;
    }

    RC RelationManager::operateTuple(const string &tableName, void *data, RID &rid, operateRecord operate) {
        vector<Attribute> tupleDescriptor;
        if (getAttributes(tableName, tupleDescriptor, false) == -1)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
        int operationSuccess = (recordManager.*operate)(handle, tupleDescriptor, data, rid);
        recordManager.closeFile(handle);
        return operationSuccess;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return RecordBasedFileManager::instance().printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        vector<Attribute> tupleDescriptor;
        if (getAttributes(tableName, tupleDescriptor) == -1)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
        int operationSuccess = recordManager.readAttribute(handle, tupleDescriptor, rid, attributeName, data);
        recordManager.closeFile(handle);
        return operationSuccess;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        vector<Attribute> tableSchema;
        if (getAttributes(tableName, tableSchema) == -1)
            return -1;

        rm_ScanIterator.tableName = tableName;
        rm_ScanIterator.conditionAttribute = conditionAttribute;
        rm_ScanIterator.compOp = compOp;
        rm_ScanIterator.value = const_cast<void *>(value);
        rm_ScanIterator.attributeNames = attributeNames;
        rm_ScanIterator.descriptor = tableSchema;
        rm_ScanIterator.pageNum = 0;
        rm_ScanIterator.slotNum = -1;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
    {
        FileHandle handle;
        RecordBasedFileManager::instance().openFile(tableName, handle);
        RBFM_ScanIterator rbfmScanner;
        RecordBasedFileManager::instance().scan(handle, descriptor, conditionAttribute, compOp, value, attributeNames, rbfmScanner);
        rbfmScanner.pageNum = pageNum;
        rbfmScanner.slotNum = slotNum;
        int end = rbfmScanner.getNextRecord(rid, data);
        rbfmScanner.close();
        RecordBasedFileManager::instance().closeFile(handle);
        pageNum = rid.pageNum;
        slotNum = rid.slotNum;
        return end == RBFM_EOF ? RM_EOF : end;
    }

    RC RM_ScanIterator::close()
    {
        return 0;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        // Get table ID
        RID tableRid;
        int tableId = getTableId(tableName, tableRid);
        if (tableId <= 2) // -1 or attempting to create index on system table
            return -1;

        // insert in index table
        char* data = (char*) malloc(INDEX_RECORD_MAX_SIZE);
        string filename = getIndexFileName(tableName, attributeName);
        getIndexRecord(tableId, attributeName, filename, data);
        RID rid;
        FileHandle rbfmHandle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(INDEX_FILE_NAME, rbfmHandle);
        int insertSuccess = recordManager.insertRecord(rbfmHandle, getIndexesDescriptor(), data, rid);
        recordManager.closeFile(rbfmHandle);
        rbfmHandle = FileHandle();

        IndexManager &ixManager = IndexManager::instance();
        ixManager.createFile(filename);
        IXFileHandle ixHandle;
        ixManager.openFile(filename, ixHandle);

        // Scan records and insert
        RBFM_ScanIterator rbfmScanner;
        Attribute attribute;
        std::vector<Attribute> tuplesDescriptor;
        getAttributes(tableName, tuplesDescriptor);
        int dataSize = 1;
        for (Attribute & attr : tuplesDescriptor) {
            if (attributeName != attr.name) {
                continue;
            }
            attribute = attr;
            dataSize += attr.length;
            if (TypeVarChar == attr.type) {
                dataSize += sizeof(int);
            }
            break;
        }
        recordManager.openFile(tableName, rbfmHandle);
        recordManager.scan(rbfmHandle, tuplesDescriptor, "", EQ_OP, nullptr,
                           std::vector<std::string>(1, attributeName), rbfmScanner);
        char recordData [dataSize];
        while(rbfmScanner.getNextRecord(rid, recordData) != RBFM_EOF) {
            if (((char *)recordData)[0] & (1 << 7))
                continue; // ignore null records
            char indexData [dataSize - 1];
            std::memcpy(indexData, recordData + 1, dataSize - 1);
            ixManager.insertEntry(ixHandle, attribute, indexData, rid);
        }

        rbfmScanner.close();
        ixManager.closeFile(ixHandle);
        recordManager.closeFile(rbfmHandle);

        return insertSuccess;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        int result = -1;
        std::vector<RID> indexRids;
        string fileToDelete = getIndexFileName(tableName, attributeName);
        std::vector<std::string> tableIndexes = getIndexFiles(tableName, indexRids);
        for (int i = 0; i < tableIndexes.size(); ++i) {
            string filename = tableIndexes.at(i);
            if (fileToDelete != filename)
                continue;
            FileHandle handle;
            RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
            recordManager.openFile(INDEX_FILE_NAME, handle);
            recordManager.deleteRecord(handle, getIndexesDescriptor(), indexRids.at(i));
            recordManager.closeFile(handle);
            result = IndexManager::instance().destroyFile(fileToDelete);
            break;
        }
        return result;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator) {
        IndexManager &ixManager = IndexManager::instance();
        string ixFile = getIndexFileName(tableName, attributeName);
        Attribute attribute = getAttribute(tableName, attributeName);
        if (attribute.name.empty())
            return -1;
        ixManager.openFile(ixFile, rm_IndexScanIterator.ixHandle);
        return ixManager.scan(rm_IndexScanIterator.ixHandle, attribute, lowKey, highKey,
                                                        lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixScanner);
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }


    vector<Attribute> RelationManager::getTablesDescriptor() {
        vector<Attribute> descriptor;
        descriptor.push_back({ "table-id", TypeInt, 4 });
        descriptor.push_back({ "table-name", TypeVarChar, 50 });
        descriptor.push_back({ "file-name", TypeVarChar, 50 });
        return descriptor;
    }

    vector<Attribute> RelationManager::getColumnsDescriptor() {
        vector<Attribute> descriptor;
        descriptor.push_back({ "table-id", TypeInt, 4 });
        descriptor.push_back({ "column-name", TypeVarChar, 50 });
        descriptor.push_back({ "column-type", TypeInt, 4 });
        descriptor.push_back({ "column-length", TypeInt, 4 });
        descriptor.push_back({ "column-position", TypeInt, 4 });
        return descriptor;
    }

    vector<Attribute> RelationManager::getIndexesDescriptor() {
        vector<Attribute> descriptor;
        descriptor.push_back({ "table-id", TypeInt, 4 });
        descriptor.push_back({ "column-name", TypeVarChar, 50 });
        descriptor.push_back({ "file-name", TypeVarChar, 50 });
        return descriptor;
    }

    vector<string> RelationManager::getAttributeSchema() {
        vector<string> schema;
        schema.emplace_back("column-name");
        schema.emplace_back("column-type");
        schema.emplace_back("column-length");
        return schema;
    }

    Attribute RelationManager::parseColumnAttribute(char* data) {
        int nameLength = 0;
        int copiedLength = 1; // Skip null byte since we should not have nulls in Columns table anyway
        CopyUtils::copyAttribute(data, &nameLength, copiedLength, sizeof(nameLength));
        char name [nameLength];
        CopyUtils::copyAttribute(data, name, copiedLength, nameLength);
        int type, length, columnFlag;
        CopyUtils::copyAttribute(data, &type, copiedLength, sizeof(type));
        CopyUtils::copyAttribute(data, &length, copiedLength, sizeof(length));
        CopyUtils::copyAttribute(data, &columnFlag, copiedLength, sizeof(columnFlag));

        Attribute column;
        column.name = string (name);
        column.type = static_cast<AttrType>(type);
        column.length = length;
        return column;
    }

    void RelationManager::getStaticTableRecord(int id, const string& name, const string& fileName, char* data) {
        getTableRecord(id, name, fileName, SYSTEM_TABLE_TYPE, data); // 1 table type means system table
    }

    void RelationManager::getTableRecord(int id, const string &name, const string &fileName, int tableType, char* data) {
        int copiedLength = 0;
        char nullMap = 0;
        int length = name.length();
        memset(&nullMap, 0, 1);
        copyData(data, &nullMap, copiedLength, 1);
        copyData(data, &id, copiedLength, sizeof(id));
        copyData(data, &length, copiedLength, sizeof(length));
        copyData(data, (char*)name.c_str(), copiedLength, length);
        length = fileName.length();
        copyData(data, &length, copiedLength, sizeof(length));
        copyData(data, (char*)fileName.c_str(), copiedLength, length);
    }

    void RelationManager::getStaticColumnRecord(int id, const Attribute &attribute, int position, char* data) {
        getColumnRecord(id, attribute, position, 0, data); // 1 column flag type means system column
    }

    void RelationManager::getColumnRecord(int id, const Attribute &attribute, int position, int columnFlag, char* data) {
        int copiedLength = 0;
        char nullMap = 0;
        int length = attribute.name.length() + 1;
        int columnType = attribute.type;
        memset(&nullMap, 0, 1);
        copyData(data, &nullMap, copiedLength, 1);
        copyData(data, &id, copiedLength, sizeof(id));

        copyData(data, &length, copiedLength, sizeof(length));
        copyData(data, (char*)attribute.name.c_str(), copiedLength, length);

        int attributeLength = attribute.length;
        copyData(data, &columnType, copiedLength, sizeof(columnType));
        copyData(data, &attributeLength, copiedLength, sizeof(attribute.length));

        copyData(data, &position, copiedLength, sizeof(position));
    }

    void RelationManager::getIndexRecord(int tableId, const string &columnName, const string &filename, char *data) {
        int copiedLength = 0;
        char nullMap = 0;
        int columnNameLength = columnName.length();
        int filenameLength = filename.length();
        std::memset(&nullMap, 0, 1);

        copyData(data, &nullMap, copiedLength, 1);
        copyData(data, &tableId, copiedLength, sizeof(tableId));
        copyData(data, &columnNameLength, copiedLength, sizeof(columnNameLength));
        copyData(data, (char *)columnName.c_str(), copiedLength, columnNameLength);
        copyData(data, &filenameLength, copiedLength, sizeof(filenameLength));
        copyData(data, (char *)filename.c_str(), copiedLength, filenameLength);
    }

    void RelationManager::copyData(void* data, void* newData, int& copiedLength, int newLength) {
        memcpy((char*)data + copiedLength, newData, newLength);
        copiedLength += newLength;
    }

    int RelationManager::getTableId(const string &tableName, RID &tableRid) {
        FileHandle handle;
        vector<Attribute> tablesDescriptor = getTablesDescriptor();
        RBFM_ScanIterator rbfmScanner;
        char *tableFilter = (char *)malloc(tableName.size() + sizeof(int));
        int tableNameLength = tableName.size();
        memcpy(tableFilter, &tableNameLength, sizeof(tableNameLength));
        memcpy(tableFilter + sizeof(tableNameLength), tableName.c_str(), tableNameLength);
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(TABLE_FILE_NAME, handle);
        recordManager.scan(handle, tablesDescriptor, "table-name", EQ_OP, tableFilter, vector<string>(1, "table-id"), rbfmScanner);
        char *tableIdData = (char *)malloc(sizeof(int) + 1);
        int tableId = -1;
        RID rid;
        while(rbfmScanner.getNextRecord(rid, tableIdData) != RBFM_EOF) {
            tableRid = rid;
            memcpy(&tableId, tableIdData + 1, sizeof(tableId));
            break;
        }

        rbfmScanner.close();
        recordManager.closeFile(handle);
        free(tableIdData);
        free(tableFilter);
        return tableId;
    }

    std::vector<std::string> RelationManager::getIndexFiles(const string &tableName, std::vector<RID> &indexRids, int tableId) {
        std::vector<std::string> filenames;
        FileHandle handle;
        vector<Attribute> descriptor = getIndexesDescriptor();
        RBFM_ScanIterator rbfmScanner;
        RID tableRid;
        int tableFilter = tableId > 2 ? tableId : getTableId(tableName, tableRid);
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(INDEX_FILE_NAME, handle);
        recordManager.scan(handle, descriptor, "table-id", EQ_OP, &tableFilter,
                           std::vector<std::string>(1, "file-name"), rbfmScanner);
        char indexData [50 + sizeof(int)];
        int filenameLength = -1;
        string filename;
        RID rid;
        while(rbfmScanner.getNextRecord(rid, indexData) != RBFM_EOF) {
            indexRids.push_back(rid);
            std::memcpy(&filenameLength, indexData + 1, sizeof(filenameLength));
            char filenameBytes [filenameLength + 1];
            std::memcpy(filenameBytes, indexData + 1 + sizeof(filenameLength), filenameLength);
            filenameBytes[filenameLength] = '\0';
            filename = string (filenameBytes);
            filenames.push_back(filename);
        }

        rbfmScanner.close();
        recordManager.closeFile(handle);

        return filenames;
    }

    void RelationManager::makeTableIdFilter(int tableId, char* tableFilter) {
        memcpy(tableFilter, &tableId, sizeof(tableId));
    }

    int RelationManager::getMaxTableId() {
        FileHandle handle;
        vector<Attribute> tablesDescriptor = getTablesDescriptor();
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        if (recordManager.openFile(TABLE_FILE_NAME, handle) == -1)
            return -1;

        RBFM_ScanIterator rbfmScanner;
        recordManager.scan(handle, tablesDescriptor, "", EQ_OP, nullptr, vector<string>(1, "table-id"), rbfmScanner);
        int maxId = -1;
        char *tableId = (char *)malloc(sizeof(int) + 1);
        RID rid;
        while(rbfmScanner.getNextRecord(rid, tableId) != RBFM_EOF) {
            int currentId = -1;
            memcpy(&currentId, tableId + 1, sizeof(currentId));
            if (maxId < currentId)
                maxId = currentId;
        }
        rbfmScanner.close();
        recordManager.closeFile(handle);

        free(tableId);
        return maxId;
    }

    void RelationManager::removeFromIndex(const std::string &tableName, const RID &rid) {
        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        std::vector<Attribute> tupleDescriptor;
        getAttributes(tableName, tupleDescriptor, false);
        IndexManager &ixManager = IndexManager::instance();
        vector<RID> indexRids;
        std::vector<std::string> indexFiles = getIndexFiles(tableName, indexRids);
        // Remove from index if present
        for (int i = 0; i < tupleDescriptor.size(); ++i) {
            Attribute attribute = tupleDescriptor.at(i);
            string currentColumnIndex = getIndexFileName(tableName, attribute.name);

            for (const std::string& index : indexFiles){
                if (currentColumnIndex != index)
                    continue;

                int keyReadSize = 1 + attribute.length;
                if (TypeVarChar == attribute.type) {
                    keyReadSize += sizeof(int);
                }
                char columnValue [keyReadSize];
                recordManager.readAttribute(handle, tupleDescriptor, rid, attribute.name, columnValue);
                if ((columnValue)[0] & (1 << 7))
                    continue;

                // Get attribute value
                char key [keyReadSize - 1];
                std::memcpy(key, columnValue + 1, keyReadSize - 1);
                IXFileHandle ixHandle;
                ixManager.openFile(index, ixHandle);
                ixManager.deleteEntry(ixHandle, attribute, key, rid);
                ixManager.closeFile(ixHandle);
            }
        }
    }

    void RelationManager::addToIndex(const string &tableName, const RID &rid, const void *data) {
        std::vector<Attribute> tupleDescriptor;
        getAttributes(tableName, tupleDescriptor);

        IndexManager &ixManager = IndexManager::instance();
        vector<RID> indexRids;
        std::vector<std::string> indexFiles = getIndexFiles(tableName, indexRids);
        // Add in index if present
        int seenLength = ceil(((float) tupleDescriptor.size()) / 8);
        for (int i = 0; i < tupleDescriptor.size(); ++i) {
            if (((char *) data)[i / 8] & (1 << (7 - i % 8))) {
                continue;
            }
            Attribute attribute = tupleDescriptor.at(i);
            string currentColumnIndex = getIndexFileName(tableName, attribute.name);

            for (const std::string& index : indexFiles){
                int fieldLength = attribute.length;
                int keySize = fieldLength;
                if (TypeVarChar == attribute.type) {
                    std::memcpy(&fieldLength, (char *) data + seenLength, sizeof(int));
                    keySize += sizeof(int);
                    seenLength += sizeof(int);
                }
                if (currentColumnIndex != index) {
                    seenLength += fieldLength;
                    continue;
                }
                // Get attribute value
                char key [keySize];
                int copiedLength = 0;
                if (TypeVarChar == attribute.type) {
                    std::memcpy(key, &fieldLength, sizeof(fieldLength));
                    copiedLength += sizeof(fieldLength);
                }
                IXFileHandle ixHandle;
                std::memcpy(key + copiedLength, (char *)data + seenLength, fieldLength);
                ixManager.openFile(index, ixHandle);
                ixManager.insertEntry(ixHandle, attribute, key, rid);
                ixManager.closeFile(ixHandle);
            }
        }
    }

    std::string RelationManager::getIndexFileName(const string &tableName, const string &columnName) {
        return tableName + "_" + columnName + ".idx";
    }

    Attribute RelationManager::getAttribute(const string &tableName, const string &columnName) {
        std::vector<Attribute> columns;
        getAttributes(tableName, columns);
        for (Attribute column : columns)
            if (columnName == column.name)
                return column;
        return PeterDB::Attribute();
    }

} // namespace PeterDB