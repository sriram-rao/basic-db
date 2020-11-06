#include "src/include/rm.h"
#include "src/utils/copy_utils.h"
#include <vector>
#include <string>

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
        free(data);
        return recordManager.closeFile(handle);
    }

    RC RelationManager::deleteCatalog() {
        // Delete tables and columns files
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.destroyFile(COLUMN_FILE_NAME);
        recordManager.destroyFile(TABLE_FILE_NAME);
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
        return insertSuccess;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RID tableRid;
        if (getTableId(tableName, tableRid) <= 2)
            return -1;

        FileHandle handle;
        RecordBasedFileManager &recordManager = RecordBasedFileManager::instance();
        recordManager.openFile(tableName, handle);
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
        int updateSuccess = recordManager.updateRecord(handle, tupleDescriptor, data, rid);
        recordManager.closeFile(handle);
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

} // namespace PeterDB