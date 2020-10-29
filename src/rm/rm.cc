#include "src/include/rm.h"

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
        // Create file tables
        // Create file columns
        // Add tables and columns records into tables
        // Add columns of "tables" and "columns" into columns
        return -1;
    }

    RC RelationManager::deleteCatalog() {
        // Delete tables and columns files
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        // Insert record into "tables"
        // Insert columns into "colums"
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        // Remove columns
        // Remove table
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        // Fetch from columns
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.insertRecord
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.deleteRecord
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.updateRecord
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.readRecord
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        // Get descriptor from columns
        // Call RBFM.printRecord
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.readAttribute
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        // Get file name from "tables"
        // Get descriptor from columns
        // Open file and get the FileHandle
        // Call RBFM.scan
        // Keep in RM scan iterator
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
    {
        // Call RBFM scanner's get next
        return RM_EOF;
    }

    RC RM_ScanIterator::close()
    {
        // Call RBFM scanner's close
        return -1;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB