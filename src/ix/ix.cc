#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        IXFileHandle ixFileHandle;
        if (ixFileHandle.create(fileName) != 0)
            return -1;
        Node root(NODE_TYPE_LEAF);
        char *bytes = (char *) malloc(PAGE_SIZE);
        root.populateBytes(bytes);
        ixFileHandle.open(fileName);
        ixFileHandle.rootPage = ixFileHandle.appendPage(bytes);
        return ixFileHandle.close();
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return remove(fileName.c_str());
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return ixFileHandle.open(fileName);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return ixFileHandle.close();
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // Start from root node
        insert(ixFileHandle, ixFileHandle.rootPage, attribute, key, rid, nullptr);

        return 0;
    }

    void IndexManager::insert(IXFileHandle &ixFileHandle, int nodePageId, const Attribute &attribute, const void *key, const RID &rid, InsertionChild *newChild) {
        char *bytes = (char *) malloc(PAGE_SIZE);
        ixFileHandle.readPage(nodePageId, bytes);
        Node currentNode(bytes);

        // If not a leaf node
        if (NODE_TYPE_INTERMEDIATE == currentNode.type) {

        }
        // Find the right sub tree
        // Recursively call
        // Handle if there is new child

        // If leaf node
        // If node has space, insert in the right place
        int keySize = 4;
        if (TypeVarChar == attribute.type)
            std::memcpy(&keySize, key, sizeof(int));
        int spaceNeeded = keySize + sizeof(unsigned) + sizeof(unsigned short); // key size + rid

        if (currentNode.hasSpace(spaceNeeded)) {
            currentNode.insertKey(attribute, spaceNeeded, key, rid);
            currentNode.populateBytes(bytes);
            ixFileHandle.writePage(nodePageId, bytes);
            free(bytes);
            return;
        }
        // Else split leaf node
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        char bytes[PAGE_SIZE];
        ixFileHandle.readPage(ixFileHandle.rootPage, bytes);
        Node rootNode(bytes);
        out << rootNode.toJsonString(attribute);
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }
} // namespace PeterDB