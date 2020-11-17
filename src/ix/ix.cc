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
        Node root(NODE_TYPE_LEAF);
        char *bytes = (char *) malloc(PAGE_SIZE);
        int rootPageId = ixFileHandle.getRootPageId();
        if (rootPageId == -1) {
            ixFileHandle.setRootPageId(ixFileHandle.getPageCount() + 1, true);
            root.populateBytes(bytes);
            rootPageId = ixFileHandle.appendPage(bytes);
        }
        free(bytes);

        insert(ixFileHandle, rootPageId, attribute, key, rid, nullptr);
        return 0;
    }

    void IndexManager::insert(IXFileHandle &ixFileHandle, int nodePageId, const Attribute &attribute, const void *key, const RID &rid, InsertionChild *newChild) {
        char *bytes = (char *) malloc(PAGE_SIZE);
        ixFileHandle.readPage(nodePageId, bytes);
        Node currentNode(bytes);

        // If not a leaf node
        if (NODE_TYPE_INTERMEDIATE == currentNode.type) {
            int newChildId = currentNode.findChildNode(attribute, key, rid);
            free(bytes);
            insert(ixFileHandle, newChildId, attribute, key, rid, newChild);
            if (nullptr == newChild)
                return;

            // Handle if there is new child
            int spaceNeeded = newChild->keyLength;
            if (currentNode.hasSpace(spaceNeeded)) {
                // insert child
                return;
            }
            // Find index of new child
            // split node, second half go to new node
            // insert new child in correct node
            // setup newChild to form the new child here

            // this is the root, create a new root (increase tree height)
            // put the children
            // write new root page ID to disk

            return;
        }

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

        // Else split leaf node, set up newChild

        free(bytes);
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char *bytes = (char *) malloc(PAGE_SIZE);
        int rootId = ixFileHandle.getRootPageId();
        ixFileHandle.readPage(rootId, bytes);
        Node currentNode(bytes);

        // Find the leaf node with this key/rid
        int keyPageId = rootId;
        while (NODE_TYPE_LEAF != currentNode.type){
            keyPageId = currentNode.findChildNode(attribute, key, rid);
            ixFileHandle.readPage(keyPageId, bytes);
            currentNode = Node(bytes);
        }
        int indexToDelete = currentNode.findKey(attribute, key, rid);
        if (-1 == indexToDelete)
            return -1; // Key not found

        currentNode.deleteKey(attribute, indexToDelete, key, rid);
        currentNode.populateBytes(bytes);
        ixFileHandle.writePage(keyPageId, bytes);
        free(bytes);
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (!ixFileHandle.works())
            return -1;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKey = const_cast<void *>(lowKey);
        ix_ScanIterator.highKey = const_cast<void *>(highKey);
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.ixFileHandle = &ixFileHandle;
        ix_ScanIterator.pageNum = ixFileHandle.getRootPageId();
        ix_ScanIterator.slotNum = 0;
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        char bytes[PAGE_SIZE];
        ixFileHandle.readPage(ixFileHandle.getRootPageId(), bytes);
        Node rootNode(bytes);
        out << rootNode.toJsonString(attribute);
        return 0;
    }
} // namespace PeterDB