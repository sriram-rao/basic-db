#include "src/include/ix.h"
#include <src/utils/compare_utils.h>

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
        this->cachedPage = -1;
        return remove(fileName.c_str());
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        this->cachedFile = ixFileHandle.filename;
        this->cachedPage = -1;
        return ixFileHandle.open(fileName);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        this->cachedPage = -1;
        return ixFileHandle.close();
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int rootPageId = ixFileHandle.getRootPageId();
        if (rootPageId == -1) {
            ixFileHandle.setRootPageId(ixFileHandle.getPageCount() + 1, true);
            Node root(NODE_TYPE_LEAF);
            char *bytes = (char *) malloc(PAGE_SIZE); // Move inside if block
            root.populateBytes(bytes);
            rootPageId = ixFileHandle.appendPage(bytes);
            free(bytes);
        }

        InsertionChild newChild{};
        newChild.leastChildValue = malloc(attribute.length + sizeof(RID::pageNum) + sizeof(RID::slotNum) + sizeof(int));
        newChild.newChildPresent = false;
        insert(ixFileHandle, rootPageId, attribute, key, rid, &newChild);
        free(newChild.leastChildValue);
        return 0;
    }

    void IndexManager::insert(IXFileHandle &ixFileHandle, int nodePageId, const Attribute &attribute, const void *key, const RID &rid, InsertionChild *newChild) {
        char *bytes = (char *) malloc(PAGE_SIZE);
        ixFileHandle.readPage(nodePageId, bytes);
        Node currentNode(bytes);

        // If not a leaf node
        if (NODE_TYPE_INTERMEDIATE == currentNode.type) {
            int childIndex;
            int childId = currentNode.findChildNode(attribute, key, rid.pageNum, rid.slotNum, childIndex);
            free(bytes);
            insert(ixFileHandle, childId, attribute, key, rid, newChild);
            if (!newChild->newChildPresent) {
                return;
            }

            bytes = (char *) malloc(PAGE_SIZE);
            int spaceNeeded = newChild->keyLength;
            if (currentNode.hasSpace(spaceNeeded)) {
                currentNode.insertChild(attribute, childIndex, newChild->leastChildValue, newChild->keyLength, newChild->childNodePage);
                currentNode.populateBytes(bytes);
                ixFileHandle.writePage(nodePageId, bytes);
                newChild->newChildPresent = false;
                free(bytes);
                return;
            }

            char *newNode = (char *) malloc(PAGE_SIZE);
            InsertionChild splitNode{};
            splitNode.leastChildValue = malloc(attribute.length + sizeof(RID::pageNum) + sizeof(RID::slotNum) + sizeof(int));

            int splitStart;
            currentNode.split(newNode, &splitNode, splitStart, childIndex);

            if (childIndex < splitStart) {
                // add in old node
                currentNode.insertChild(attribute, childIndex, newChild->leastChildValue, newChild->keyLength, newChild->childNodePage);
            } else {
                // add in split node
                Node split (newNode);
                if (childIndex == splitStart) {
                    split.insertChild(attribute, 0, splitNode.leastChildValue, splitNode.keyLength, split.nextPage);
                    split.nextPage = newChild->childNodePage;
                    free(splitNode.leastChildValue);
                    splitNode.leastChildValue = malloc(newChild->keyLength);
                    std::memcpy(splitNode.leastChildValue, newChild->leastChildValue, newChild->keyLength);
                    splitNode.keyLength = newChild->keyLength;
                } else {
                    split.insertChild(attribute, childIndex - splitStart - 1, newChild->leastChildValue,
                                      newChild->keyLength, newChild->childNodePage);
                }
                split.populateBytes(newNode);
            }

            currentNode.populateBytes(bytes);
            ixFileHandle.writePage(nodePageId, bytes);
            int splitNodePage = ixFileHandle.appendPage(newNode);
            free(newNode);
            free(newChild->leastChildValue);
            newChild->leastChildValue = splitNode.leastChildValue;
            newChild->newChildPresent = true;
            newChild->childNodePage = splitNodePage; // page ID after writing splitNode;
            newChild->keyLength = splitNode.keyLength;

            if (nodePageId != ixFileHandle.getRootPageId()) {
                free(bytes);
                return;
            }
            // This is the root, create a new root (increase tree height)
            Node newRootNode(NODE_TYPE_INTERMEDIATE);
            // put the children
            newRootNode.nextPage = nodePageId;
            newRootNode.keys = (char *) malloc(PAGE_SIZE);
            newRootNode.insertChild(attribute, 0, newChild->leastChildValue, newChild->keyLength, newChild->childNodePage);
            newRootNode.populateBytes(bytes);
            // write new root page ID to disk
            int newRootId = ixFileHandle.appendPage(bytes);
            // write all nodes to disk, set the root pointer to new root.
            ixFileHandle.setRootPageId(newRootId);

            free(bytes);
            return;
        }

        // Leaf node
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

        // Split leaf node, set up newChild
        int splitStart;
        char *newLeaf = (char *) malloc(PAGE_SIZE);
        currentNode.split(newLeaf, newChild, splitStart, 0);

        char formattedChildKey [newChild->keyLength];
        RID childKeyId{};
        parseKey(attribute.type, newChild, formattedChildKey, childKeyId);

        if (CompareUtils::checkLessThan(attribute.type, key, formattedChildKey)) {
            currentNode.insertKey(attribute, spaceNeeded, key, rid);
        } else {
            Node newLeafNode (newLeaf);
            newLeafNode.insertKey(attribute, spaceNeeded, key, rid);
            newLeafNode.populateBytes(newLeaf);
        }

        int newPageId = ixFileHandle.appendPage(newLeaf);
        free(newLeaf);
        currentNode.nextPage = newPageId;
        currentNode.populateBytes(bytes);
        ixFileHandle.writePage(nodePageId, bytes);
        newChild->newChildPresent = true;
        newChild->childNodePage = newPageId;

        if (nodePageId == ixFileHandle.getRootPageId()) {
            newChild->newChildPresent = false;
            // if this is the root, make a new root and make children
            Node newRoot(NODE_TYPE_INTERMEDIATE);
            newRoot.nextPage = nodePageId;
            newRoot.keys = (char *) malloc(PAGE_SIZE);
            newRoot.insertChild(attribute, 0, newChild->leastChildValue, newChild->keyLength, newChild->childNodePage);
            // write all nodes to disk, set the root pointer to new root.
            newRoot.populateBytes(bytes);
            int newRootId = ixFileHandle.appendPage(bytes);
            ixFileHandle.setRootPageId(newRootId);
        }

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
            int location;
            keyPageId = currentNode.findChildNode(attribute, key, rid.pageNum, rid.slotNum, location);
            ixFileHandle.readPage(keyPageId, bytes);
            currentNode.reload(bytes);
        }
        int indexToDelete = currentNode.findKey(attribute, key, rid);
        if (-1 == indexToDelete) {
            free(bytes);
            return -1; // Key not found
        }

        currentNode.deleteKey(attribute, indexToDelete);
        currentNode.populateBytes(bytes);
        ixFileHandle.writePage(keyPageId, bytes);

        if (cached(ixFileHandle.filename, keyPageId))
            refreshCache(bytes, keyPageId);

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
        ix_ScanIterator.searching = true;
        this->cachedPage = -1;
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        out << getJson(ixFileHandle, attribute, ixFileHandle.getRootPageId());
        return 0;
    }

    std::string IndexManager::getJson(IXFileHandle &ixFileHandle, const Attribute &attribute, int pageId) const {
        string json = "{";

        char *bytes = (char *)malloc(PAGE_SIZE);
        ixFileHandle.readPage(pageId, bytes);
        Node currentNode(bytes);
        free(bytes);

        json.append(currentNode.toJsonKeys(attribute));

        if (NODE_TYPE_INTERMEDIATE == currentNode.type) {
            string childrenJson = "\"children\":[";
            // get the children and call recursively for each child.
            bool firstEntry = true;
            for(int & childPage : currentNode.getChildren(attribute) ) {
                if (!firstEntry)
                    childrenJson.append(",");
                childrenJson.append(getJson(ixFileHandle, attribute, childPage));
                firstEntry = false;
            }
            childrenJson.append("]"); // close the children array
            json.append(",");
            json.append(childrenJson);
        }

        json.append(", \"page\": " + to_string(pageId));

        json.append("}");
        return json;
    }

    void IndexManager::refreshCache(IXFileHandle &ixFileHandle, int pageId) {
        char nodeBytes[PAGE_SIZE];
        ixFileHandle.readPage(pageId, nodeBytes);
        cachedNode.reload(nodeBytes);
        cachedPage = pageId;
        cachedFile = ixFileHandle.filename;
    }

    void IndexManager::refreshCache(char *bytes, int pageId) {
        cachedNode.reload(bytes);
        cachedPage = pageId;
    }

    bool IndexManager::cached(const string& filename, int pageId) const {
        return cachedPage == pageId && this->cachedFile == filename;
    }

    void IndexManager::parseKey(AttrType attrType, InsertionChild *child, char *key, RID &rid) {
        int formattedKeyLength = TypeVarChar != attrType ? 4 : child->keyLength - sizeof(RID::pageNum) - sizeof(RID::slotNum);
        int copiedOffset = 0;
        if (TypeVarChar == attrType) {
            std::memcpy(key, &formattedKeyLength, sizeof(formattedKeyLength));
            copiedOffset += sizeof(formattedKeyLength);
        }

        std::memcpy(key + copiedOffset, child->leastChildValue, formattedKeyLength);
        std::memcpy(&rid.pageNum, (char *)child->leastChildValue + formattedKeyLength, sizeof(RID::pageNum));
        std::memcpy(&rid.slotNum, (char *)child->leastChildValue + formattedKeyLength + sizeof(RID::pageNum), sizeof(RID::slotNum));
    }
} // namespace PeterDB