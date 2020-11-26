#include <src/utils/parse_utils.h>
#include <src/utils/compare_utils.h>
#include "src/include/ix.h"

namespace PeterDB{

    Node::Node() {
        keys = nullptr;
        freeSpace = PAGE_SIZE - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
        type = NODE_TYPE_INTERMEDIATE;
        nextPage = -1;
    }

    Node::Node(char nodeType) {
        keys = nullptr;
        freeSpace = PAGE_SIZE - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
        type = nodeType;
        nextPage = -1;
    }

    Node::Node(char *bytes) {
        // Populate metadata
        std::memcpy(&type, bytes + PAGE_SIZE - sizeof(type), sizeof(type));
        std::memcpy(&freeSpace, bytes + PAGE_SIZE - sizeof(freeSpace) - sizeof(type), sizeof(freeSpace));
        std::memcpy(&nextPage,  bytes + PAGE_SIZE - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(nextPage));
        int directoryCount = 0;
        std::memcpy(&directoryCount, bytes + PAGE_SIZE - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(directoryCount));
        if (directoryCount > 0) {
            int directorySize = directoryCount * sizeof(Slot);
            directory = vector<Slot>(directoryCount, {0, 0});
            std::memcpy(directory.data(), bytes + PAGE_SIZE - directorySize - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), directorySize);
        }

        // Populate data
        keys = (char *) malloc(PAGE_SIZE);
        std::memcpy(keys, bytes, PAGE_SIZE);
    }

    void Node::reload(char *bytes) {
        // Populate metadata
        std::memcpy(&type, bytes + PAGE_SIZE - sizeof(type), sizeof(type));
        std::memcpy(&freeSpace, bytes + PAGE_SIZE - sizeof(freeSpace) - sizeof(type), sizeof(freeSpace));
        std::memcpy(&nextPage,  bytes + PAGE_SIZE - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(nextPage));
        int directoryCount = 0;
        std::memcpy(&directoryCount, bytes + PAGE_SIZE - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), sizeof(directoryCount));
        if (directoryCount > 0) {
            int directorySize = directoryCount * sizeof(Slot);
            directory = vector<Slot>(directoryCount, {0, 0});
            std::memcpy(directory.data(), bytes + PAGE_SIZE - directorySize - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type), directorySize);
        }

        // Populate data
        if (nullptr != keys)
            free(keys);
        keys = (char *) malloc(PAGE_SIZE);
        std::memcpy(keys, bytes, PAGE_SIZE);
    }

    int Node::getOccupiedSpace() const {
        return PAGE_SIZE - freeSpace;
    }

    void Node::populateBytes(char *bytes) {
        int directoryCount = directory.size();
        int directorySize = sizeof(Slot) * directoryCount;
        int dataSize = PAGE_SIZE - freeSpace - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type) - directorySize;
        if (nullptr != keys)
            std::memcpy(bytes, keys, dataSize);

        if (directoryCount > 0)
            std::memcpy(bytes + PAGE_SIZE - directorySize - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                        directory.data(), directorySize);

        std::memcpy(bytes + PAGE_SIZE - sizeof(directoryCount) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                    &directoryCount, sizeof(directoryCount));
        std::memcpy(bytes + PAGE_SIZE - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type),
                    &nextPage, sizeof(nextPage));
        std::memcpy(bytes + PAGE_SIZE - sizeof(freeSpace) - sizeof(type),
                    &freeSpace, sizeof(freeSpace));
        std::memcpy(bytes + PAGE_SIZE - sizeof(type),
                    &type, sizeof(type));
    }

    bool Node::hasSpace(int dataSpace) const {
        int spaceNeeded = dataSpace + static_cast<int>(sizeof(Slot));
        if (NODE_TYPE_INTERMEDIATE == type)
            spaceNeeded += sizeof(int);
        return freeSpace > spaceNeeded;
    }

    int Node::findChildNode(const Attribute &keyField, const void *key, long pageId, int slotId, int &index, bool compareRids){
        // Key is expected formatted with varchar length at the start

        // Find the correct sub-tree, return child page ID
        if (NODE_TYPE_LEAF == type)
            return -1;

        if (directory.empty()) {
            index = 0;
            return nextPage;
        }

        char firstKey [directory.at(0).length + sizeof(int)]; // Why adding sizeof(int)?
        RID firstRid {};
        populateFormattedKey(keyField.type, firstKey, firstRid, 0);

        // given key < leftmost entry in node
        if (CompareUtils::checkLessThan(keyField.type, key, firstKey)) {
            index = 0;
            return nextPage;
        }
        if (CompareUtils::checkEqual(keyField.type, key, firstKey) && compareRids &&
            (pageId < firstRid.pageNum || (pageId == firstRid.pageNum && slotId < firstRid.slotNum))) {
            index = 0;
            return nextPage;
        }

        int lastIndex = 0;
        for (int i = 0; i < directory.size() - 1; ) {
            Slot current = directory.at(i);
            if (current.offset == -1) {
                ++i;
                continue;
            }

            char currentKey [current.length + sizeof(int)];
            RID currentRid {};
            populateFormattedKey(keyField.type, currentKey, currentRid, i);

            int nextIndex = i + 1;
            Slot next = directory.at(nextIndex);
            while (next.offset == -1) {
                ++nextIndex;
                if (nextIndex >= directory.size())
                    break;
                next = directory.at(nextIndex);
            }
            if (nextIndex >= directory.size())
                break;
            lastIndex = nextIndex;

            char nextKey [next.length + sizeof(int)];
            RID nextRid;
            populateFormattedKey(keyField.type, nextKey, nextRid, nextIndex);

            // current key <= given key < next key
            if ((CompareUtils::checkGreaterThan(keyField.type, key, currentKey)
                    || (CompareUtils::checkEqual(keyField.type, key, currentKey) &&
                        (pageId > currentRid.pageNum || (pageId == currentRid.pageNum && slotId >= currentRid.slotNum) ))
                ) &&
                (CompareUtils::checkLessThan(keyField.type, key, nextKey)
                    || (CompareUtils::checkEqual(keyField.type, key, nextKey) &&
                        (pageId < nextRid.pageNum || (pageId == nextRid.pageNum && slotId < nextRid.slotNum)))
                 )) {
                index = nextIndex;
                int childPage = -1;
                std::memcpy(&childPage, keys + current.offset + current.length - sizeof(childPage), sizeof(childPage));
                return childPage;
            }

            i = nextIndex;
        }

        // Key >= last key in node, return last childNode
        int lastChild = -1;
        Slot last = directory.at(lastIndex);
        std::memcpy(&lastChild, keys + last.offset + last.length - sizeof(lastChild), sizeof(lastChild));
        index = lastIndex + 1;
        return lastChild;
    }

    int Node::findKey(const Attribute &keyField, const void *key, const RID &rid, bool compareRid, bool getIndex){
        if (directory.empty() || NODE_TYPE_INTERMEDIATE == type)
            return -1;

        // Find the correct leaf entry
        int left  = 0; // initialise to first non-empty index
        while (left < directory.size() && directory.at(left).offset == -1)
            left++;

        int right = directory.size() - 1; // initialise to last non-empty index
        while (right >= 0 && directory.at(right).offset == -1)
            right--;

        while (left <= right) {
            int middleIndex = (left + right) / 2;
            int initialMiddle = middleIndex;
            Slot middle = directory.at(middleIndex);
            while (middle.offset == -1 && middleIndex <= right) {
                middleIndex++;
                middle = directory.at(middleIndex);
            }

            int middleKeyLength = middle.length - sizeof(RID::pageNum) - sizeof(RID::slotNum);
            char middleKey[middleKeyLength + sizeof(int)];
            int copiedOffset = 0;
            if (TypeVarChar == keyField.type) {
                std::memcpy(middleKey, &middleKeyLength, sizeof(middleKeyLength));
                copiedOffset += sizeof(middleKeyLength);
            }
            std::memcpy(middleKey + copiedOffset, keys + middle.offset, middleKeyLength);

            if (CompareUtils::checkLessThan(keyField.type, middleKey, key)) {
                left = middleIndex + 1;
                continue;
            }
            if (CompareUtils::checkGreaterThan(keyField.type, middleKey, key)) {
                right = initialMiddle - 1;
                continue;
            }

            if (!compareRid) {
                while(middleIndex >= 0 && CompareUtils::checkEqual(keyField.type, middleKey, key)){
                    middle = directory.at(middleIndex);
                    if (TypeVarChar == keyField.type) {
                        std::memcpy(middleKey, &middleKeyLength, sizeof(middleKeyLength));
                        copiedOffset += sizeof(middleKeyLength);
                    }
                    std::memcpy(middleKey + copiedOffset, keys + middle.offset, middleKeyLength);
                    middleIndex--;
                }
                return middleIndex + 1;
            }

            unsigned keyPageNum;
            unsigned short keySlotNum;
            std::memcpy(&keyPageNum, keys + middle.offset + middleKeyLength, sizeof(keyPageNum));
            std::memcpy(&keySlotNum, keys + middle.offset + middle.length - sizeof(keySlotNum),
                        sizeof(keySlotNum));
            if (keyPageNum < rid.pageNum || (keyPageNum == rid.pageNum && keySlotNum < rid.slotNum)) {
                left = middleIndex + 1;
            } else if (keyPageNum > rid.pageNum || (keyPageNum == rid.pageNum && keySlotNum > rid.slotNum)) {
                right = initialMiddle - 1;
            } else {
                return middleIndex;
            }
        }
        // If not found
        return getIndex ? left : -1; // TODO: check which index to return when index is needed
    }

    void Node::insertKey(const Attribute &keyField, int dataSpace, const void *key, const RID &rid) {
        int keySize = 4;
        int keyStart = 0;
        if (TypeVarChar == keyField.type) {
            std::memcpy(&keySize, key, sizeof(int));
            keyStart += sizeof(int);
        }

        int freeSpaceStart = getFreeSpaceStart();

        std::memcpy(keys + freeSpaceStart, (char *)key + keyStart, keySize);
        std::memcpy(keys + freeSpaceStart + keySize, &rid.pageNum, sizeof(rid.pageNum));
        std::memcpy(keys + freeSpaceStart + keySize + sizeof(rid.pageNum), &rid.slotNum, sizeof(rid.slotNum));

        int index = directory.empty() ? 0 : findKey(keyField, key, rid, true, true);
        directory.insert(directory.begin() + index, { static_cast<short>(freeSpaceStart), static_cast<short>(dataSpace) });
        freeSpace = freeSpace - dataSpace - sizeof(Slot);
    }

    void Node::deleteKey(const Attribute &keyField, int index) {
        Slot current = directory.at(index);
        int dataToMove = PAGE_SIZE - current.offset - current.length;

        std::memmove(keys + current.offset, keys + current.offset + current.length, dataToMove);
        freeSpace += current.length;
        for (auto & i : directory)
            if (i.offset > current.offset)
                i.offset -= current.length;

        directory.at(index).offset = -1;
        directory.at(index).length = -1;
    }

    void Node::getKeyData(const Attribute &keyField, int index, char *key, RID &rid) {
        Slot keySlot = directory.at(index);
        int keySize =  getKeySize(index, keyField);
        int copiedLength = 0;
        if (TypeVarChar == keyField.type) {
            std::memcpy(key, &keySize, sizeof(keySize));
            copiedLength += sizeof(keySize);
        }

        std::memcpy(key + copiedLength, keys + keySlot.offset, keySize);
        std::memcpy(&rid.pageNum, keys + keySlot.offset + keySize, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + keySlot.offset + keySize + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    void Node::insertChild(const Attribute &keyField, int index, void *key, int keyLength, int childPageId) {
        int newKeySpace = keyLength + sizeof(childPageId);
        if (directory.empty()){
            std::memcpy(keys, key, keyLength);
            std::memcpy(keys + keyLength, &childPageId, sizeof(childPageId));
            directory.push_back({ 0, static_cast<short>(newKeySpace) });
            freeSpace -= newKeySpace + sizeof(Slot);
            return;
        }

        int freeSpaceStart = getFreeSpaceStart();
        std::memcpy(keys + freeSpaceStart, key, keyLength);
        std::memcpy(keys + freeSpaceStart + keyLength, &childPageId, sizeof(childPageId));
        directory.insert(directory.begin() + index, { static_cast<short>(freeSpaceStart), static_cast<short>(newKeySpace) });
        freeSpace -= newKeySpace + sizeof(Slot);
    }

    void Node::cleanDirectory() {
        for (int i = 0; i < directory.size(); ++i) {
            Slot current = directory.at(i);
            if (current.offset == -1) {
                directory.erase(directory.begin() + i);
                freeSpace += sizeof(Slot);
            }
        }
    }

    void Node::split(char *newNode, InsertionChild *child, int &splitStart, int childIndex) {
        cleanDirectory();

        Node splitNode(type);
        splitNode.keys = (char *) malloc(PAGE_SIZE);
        int copyStartIndex = 0;
        int dataToKeep = 0;
        while (dataToKeep < PAGE_SIZE / 2) {
            dataToKeep += directory.at(copyStartIndex).length + sizeof(Slot);
            copyStartIndex++;
        }
        copyStartIndex--;
        if (NODE_TYPE_INTERMEDIATE == type && childIndex < copyStartIndex)
            copyStartIndex--;
        splitStart = copyStartIndex;

        Slot copyStart = directory.at(copyStartIndex);
        child->keyLength = copyStart.length;
        std::memcpy(child->leastChildValue, keys + copyStart.offset, copyStart.length);
        splitNode.nextPage = this->nextPage;

        if (NODE_TYPE_INTERMEDIATE == type) {
            int pageId;
            std::memcpy(&pageId, keys + copyStart.offset + copyStart.length - sizeof(pageId), sizeof(pageId));
            splitNode.nextPage = pageId;
            child->keyLength -= sizeof(pageId);

            // We copied over the least child and left-most child page
            // These don't go into split's keys and we can remove them from current node
            int dataToMove = PAGE_SIZE - copyStart.offset - copyStart.length;
            std::memmove(this->keys + copyStart.offset, this->keys + copyStart.offset + copyStart.length, dataToMove);

            // Fixing offsets for all moved entries
            for (auto & j : directory)
                if (j.offset > copyStart.offset)
                    j.offset -= copyStart.length;
            this->freeSpace += copyStart.length + sizeof(Slot);
            directory.erase(directory.begin() + copyStartIndex);
        }

        int copiedLength = 0, copyEndIndex = directory.size();
        for (int i = copyStartIndex; i < copyEndIndex; ++i) {
            // Move keys from current to split node
            Slot current = directory.at(copyStartIndex);
            std::memcpy(splitNode.keys + copiedLength, this->keys + current.offset, current.length);
            int dataToMove = PAGE_SIZE - current.offset - current.length;
            std::memmove(this->keys + current.offset, this->keys + current.offset + current.length, dataToMove);

            // Fixing offsets in current for all moved entries
            for (auto & j : directory)
                if (j.offset > current.offset)
                    j.offset -= current.length;

            splitNode.freeSpace -= current.length + sizeof(Slot);
            this->freeSpace += current.length + sizeof(Slot);
            splitNode.directory.push_back({ static_cast<short>(copiedLength), current.length });
            copiedLength += current.length;
            directory.erase(directory.begin() + copyStartIndex);
        }
        splitNode.populateBytes(newNode);
    }

    std::string Node::toJsonKeys(const Attribute &keyField) {
        return NODE_TYPE_INTERMEDIATE == type
            ? toJsonKeysIntermediate(keyField)
            : toJsonKeysLeaf(keyField);
    }

    std::string Node::toJsonKeysLeaf(const Attribute &keyField) {
        string keysJson = "\"keys\": [";
        std::string previousKey;
        bool processedKeys = false;
        int currentKeyCount = 0;

        for(int i = 0; i < directory.size(); ++i) {
            Slot current = directory.at(i);
            if (-1 == current.offset)
                continue;

            int keySize = getKeySize(i, keyField);
            char *key = (char *) malloc(keySize);
            std::memcpy(key, keys + current.offset, keySize);

            std::string currentKey = ParseUtils::parse(keyField.type, key, keySize);
            free(key);
            if ((!processedKeys && currentKey.empty()) || currentKey != previousKey) {
                currentKeyCount = 0;
                std::string prefix = !processedKeys ? "\"" : "]\",\"";
                std::string suffix = ":[";
                keysJson.append(prefix.append(currentKey + suffix));
                processedKeys = true;
            }

            unsigned pageNum;
            unsigned short slotNum;
            std::memcpy(&pageNum, keys + current.offset + keySize, sizeof(pageNum));
            std::memcpy(&slotNum, keys + current.offset + keySize + sizeof(pageNum), sizeof(slotNum));

            std::string comma = currentKeyCount > 0 ? "," : "";
            keysJson.append(comma + "(" + to_string(pageNum) + "," + to_string(slotNum) + ")");
            ++currentKeyCount;

            if (previousKey != currentKey)
                previousKey = currentKey;
        }
        if (processedKeys)
            keysJson.append("]\""); // closing last key's RID list
        keysJson.append("]"); // closing keys array
        return keysJson;
    }

    std::string Node::toJsonKeysIntermediate(const Attribute &keyField) {
        string keysJson = "\"keys\":[";
        bool first = true;

        for(int i = 0; i < directory.size(); ++i) {
            if (-1 == directory.at(i).offset)
                continue;

            Slot current = directory.at(i);
            int keySize = getKeySize(i, keyField);
            char *key = (char *) malloc(keySize);
            std::memcpy(key, keys + current.offset, keySize);
            std::string currentKey = ParseUtils::parse(keyField.type, key, keySize);
            free(key);
            std::string prefix = first ? "\"" : "\",\"";
            keysJson.append(prefix + currentKey);
            first = false;
        }
        if (!first)
            keysJson.append("\""); // closing last key quote
        keysJson.append("]"); // closing the keys array
        return keysJson;
    }

    std::vector<int> Node::getChildren(const Attribute &keyField) {
        std::vector<int> childPages;
        if (NODE_TYPE_LEAF == type)
            return childPages;

        if (-1 != nextPage)
            childPages.push_back(nextPage);

        for (auto & slot : directory) {
            if (-1 == slot.offset)
                continue;

            int pageId = -1;
            std::memcpy(&pageId, keys + slot.offset + slot.length - sizeof(pageId), sizeof(pageId));
            childPages.push_back(pageId);
        }

        return childPages;
    }

    int Node::getKeySize(int index, const Attribute &keyField) const {
        if (TypeVarChar != keyField.type)
            return 4;

        Slot keySlot = directory.at(index);
        int keySize = keySlot.length - static_cast<int>(sizeof(unsigned)) - static_cast<int>(sizeof(unsigned short));
        if (NODE_TYPE_INTERMEDIATE == type)
            keySize -= sizeof(int);
        return keySize;
    }

    int Node::getKeyCount() const {
        return directory.size();
    }

    int Node::getFreeSpaceStart() {
        return PAGE_SIZE - freeSpace - sizeof(Slot) * directory.size()
               - sizeof(int) - sizeof(nextPage) - sizeof(freeSpace) - sizeof(type);
    }

    void Node::populateFormattedKey(AttrType attrType, char *key, RID &rid, int index) {
        Slot slot = directory.at(index);
        int keyLength = slot.length - sizeof(RID::pageNum) - sizeof(RID::slotNum) - sizeof(int);
        int copiedOffset = 0;
        if (TypeVarChar == attrType) {
            std::memcpy(key, &keyLength, sizeof(keyLength));
            copiedOffset += sizeof(keyLength);
        }
        std::memcpy(key + copiedOffset, keys + slot.offset, keyLength);
        std::memcpy(&rid.pageNum, keys + slot.offset + keyLength, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + slot.offset + keyLength + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    Node::~Node() {
        if (nullptr != keys)
            free(keys);
    }

    bool Node::validateIndex(int index) {
        return index < directory.size() && directory.at(index).offset != -1;
    }
}