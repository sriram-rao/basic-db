#include <src/utils/parse_utils.h>
#include "src/include/ix.h"

namespace PeterDB{
    //    char type;
    //    int freeSpace;
    //    PageNum nextPage;
    //    vector<Slot> directory;

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
        return freeSpace > (dataSpace + sizeof(Slot));
    }

    void Node::insertKey(const Attribute &keyField, int dataSpace, const void *key, const RID &rid) {
        int keySize = 4;
        if (TypeVarChar == keyField.type) {
            std::memcpy(&keySize, key, sizeof(int));
        }
        std::memcpy(keys, key, keySize);
        std::memcpy(keys + keySize, &rid.pageNum, sizeof(rid.pageNum));
        std::memcpy(keys + keySize + sizeof(rid.pageNum), &rid.slotNum, sizeof(rid.slotNum));

        directory.push_back({ 0, static_cast<short>(dataSpace) });
        freeSpace = freeSpace - dataSpace - sizeof(Slot);
    }

    void Node::getKeyData(int index, const Attribute &keyField, char *key, RID &rid) {
        Slot keySlot = directory.at(index);
        int keySize =  getKeySize(index, keyField);
        std::memcpy(key, keys + keySlot.offset, keySize);
        std::memcpy(&rid.pageNum, keys + keySlot.offset + keySize, sizeof(rid.pageNum));
        std::memcpy(&rid.slotNum, keys + keySlot.offset + keySize + sizeof(rid.pageNum), sizeof(rid.slotNum));
    }

    std::string Node::toJsonString(const Attribute &keyField) {
        std::string json = "{ \"keys\": [";

        for(int i = 0; i < directory.size(); i++) {
            int keySize = getKeySize(i, keyField);
            Slot current = directory.at(i);
            char key [keySize];
            std::memcpy(key, keys + current.offset, keySize);
            json.append("\"" + ParseUtils::parse(keyField.type, key) + ":[");
            unsigned pageNum; unsigned short slotNum;
            std::memcpy(&pageNum, keys + current.offset + keySize, sizeof(pageNum));
            std::memcpy(&slotNum, keys + current.offset + keySize + sizeof(pageNum), sizeof(slotNum));
            json.append("(" + to_string(pageNum) + "," + to_string(slotNum) + ")");
            json.append("]\"");

            if (i < directory.size() - 1)
                json.append(", ");
        }
        json.append("]}");
        return json;
    }

    int Node::getKeySize(int index, const Attribute &keyField) const {
        if (TypeVarChar != keyField.type)
            return 4;

        Slot keySlot = directory.at(index);
        return keySlot.length - static_cast<int>(sizeof(unsigned)) - static_cast<int>(sizeof(unsigned short ));
    }

    int Node::getKeyCount() const {
        return directory.size();
    }

    Node::~Node() {
        if (nullptr != keys)
            free(keys);
    }
}