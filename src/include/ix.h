#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan
# define IX_HIDDEN_PAGE_COUNT 1
# define NODE_TYPE_INTERMEDIATE 1
# define NODE_TYPE_LEAF 2

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class Node;

    class InsertionChild;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        void insert(IXFileHandle &ixFileHandle, int nodePageId, const Attribute &attribute, const void *key, const RID &rid, InsertionChild *newChild);

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        int rootPage;
        std::fstream ixFile;

        // Constructor
        IXFileHandle();

        IXFileHandle& operator= (const IXFileHandle& other);

        void setFile(std::fstream&& file);

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        int appendPage(const void *data);                                   // Append a specific page, returns the new page number
        int getRootPageId();
        RC create(const std::string &fileName);
        void init();
        RC open(const std::string &fileName);
        RC close();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        unsigned getPageCount() const;

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        bool meetsCondition(void *key);

        void incrementCursor(int currentKeyCount, int nextPage);

        IXFileHandle *ixFileHandle;
        int pageNum;
        int slotNum;
        Attribute attribute;
        void *lowKey;
        void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
    };

    class Node {
    public:
        char *keys;
        char type; // Intermediate node or leaf node
        int freeSpace;
        int nextPage;
        vector<Slot> directory;

        Node(char type);
        Node(char *bytes);

        int getOccupiedSpace() const;
        int findChildNode(const Attribute &keyField, const void *key, const RID &rid);
        int findKey(const Attribute &keyField, const void *key, const RID &rid);
        void insertKey(const Attribute &keyField, int dataSpace, const void *key, const RID &rid);
        void deleteKey(const Attribute &keyField, int index, const void *key, const RID &rid);
        void getKeyData(const Attribute &attribute, int index, char *key, RID &rid);
        int getKeyCount() const;
        bool hasSpace(int dataSpace) const;
        void populateBytes(char *bytes);
        std::string toJsonString(const Attribute &keyField);

        ~Node();

    private:
        int getKeySize(int index, const Attribute &keyField) const;
    };

    class InsertionChild{
    public:
        void *leastChildValue;
        int childNodePage;
    };

}// namespace PeterDB
#endif // _ix_h_
