#ifndef PETERDB_SLOT_H
#define PETERDB_SLOT_H


class Slot {
protected:

    int sizeOfRecord;
public:
    char* recordValueOffset;
    Slot();
    Slot(char* recordValueOffset, int sizeOfRecord);

    virtual ~Slot();
};


#endif //PETERDB_SLOT_H
