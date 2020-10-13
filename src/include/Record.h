#ifndef PETERDB_RECORD_H
#define PETERDB_RECORD_H


class Record {
protected:
    int pageNo;
    int noOfAttributes;
    char* slotDirOffset;
    char* valueOffsets;
    char* values;

public:
    Record();
    Record(int pageNo, int noOfAttributes, char* slotDirOffset, char* valueOffsets, char* values);
    operator const char * ();

    virtual ~Record();
};


#endif //PETERDB_RECORD_H
